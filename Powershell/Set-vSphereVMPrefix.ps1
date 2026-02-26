<#
.SYNOPSIS
    Searches for vSphere VMs based on name prefixes or exact names and applies an SLA Domain.

.DESCRIPTION
    This script authenticates using a Rubrik Service Account JSON file. It allows 
    searching for VMs using one or multiple comma-separated string matches (prefixes or exact names). 
    
    It evaluates the matched VMs and excludes any that are already assigned to the target 
    SLA Domain (e.g., DO_NOT_PROTECT) to prevent redundant API calls. Finally, it applies 
    the target SLA to the aggregated list of eligible VMs.

    Use the -ReportOnly switch to safely preview the VMs that would be affected, 
    along with their Current SLA, without actually making any changes to the system.

.EXAMPLE
    # Example 1: Safely preview which VMs match the "ad" prefix and see their current SLA
    .\Get-VSphereVMsList.ps1 -ServiceAccountJson "C:\path\to\sa.json" -VmPrefixes "ad" -ReportOnly

.EXAMPLE
    # Example 2: Apply the DO_NOT_PROTECT SLA to a specific VM by its exact name
    .\Get-VSphereVMsList.ps1 -ServiceAccountJson "C:\path\to\sa.json" -VmPrefixes "ad-m-child-01"

.EXAMPLE
    # Example 3: Search for multiple prefixes/names and assign a custom SLA
    .\Get-VSphereVMsList.ps1 -ServiceAccountJson "C:\path\to\sa.json" -VmPrefixes "ad, sql, web" -SlaId "YOUR_SLA_ID_HERE"

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : February 26, 2026
    Company : Rubrik Inc
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,

    [Parameter(Mandatory=$true)]
    [string[]]$VmPrefixes,

    [Parameter(Mandatory=$false)]
    [string]$SlaId = "DO_NOT_PROTECT",

    [Parameter(Mandatory=$false)]
    [switch]$ReportOnly
)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

function Connect-Polaris {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [object]$ServiceAccountObj
    )
    begin {
        $connectionData = [ordered]@{
            'client_id' = $ServiceAccountObj.client_id
            'client_secret' = $ServiceAccountObj.client_secret
        } | ConvertTo-Json
    }
    process {
        try {
            $polaris = Invoke-RestMethod -Method Post -Uri $ServiceAccountObj.access_token_uri -ContentType application/json -Body $connectionData
        }
        catch {
            Write-Error "The provided JSON has null or empty fields, or authentication failed. Error: $_"
            throw
        }
    }
    end {
        if ($polaris.access_token) {
            Write-Output $polaris
        } else {
            Write-Error "Unable to connect, no token received."
            throw
        }
    }
}

function Disconnect-Polaris {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [Parameter(Mandatory=$true)]
        [string]$LogoutUrl
    )
    process {
        try {
            $closeStatus = (Invoke-WebRequest -Method Delete -Headers $Headers -ContentType "application/json; charset=utf-8" -Uri $LogoutUrl).StatusCode
        }
        catch {
            Write-Error "Failed to logout. Error $_"
        }
    }
    end {
        if ($closeStatus -eq 204) {
            Write-Host "Successfully logged out."
        } else {
            Write-Warning "Logout may have failed. Status code: $closeStatus"
        }
    }
}

function Get-VSphereVMsList {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$GraphQLEndpoint,

        [Parameter(Mandatory = $true)]
        [hashtable]$Headers,

        [Parameter(Mandatory = $false)]
        [int]$First = 50,

        [Parameter(Mandatory = $false)]
        [string]$After,

        [Parameter(Mandatory = $false)]
        [string]$VmPrefix,

        [Parameter(Mandatory = $false)]
        [array]$Filter = @(
            @{ field = "IS_GHOST"; texts = @("false") }
            @{ field = "IS_RELIC"; texts = @("false") }
            @{ field = "IS_REPLICATED"; texts = @("false") }
            @{ field = "IS_ACTIVE"; texts = @("true") }
            @{ field = "IS_ACTIVE_AMONG_DUPLICATED_OBJECTS"; texts = @("true") }
        ),

        [Parameter(Mandatory = $false)]
        [string]$SortBy = "NAME",

        [Parameter(Mandatory = $false)]
        [string]$SortOrder = "ASC",

        [Parameter(Mandatory = $false)]
        [switch]$IsMultitenancyEnabled,

        [Parameter(Mandatory = $false)]
        [switch]$IsDuplicatedVmsIncluded,

        [Parameter(Mandatory = $false)]
        [switch]$IncludeRscNativeObjectPendingSla,

        [Parameter(Mandatory = $false)]
        [switch]$IsObjectProtectionPauseEnabled,

        [Parameter(Mandatory = $false)]
        [switch]$IsRscTagEnabled,

        [Parameter(Mandatory = $false)]
        [switch]$ExcludeUnprotected
    )

    $Query = @"
    query VSphereVMsListQuery(`$first: Int!, `$after: String, `$filter: [Filter!]!, `$isMultitenancyEnabled: Boolean = false, `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isDuplicatedVmsIncluded: Boolean = true, `$includeRscNativeObjectPendingSla: Boolean = false, `$isObjectProtectionPauseEnabled: Boolean = false, `$isRscTagEnabled: Boolean = false) {  
      vSphereVmNewConnection(filter: `$filter, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
        edges {
          cursor
          node {
            id
            snapshotConsistencyMandate
            snapshotConsistencySource
            ...VSphereNameColumnFragment
            ...CdmClusterColumnFragment
            ...EffectiveSlaColumnFragment
            ...VSphereSlaAssignmentColumnFragment
            ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
            ...SystemTagsColumnFragment @include(if: `$isRscTagEnabled)
            isRelic
            authorizedOperations
            templateType
            primaryClusterLocation {
              id
              name
              __typename
            }
            logicalPath {
              fid
              name
              objectType
              __typename
            }
            physicalPath {
              fid
              name
              objectType
              __typename
            }
            cdmPendingObjectPauseAssignment @include(if: `$isObjectProtectionPauseEnabled)
            ...ObjectPauseStatusFragment @include(if: `$isObjectProtectionPauseEnabled)
            slaPauseStatus
            snapshotDistribution {
              id
              totalCount
              __typename
            }
            snapshotConnection {
              count
              __typename
            }
            reportWorkload {
              id
              archiveStorage
              physicalBytes
              __typename
            }
            vmwareToolsInstalled
            templateType
            agentStatus {
              agentStatus
              __typename
            }
            vsphereVirtualDisks {
              edges {
                node {
                  fid
                  datastoreFid
                  fileName
                  size
                  __typename
                }
                __typename
              }
              __typename
            }
            duplicatedVms @include(if: `$isDuplicatedVmsIncluded) {
              fid
              cluster {
                id
                name
                version
                status
                pauseStatus
                __typename
              }
              slaAssignment
              effectiveSlaDomain {
                ... on GlobalSlaReply {
                  id
                  name
                  isRetentionLockedSla
                  retentionLockMode
                  description
                  __typename
                }
                ... on ClusterSlaDomain {
                  id
                  fid
                  name
                  isRetentionLockedSla
                  retentionLockMode
                  cluster {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                __typename
              }
              snapshotDistribution {
                id
                totalCount
                __typename
              }
              effectiveSlaSourceObject {
                fid
                objectType
                name
                __typename
              }
              reportWorkload {
                archiveStorage
                physicalBytes
                id
                __typename
              }
              allTags @include(if: `$isRscTagEnabled) {
                id
                name
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        pageInfo {
          startCursor
          endCursor
          hasNextPage
          hasPreviousPage
          __typename
        }
        __typename
      }
    }

    fragment VSphereNameColumnFragment on HierarchyObject {
      id
      name
      ...HierarchyObjectTypeFragment
      __typename
    }

    fragment HierarchyObjectTypeFragment on HierarchyObject {
      objectType
      __typename
    }

    fragment EffectiveSlaColumnFragment on HierarchyObject {
      id
      effectiveSlaDomain {
        ...EffectiveSlaDomainFragment
        ... on GlobalSlaReply {
          description
          __typename
        }
        __typename
      }
      ... on CdmHierarchyObject {
        pendingSla {
          ...SLADomainFragment
          __typename
        }
        __typename
      }
      ... on CloudDirectHierarchyObject {
        pendingSla {
          ...SLADomainFragment
          __typename
        }
        __typename
      }
      ... on PolarisHierarchyObject {
        rscNativeObjectPendingSla @include(if: `$includeRscNativeObjectPendingSla) {
          ...CompactSLADomainFragment
          __typename
        }
        __typename
      }
      __typename
    }

    fragment EffectiveSlaDomainFragment on SlaDomain {
      id
      name
      ... on GlobalSlaReply {
        isRetentionLockedSla
        retentionLockMode
        haPolicy {
          id
          __typename
        }
        __typename
      }
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
          __typename
        }
        isRetentionLockedSla
        retentionLockMode
        __typename
      }
      __typename
    }

    fragment SLADomainFragment on SlaDomain {
      id
      name
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
          __typename
        }
        __typename
      }
      __typename
    }

    fragment CompactSLADomainFragment on CompactSlaDomain {
      id
      name
      __typename
    }

    fragment CdmClusterColumnFragment on CdmHierarchyObject {
      replicatedObjectCount
      cluster {
        id
        name
        version
        status
        pauseStatus
        __typename
      }
      __typename
    }

    fragment VSphereSlaAssignmentColumnFragment on HierarchyObject {
      effectiveSlaSourceObject {
        fid
        name
        objectType
        __typename
      }
      ...SlaAssignmentColumnFragment
      __typename
    }

    fragment SlaAssignmentColumnFragment on HierarchyObject {
      slaAssignment
      __typename
    }

    fragment OrganizationsColumnFragment on HierarchyObject {
      allOrgs {
        fullName
        __typename
      }
      __typename
    }

    fragment SystemTagsColumnFragment on HierarchyObject {
      allTags {
        id
        name
        __typename
      }
      __typename
    }

    fragment ObjectPauseStatusFragment on HierarchyObject {
      objectPauseStatus {
        isDirectlyPaused
        isEffectivelyPaused
        pausedSources {
          pausedSourceDetails {
            pausedSourceId
            pausedSourceType
            pausedSourceObjectName
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
"@

    $ActiveFilter = @($Filter)
    if (-not [string]::IsNullOrWhiteSpace($VmPrefix)) {
        $ActiveFilter += @{ field = "NAME"; texts = @($VmPrefix) }
    }

    $Variables = @{
        first                            = $First
        filter                           = $ActiveFilter
        sortBy                           = $SortBy
        sortOrder                        = $SortOrder
        isMultitenancyEnabled            = [bool]$IsMultitenancyEnabled.IsPresent
        isDuplicatedVmsIncluded          = [bool]$IsDuplicatedVmsIncluded.IsPresent
        includeRscNativeObjectPendingSla = [bool]$IncludeRscNativeObjectPendingSla.IsPresent
        isObjectProtectionPauseEnabled   = [bool]$IsObjectProtectionPauseEnabled.IsPresent
        isRscTagEnabled                  = [bool]$IsRscTagEnabled.IsPresent
    }

    if ($PSBoundParameters.ContainsKey('After')) {
        $Variables.Add('after', $After)
    }

    $Payload = @{
        query     = $Query
        variables = $Variables
    } | ConvertTo-Json -Depth 10

    try {
        $Response = Invoke-RestMethod -Uri $GraphQLEndpoint -Method Post -Headers $Headers -Body $Payload -ContentType "application/json"
        
        if ($Response.errors) {
            Write-Error "GraphQL returned errors: $($Response.errors | ConvertTo-Json -Depth 5)"
        }

        # Filter out DO_NOT_PROTECT if switch is provided
        if ($ExcludeUnprotected.IsPresent -and $Response.data.vSphereVmNewConnection.edges) {
            $FilteredEdges = $Response.data.vSphereVmNewConnection.edges | Where-Object {
                $_.node.effectiveSlaDomain.id -ne 'DO_NOT_PROTECT'
            }
            $Response.data.vSphereVmNewConnection.edges = @($FilteredEdges)
        }

        return $Response.data
    }
    catch {
        Write-Error "Failed to execute GraphQL query. Error: $_"
        throw
    }
}

function Set-SLADomains {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [string]$GraphQLEndpoint,

        [Parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [Parameter(Mandatory=$true)]
        [string]$SlaId,

        [Parameter(Mandatory=$true)]
        [string[]]$ObjectIds
    )
    try {
        if ($ObjectIds.Count -eq 1 -and $ObjectIds[0] -match ",") {
            $ObjectIds = $ObjectIds[0] -split "," | ForEach-Object { $_.Trim() }
        }

        # Dynamically define variables depending on DO_NOT_PROTECT vs Standard SLA ID
        $SlaAssignType = "protectWithSlaId"
        $SlaOptionalId = $SlaId
        $ExistingSnapshotRetention = $null
        $ShouldApplyToExistingSnapshots = $true

        if ($SlaId -eq "DO_NOT_PROTECT") {
            $SlaAssignType = "doNotProtect"
            $SlaOptionalId = $null
            $ExistingSnapshotRetention = "RETAIN_SNAPSHOTS"
            $ShouldApplyToExistingSnapshots = $null
        }

        $Variables = @{
            input = @{
                assignSlaRequests = @(
                    @{
                        slaDomainAssignType = $SlaAssignType
                        objectIds = $ObjectIds
                        shouldApplyToExistingSnapshots = $ShouldApplyToExistingSnapshots
                        shouldApplyToNonPolicySnapshots = $false
                        slaOptionalId = $SlaOptionalId
                        existingSnapshotRetention = $ExistingSnapshotRetention
                    }
                )
                parentObjectIdToConflictObjectIdsMap = @()
                userNote = ""
            }
        }

        $Query = @"
        mutation BulkAssignSlasMutation(`$input: BulkAssignSlasInput!) {
          bulkAssignSlas(input: `$input) {
            slaAssignResults {
              success
              __typename
            }
            __typename
          }
        }
"@

        $Payload = @{
            query = $Query
            variables = $Variables
        } | ConvertTo-Json -Depth 10

        $Result = Invoke-RestMethod -Uri $GraphQLEndpoint -Method Post -Headers $Headers -Body $Payload -ContentType "application/json"
        
        if ($Result.errors) {
            Write-Error "GraphQL returned errors: $($Result.errors | ConvertTo-Json -Depth 5)"
        }

        # Drill down into the specific response structure of BulkAssignSlas
        if ($Result.data.bulkAssignSlas.slaAssignResults) {
            $JobStatus = $Result.data.bulkAssignSlas.slaAssignResults[0][0]
        }
    }
    catch {
        Write-Error "Error $_"
    }
    finally {
        Write-Output $JobStatus
    }
}


# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================

# 1. Read JSON and Authenticate
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = Connect-Polaris -ServiceAccountObj $serviceAccountObj

$rubtok = $polSession.access_token
$Headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = "Bearer $rubtok"
}

$GraphQLUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$LogoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

# 2. Iterate through VM Prefixes and accumulate matching VMs
$AllVMs = New-Object System.Collections.Generic.List[object]

# Ensure we handle comma separated prefixes accurately
if ($VmPrefixes.Count -eq 1 -and $VmPrefixes[0] -match ",") {
    $VmPrefixes = $VmPrefixes[0] -split "," | ForEach-Object { $_.Trim() }
}

foreach ($Prefix in $VmPrefixes) {
    if (-not [string]::IsNullOrWhiteSpace($Prefix)) {
        Write-Host "Searching for VMs with prefix: '$Prefix'..."
        # NOTE: We use -ExcludeUnprotected to ensure we drop items that are ALREADY assigned DO_NOT_PROTECT
        $Result = Get-VSphereVMsList -GraphQLEndpoint $GraphQLUrl -Headers $Headers -VmPrefix $Prefix -ExcludeUnprotected
        
        $VMs = $Result.vSphereVmNewConnection.edges.node
        if ($VMs) {
            foreach ($VM in $VMs) {
                $AllVMs.Add($VM)
            }
        }
    }
}

# 3. Deduplicate VMs (in case prefixes overlap, like "ad" and "admin")
# We group by ID and grab the first object in each group so we retain the VM Name for reporting
$UniqueVMs = $AllVMs | Group-Object -Property id | ForEach-Object { $_.Group[0] }
$UniqueVmIds = $UniqueVMs.id

# 4. Assign the SLA Domain or output Report
if ($UniqueVmIds) {
    Write-Host ("Found {0} eligible VMs." -f $UniqueVmIds.Count)
    
    if ($ReportOnly.IsPresent) {
        Write-Host "===========================================================" -ForegroundColor Cyan
        Write-Host " REPORT ONLY MODE: The following VMs would be updated to '$SlaId'" -ForegroundColor Cyan
        Write-Host "===========================================================" -ForegroundColor Cyan
        $UniqueVMs | Select-Object name, id, @{Name="Current SLA"; Expression={$_.effectiveSlaDomain.name}} | Format-Table -AutoSize | Out-String | Write-Host
        Write-Host "No changes were made to the system." -ForegroundColor Cyan
    } else {
        Write-Host ("Applying SLA Domain '{0}'..." -f $SlaId)
        $AssignmentResult = Set-SLADomains -GraphQLEndpoint $GraphQLUrl -Headers $Headers -SlaId $SlaId -ObjectIds $UniqueVmIds
        
        if ($AssignmentResult.success) {
            Write-Host "Success: SLA Domain update initiated." -ForegroundColor Green
        } else {
            Write-Warning "SLA Assignment may have failed. Verification required."
        }
    }
} else {
    Write-Host "No eligible VMs were found matching the provided prefixes."
}

# 5. Cleanup session
Disconnect-Polaris -Headers $Headers -LogoutUrl $LogoutUrl
