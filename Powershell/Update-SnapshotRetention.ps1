<#
.SYNOPSIS
This script updates the retention SLA for a list of specified Rubrik snapshots.
It can either query for snapshots and export them, or import a previously exported CSV to perform the update.

.EXAMPLE
# Example 1: Update retention for manually specified snapshot IDs
./Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -NewSlaId "sla-id-123" -SnapshotIds "snap-123", "snap-456"

.EXAMPLE
# Example 2: Query for objects, get all their snapshots, export them to CSV, and then update their retention
./Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -NewSlaId "sla-id-123" -ObjectId "obj-id-789" -ClusterUuid "cluster-uuid-abc" -FilterRetentionSlaDomainIds "sla-abc" -ExportCsvPath "C:\temp\all_snapshots_for_review.csv" -UserNote "Bulk update per ticket #12345"

.EXAMPLE
# Example 3: Query for *all* objects on a cluster, get all snapshots, and just export to CSV (no update)
./Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -ClusterUuid "cluster-uuid-abc" -ExportCsvPath "C:\temp\all_cluster_snapshots.csv"

.EXAMPLE
# Example 4: Query for *only relic* objects on a cluster, get all their snapshots, and export to CSV
./Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -ClusterUuid "cluster-uuid-abc" -RelicObjectsOnly -ExportCsvPath "C:\temp\relic_snapshots.csv"

.EXAMPLE
# Example 5: Import a manually reviewed CSV and apply a new SLA to only the snapshots listed in the file.
# This is the recommended safe workflow for bulk updates.
# Step 1 (Export): .\Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -ClusterUuid "cluster-uuid-abc" -ExportCsvPath "C:\temp\review_snapshots.csv"
# Step 2 (Manual): Manually edit "review_snapshots.csv" and remove any rows you do not want to update.
# Step 3 (Import & Update): .\Update-SnapshotRetention.ps1 -ServiceAccountJson "C:\scripts\ServiceAccount.json" -NewSlaId "sla-id-123" -ImportCsvPath "C:\temp\review_snapshots.csv" -UserNote "Bulk update per ticket #12345"

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Adapted : November 6, 2025
    Company : Rubrik Inc
    Purpose : Framework for bulk snapshot SLA retention updates.
#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,

    [parameter(Mandatory=$false)] # Changed from Mandatory=$true
    [string]$NewSlaId,

    [parameter(Mandatory=$false)]
    [string[]]$SnapshotIds,

    # Parameters for querying objects
    [parameter(Mandatory=$false)]
    [string]$ObjectId,

    [parameter(Mandatory=$false)]
    [string]$ClusterUuid,

    [parameter(Mandatory=$false)]
    [string[]]$FilterRetentionSlaDomainIds = @(),

    [parameter(Mandatory=$false)]
    [string[]]$FilterObjectTypes = @(),

    [parameter(Mandatory=$false)]
    [string[]]$FilterUnmanagedStatuses = @(),

    [parameter(Mandatory=$false)]
    [hashtable]$SortParam = @{ "sortOrder" = "ASC"; "type" = "NAME" },

    [parameter(Mandatory=$false)]
    [switch]$RelicObjectsOnly,
    
    # Parameter for CSV export
    [parameter(Mandatory=$false)]
    [string]$ExportCsvPath,

    # Parameter for CSV import
    [parameter(Mandatory=$false)]
    [string]$ImportCsvPath,

    # Optional note for the retention change
    [parameter(Mandatory=$false)]
    [string]$UserNote = ""
)

# --- Function Definitions ---

function Connect-Polaris {
    # Function that uses the Polaris/RSC Service Account JSON and opens a new session, and returns the session temp token
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [psobject]$ServiceAccountObject
    )
    
    begin {
        # Parse the JSON and build the connection string
        $connectionData = [ordered]@{
            'client_id' = $ServiceAccountObject.client_id
            'client_secret' = $ServiceAccountObject.client_secret
        } | ConvertTo-Json
    }
    
    process {
        try {
            $polaris = Invoke-RestMethod -Method Post -uri $ServiceAccountObject.access_token_uri -ContentType application/json -body $connectionData
        }
        catch [System.Management.Automation.ParameterBindingException] {
            Write-Error("The provided JSON has null or empty fields, try the command again with the correct file or redownload the service account JSON from Polaris")
        }
        catch {
            Write-Error("An error occurred during connection: $($_)")
        }
    }
    
    end {
        if ($polaris.access_token) {
            Write-Output $polaris
        }
        else {
            Write-Error("Unable to connect. Check credentials and Polaris URL.")
        }
    }
}

function Disconnect-Polaris {
    # Closes the session with the session token passed here
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [parameter(Mandatory=$true)]
        [string]$LogoutUrl
    )
    
    begin {
        # Headers and LogoutUrl are now passed in as parameters
    }
    
    process {
        try {
            $closeStatus = $(Invoke-WebRequest -Method Delete -Headers $Headers -ContentType "application/json; charset=utf-8" -Uri $LogoutUrl).StatusCode
        }
        catch {
            # Catch all errors, including ParameterBindingException
            Write-Error("Failed to logout. Error $($_)")
        }
    }
    
    end {
        if ($closeStatus -eq 204) {
            Write-Output("Successfully logged out.")
        }
        else {
            Write-Warning("Logout may not have been successful. Status code: $closeStatus")
        }
    }
}

function Get-UnmanagedSnapshots {
    # Fetches unmanaged snapshot *objects* using the specified query, handling pagination and returning flattened objects.
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [string]$ObjectId,

        [parameter(Mandatory=$true)]
        [string]$ClusterUuid,

        [parameter(Mandatory=$false)] # Changed from Mandatory=true and removed ValidateNotNullOrEmpty
        [string[]]$GUS_RetentionSlaDomainIds = @(),

        [parameter(Mandatory=$false)]
        [string[]]$GUS_ObjectTypes = @(),

        [parameter(Mandatory=$false)]
        [string[]]$GUS_UnmanagedStatuses = @(),

        [parameter(Mandatory=$false)]
        [hashtable]$GUS_SortParam = @{ "sortOrder" = "ASC"; "type" = "NAME" },

        [parameter(Mandatory=$false)]
        [int]$First = 50,

        [parameter(Mandatory=$false)]
        [bool]$IsReplicatedSnapshotSlaRetentionChangeEnabled = $false,

        [parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [parameter(Mandatory=$true)]
        [string]$GraphQL_URL
    )

    begin {
        # Define GraphQL fragments
        $fragmentEffectiveSlaDomain = "
        fragment EffectiveSlaDomainFragment on SlaDomain {
          id
          name
          ... on GlobalSlaReply {
            isRetentionLockedSla
            retentionLockMode
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
        }"
        
        $fragmentSLADomain = "
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
        }"

        # Define main query
        $query = "
        query SnapshotManagementGlobalObjectsQuery(`$input: UnmanagedObjectsInput!, `$after: String, `$first: Int, `$isReplicatedSnapshotSlaRetentionChangeEnabled: Boolean = false) {
          unmanagedObjects(input: `$input, first: `$first, after: `$after) {
            edges {
              cursor
              node {
                archiveStorage
                hasSnapshotsWithPolicy
                id
                workloadId
                isRemote
                localStorage
                name
                objectType
                physicalLocation {
                  name
                  managedId
                  __typename
                }
                unmanagedStatus
                backupCopyType @include(if: `$isReplicatedSnapshotSlaRetentionChangeEnabled)
                snapshotCount
                retentionSlaDomainId
                retentionSlaDomainName
                retentionSlaDomainRscManagedId
                region {
                  awsNativeRegion
                  azureNativeRegion
                  gcpNativeRegion
                  __typename
                }
                cloudAccountId
                cloudAccountName
                pendingSla {
                  ...SLADomainFragment
                  __typename
                }
                effectiveSlaDomain {
                  ...EffectiveSlaDomainFragment
                  __typename
                }
                cluster {
                  id
                  name
                  version
                  status
                  __typename
                }
                recoveryInfo {
                  isRefreshInProgressOpt
                  lastUpdatedTimeOpt
                  locationId
                  newWorkloadId
                  oldWorkloadId
                  __typename
                }
                __typename
              }
              __typename
            }
            pageInfo {
              endCursor
              hasPreviousPage
              hasNextPage
              __typename
            }
            __typename
          }
        }
        $($fragmentEffectiveSlaDomain)
        $($fragmentSLADomain)
        "

        $allFlatObjects = [System.Collections.ArrayList]::new() # Changed to ArrayList
        $afterCursor = $null
        $hasNextPage = $true
    }

    process {
        Write-Host "Fetching unmanaged *objects* for object '$ObjectId' on cluster '$ClusterUuid'..."
        
        try {
            do {
                # Build variables for the query
                $input = @{
                    "clusterUuid" = $ClusterUuid
                    "retentionSlaDomainIds" = $GUS_RetentionSlaDomainIds
                    "objectTypes" = $GUS_ObjectTypes
                    "unmanagedStatuses" = $GUS_UnmanagedStatuses
                    "sortParam" = $GUS_SortParam
                }

                if ($PSBoundParameters.ContainsKey('ObjectId') -and -not [string]::IsNullOrWhiteSpace($ObjectId)) {
                    $input.Add("objectId", $ObjectId)
                }

                $variables = @{
                    "isReplicatedSnapshotSlaRetentionChangeEnabled" = $IsReplicatedSnapshotSlaRetentionChangeEnabled
                    "input" = $input
                    "first" = $First
                    "after" = $afterCursor
                }
                
                $JSON_BODY = @{
                    "variables" = $variables
                    "query" = $query
                } | ConvertTo-Json -Depth 10
                
                # Make the API call
                $result = Invoke-WebRequest -Uri $GraphQL_URL -Method POST -Headers $Headers -Body $JSON_BODY
                $data = $result.Content | ConvertFrom-Json
                
                if ($data.errors) {
                    Write-Error "GraphQL query returned errors: $($data.errors | ConvertTo-Json -Depth 5)"
                    break # Exit loop on error
                }

                $pageObjects = $data.data.unmanagedObjects
                if ($pageObjects.edges) {
                    Write-Host "  Fetched $($pageObjects.edges.Count) objects..."
                    
                    # Flatten the results for CSV export and easier processing
                    foreach ($edge in $pageObjects.edges) {
                        $node = $edge.node

                        # Flatten nested properties
                        $physicalLocationNames = ($node.physicalLocation | ForEach-Object { $_.name }) -join ", "
                        $effectiveSlaDomainName = if ($node.effectiveSlaDomain) { $node.effectiveSlaDomain.name } else { $null }
                        $effectiveSlaDomainId = if ($node.effectiveSlaDomain) { $node.effectiveSlaDomain.id } else { $null }
                        $clusterName = if ($node.cluster) { $node.cluster.name } else { $null }
                        $clusterId = if ($node.cluster) { $node.cluster.id } else { $null }

                        $flatObj = [PSCustomObject]@{
                            ObjectId = $node.id # This is the object ID, not the snapshot ID
                            WorkloadId = $node.workloadId # This is the SnappableId for the next query
                            Name = $node.name
                            ObjectType = $node.objectType
                            UnmanagedStatus = $node.unmanagedStatus
                            SnapshotCount = $node.snapshotCount
                            LocalStorage = $node.localStorage
                            ArchiveStorage = $node.archiveStorage
                            CurrentEffectiveSlaName = $effectiveSlaDomainName
                            CurrentEffectiveSlaId = $effectiveSlaDomainId
                            PendingSlaName = if ($node.pendingSla) { $node.pendingSla.name } else { $null }
                            PendingSlaId = if ($node.pendingSla) { $node.pendingSla.id } else { $null }
                            RetentionSlaDomainId = $node.retentionSlaDomainId
                            RetentionSlaDomainName = $node.retentionSlaDomainName
                            PhysicalLocation = $physicalLocationNames
                            ClusterName = $clusterName
                            ClusterId = $clusterId
                            IsRemote = $node.isRemote
                        }
                        [void]$allFlatObjects.Add($flatObj) # Use [void] to suppress output from .Add()
                    }
                }

                $hasNextPage = $pageObjects.pageInfo.hasNextPage
                $afterCursor = $pageObjects.pageInfo.endCursor

            } while ($hasNextPage)
        }
        catch {
            Write-Error "Failed to fetch snapshot objects. Error: $($_)"
        }
    }

    end {
        Write-Host "Total objects processed: $($allFlatObjects.Count)"
        return $allFlatObjects
    }
}

function Get-SnapshotsForObject {
    # Fetches the detailed snapshot list for a single snappable object, handling pagination.
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$SnappableId,

        [parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [parameter(Mandatory=$true)]
        [string]$GraphQL_URL,

        [parameter(Mandatory=$false)]
        [string]$ParentUnmanagedStatus = "",

        [parameter(Mandatory=$false)]
        [string]$ParentObjectName = "",

        [parameter(Mandatory=$false)]
        [int]$First = 50,

        [parameter(Mandatory=$false)]
        [string]$SortBy = "CREATION_TIME",

        [parameter(Mandatory=$false)]
        [string]$SortOrder = "DESC",

        [parameter(Mandatory=$false)]
        [array]$SnapshotFilter = @( @{ "field" = "SNAPSHOT_TYPE"; "typeFilters" = @() } ),

        [parameter(Mandatory=$false)]
        [object]$TimeRange = $null,

        [parameter(Mandatory=$false)]
        [bool]$IsLegalHoldThroughRbacEnabled = $true,

        [parameter(Mandatory=$false)]
        [bool]$IsStaticRetentionEnabled = $true,

        [parameter(Mandatory=$false)]
        [bool]$IsBackupLocationSupported = $false,

        [parameter(Mandatory=$false)]
        [bool]$IncludeOnlySourceSnapshots = $false,

        [parameter(Mandatory=$false)]
        [bool]$IncludeSapHanaAppMetadata = $false,

        [parameter(Mandatory=$false)]
        [bool]$IncludeDb2AppMetadata = $false,

        [parameter(Mandatory=$false)]
        [bool]$IncludeMongoSourceAppMetadata = $false
    )

    begin {
        # Define GraphQL fragments required by this query
        $fragmentEffectiveSlaDomain = "
        fragment EffectiveSlaDomainFragment on SlaDomain {
          id
          name
          ... on GlobalSlaReply {
            isRetentionLockedSla
            retentionLockMode
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
        }"
        
        $fragmentSLADomain = "
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
        }"
        
        $fragmentCdmSnapshotLatestUserNotes = "
        fragment CdmSnapshotLatestUserNotesFragment on CdmSnapshot {
          latestUserNote {
            time
            userName
            userNote
            __typename
          }
          __typename
        }"
        
        $fragmentPolarisSnapshotRetentionInfo = "
        fragment PolarisSnapshotRetentionInfoFragment on PolarisSnapshot {
          isRetentionLocked
          archivalLocationName
          snapshotRetentionInfo {
            isCustomRetentionApplied
            localInfo {
              locationName
              expirationTime
              isExpirationDateCalculated
              isSnapshotPresent
              __typename
            }
            archivalInfos {
              locationName
              expirationTime
              isExpirationDateCalculated
              isSnapshotPresent
              __typename
            }
            replicationInfos {
              locationName
              expirationTime
              isExpirationDateCalculated
              isSnapshotPresent
              __typename
            }
            __typename
          }
          __typename
        }"

        # Define the main query
        $query = "
        query SnapshotsListSingleQuery(
            `$snappableId: String!, `$first: Int, `$after: String, `$snapshotFilter: [SnapshotQueryFilterInput!], 
            `$sortBy: SnapshotQuerySortByField, `$sortOrder: SortOrder, `$timeRange: TimeRangeInput, 
            `$includeSapHanaAppMetadata: Boolean!, `$includeDb2AppMetadata: Boolean!, `$includeMongoSourceAppMetadata: Boolean!, 
            `$isLegalHoldThroughRbacEnabled: Boolean = false, `$isStaticRetentionEnabled: Boolean = false, 
            `$isBackupLocationSupported: Boolean = false, `$includeOnlySourceSnapshots: Boolean = false
        ) {
          snapshotsListConnection: snapshotOfASnappableConnection(
            workloadId: `$snappableId, first: `$first, after: `$after, snapshotFilter: `$snapshotFilter, 
            sortBy: `$sortBy, sortOrder: `$sortOrder, timeRange: `$timeRange, 
            includeOnlySourceSnapshots: `$includeOnlySourceSnapshots
          ) {
            edges {
              cursor
              node {
                ...CdmSnapshotLatestUserNotesFragment
                id
                date
                expirationDate
                isOnDemandSnapshot
                ... on CdmSnapshot {
                  cdmVersion
                  isRetentionLocked
                  isDownloadedSnapshot
                  cluster {
                    id
                    name
                    version
                    status
                    timezone
                    __typename
                  }
                  pendingSnapshotDeletion {
                    id: snapshotFid
                    status
                    __typename
                  }
                  slaDomain {
                    ...EffectiveSlaDomainFragment
                    __typename
                  }
                  pendingSla {
                    ...SLADomainFragment
                    __typename
                  }
                  snapshotRetentionInfo {
                    isCustomRetentionApplied
                    archivalInfos {
                      name
                      isExpirationDateCalculated
                      expirationTime
                      locationId
                      isSnapshotOnLegalHold @include(if: `$isLegalHoldThroughRbacEnabled)
                      __typename
                    }
                    localInfo {
                      name
                      isExpirationDateCalculated
                      expirationTime
                      isSnapshotOnLegalHold @include(if: `$isLegalHoldThroughRbacEnabled)
                      __typename
                    }
                    replicationInfos {
                      name
                      isExpirationDateCalculated
                      expirationTime
                      locationId
                      isExpirationInformationUnavailable
                      isSnapshotOnLegalHold @include(if: `$isLegalHoldThroughRbacEnabled)
                      __typename
                    }
                    __typename
                  }
                  sapHanaAppMetadata @include(if: `$includeSapHanaAppMetadata) {
                    backupId
                    backupPrefix
                    snapshotType
                    files {
                      backupFileSizeInBytes
                      __typename
                    }
                    __typename
                  }
                  db2AppMetadata @include(if: `$includeDb2AppMetadata) {
                    backupId
                    snapshotType
                    files {
                      backupFileSizeInBytes
                      __typename
                    }
                    __typename
                  }
                  mongoSourceAppMetadata @include(if: `$includeMongoSourceAppMetadata) {
                    isFullSnapshot
                    snapshotSize
                    __typename
                  }
                  legalHoldInfo {
                    shouldHoldInPlace
                    __typename
                  }
                  __typename
                }
                ... on PolarisSnapshot {
                  archivalLocationId
                  archivalLocationName
                  isDeletedFromSource
                  isDownloadedSnapshot
                  isReplica
                  isArchivalCopy
                  snappableId
                  slaDomain {
                    name
                    ...EffectiveSlaDomainFragment
                    ... on ClusterSlaDomain {
                      fid
                      cluster {
                        id
                        name
                        __typename
                      }
                      __typename
                    }
                    ... on GlobalSlaReply {
                      id
                      __typename
                    }
                    __typename
                  }
                  pendingSla {
                    ...SLADomainFragment
                    __typename
                  }
                  polarisSpecificSnapshot @include(if: `$isBackupLocationSupported) {
                    snapshotId
                    __typename
                    ... on AwsNativeS3SpecificSnapshot {
                      snapshotStartTime
                      __typename
                    }
                  }
                  ...PolarisSnapshotRetentionInfoFragment @include(if: `$isStaticRetentionEnabled)
                  __typename
                }
                __typename
              }
              __typename
            }
            pageInfo {
              endCursor
              hasNextPage
              __typename
            }
            __typename
          }
        }
        $($fragmentEffectiveSlaDomain)
        $($fragmentSLADomain)
        $($fragmentCdmSnapshotLatestUserNotes)
        $($fragmentPolarisSnapshotRetentionInfo)
        "

        $allFlatSnapshots = [System.Collections.ArrayList]::new() # Changed to ArrayList
        $afterCursor = $null
        $hasNextPage = $true
    }

    process {
        Write-Host "  Fetching snapshots for SnappableId '$SnappableId'..."
        
        try {
            do {
                # Build variables for the query
                $variables = @{
                    "isLegalHoldThroughRbacEnabled" = $IsLegalHoldThroughRbacEnabled
                    "isStaticRetentionEnabled" = $IsStaticRetentionEnabled
                    "isBackupLocationSupported" = $IsBackupLocationSupported
                    "includeOnlySourceSnapshots" = $IncludeOnlySourceSnapshots
                    "snappableId" = $SnappableId
                    "first" = $First
                    "sortBy" = $SortBy
                    "sortOrder" = $SortOrder
                    "includeSapHanaAppMetadata" = $IncludeSapHanaAppMetadata
                    "includeDb2AppMetadata" = $IncludeDb2AppMetadata
                    "includeMongoSourceAppMetadata" = $IncludeMongoSourceAppMetadata
                    "snapshotFilter" = $SnapshotFilter
                    "timeRange" = $TimeRange
                    "after" = $afterCursor
                }
                
                $JSON_BODY = @{
                    "variables" = $variables
                    "query" = $query
                } | ConvertTo-Json -Depth 10
                
                # Make the API call
                $result = Invoke-WebRequest -Uri $GraphQL_URL -Method POST -Headers $Headers -Body $JSON_BODY
                $data = $result.Content | ConvertFrom-Json
                
                if ($data.errors) {
                    Write-Error "  GraphQL query returned errors for $SnappableId $($data.errors | ConvertTo-Json -Depth 5)"
                    break # Exit loop on error
                }

                $pageData = $data.data.snapshotsListConnection
                if ($pageData.edges) {
                    Write-Host "    ...fetched $($pageData.edges.Count) snapshots this page."
                    
                    # Flatten the results for CSV export
                    foreach ($edge in $pageData.edges) {
                        $node = $edge.node

                        # --- Parse Expiration Dates ---
                        $localExpiration = "Forever" # Default to "Forever"
                        if ($node.snapshotRetentionInfo -and $node.snapshotRetentionInfo.localInfo -and $node.snapshotRetentionInfo.localInfo.expirationTime) {
                            $localExpiration = $node.snapshotRetentionInfo.localInfo.expirationTime
                        }

                        # Handle Archival Expirations (it's an array)
                        $archivalExpirations = "N/A" # Default if no archives
                        if ($node.snapshotRetentionInfo -and $node.snapshotRetentionInfo.archivalInfos -and $node.snapshotRetentionInfo.archivalInfos.Count -gt 0) {
                            $archivalExpirationStrings = $node.snapshotRetentionInfo.archivalInfos | ForEach-Object {
                                $locName = if ($_.name) { $_.name } else { "UnknownLocation" }
                                $expTime = if ($_.expirationTime) { $_.expirationTime } else { "Forever" }
                                "$locName $expTime"
                            }
                            $archivalExpirations = $archivalExpirationStrings -join "; "
                        }

                        # Handle Replication Expirations (it's an array)
                        $replicationExpirations = "N/A" # Default if no replicas
                        if ($node.snapshotRetentionInfo -and $node.snapshotRetentionInfo.replicationInfos -and $node.snapshotRetentionInfo.replicationInfos.Count -gt 0) {
                            $replicationExpirationStrings = $node.snapshotRetentionInfo.replicationInfos | ForEach-Object {
                                $locName = if ($_.name) { $_.name } else { "UnknownLocation" }
                                $expTime = if ($_.expirationTime) { $_.expirationTime } else { "Forever" }
                                "$locName $expTime"
                            }
                            $replicationExpirations = $replicationExpirationStrings -join "; "
                        }
                        # --- End Parse Expiration Dates ---

                        $flatSnap = [PSCustomObject]@{
                            ObjectName = $ParentObjectName
                            SnapshotId = $node.id
                            SnapshotDate = $node.date
                            LocalExpiration = $localExpiration
                            ArchivalExpirations = $archivalExpirations
                            ReplicationExpirations = $replicationExpirations
                            UnmanagedStatus = $ParentUnmanagedStatus # Added this field
                            IsOnDemand = $node.isOnDemandSnapshot
                            IsRetentionLocked = $node.isRetentionLocked
                            SlaId = if ($node.slaDomain) { $node.slaDomain.id } else { $null }
                            SlaName = if ($node.slaDomain) { $node.slaDomain.name } else { $null }
                            PendingSlaName = if ($node.pendingSla) { $node.pendingSla.name } else { $null }
                            IsCustomRetention = if ($node.snapshotRetentionInfo) { $node.snapshotRetentionInfo.isCustomRetentionApplied } else { $null }
                            ClusterName = if ($node.cluster) { $node.cluster.name } else { $null }
                            ClusterId = if ($node.cluster) { $node.cluster.id } else { $null }
                            SnappableId = $SnappableId # Add the parent ID for reference
                        }
                        [void]$allFlatSnapshots.Add($flatSnap) # Use [void] to suppress output from .Add()
                    }
                }

                $hasNextPage = $pageData.pageInfo.hasNextPage
                $afterCursor = $pageData.pageInfo.endCursor

            } while ($hasNextPage)
        }
        catch {
            Write-Error "  Failed to fetch snapshots for $SnappableId. Error: $($_)"
        }
    }

    end {
        Write-Host "  Total snapshots found for $SnappableId $($allFlatSnapshots.Count)" # Removed colon
        return $allFlatSnapshots
    }
}


function Set-SnapshotRetention {
    # Applies the specified retention SLA to a single snapshot.
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$SnapshotId,

        [parameter(Mandatory=$true)]
        [string]$NewSlaId,

        [parameter(Mandatory=$true)]
        [hashtable]$Headers,

        [parameter(Mandatory=$true)]
        [string]$GraphQL_URL,

        [parameter(Mandatory=$false)]
        [string]$UserNote = ""
    )

    process {
        Write-Host "Setting retention for Snapshot '$SnapshotId' to SLA '$NewSlaId'..."

        $query = "mutation ChangeSnapshotsRetentionMutation(`$globalSlaAssignType: SlaAssignTypeEnum!, `$snapshotFids: [UUID!]!, `$globalSlaOptionalFid: UUID, `$userNote: String) {
          assignRetentionSLAToSnapshots(globalSlaAssignType: `$globalSlaAssignType, snapshotFids: `$snapshotFids, globalSlaOptionalFid: `$globalSlaOptionalFid, userNote: `$userNote) {
            success
            __typename
          }
        }"

        $variables = @{
            "userNote" = $UserNote
            "globalSlaAssignType" = "protectWithSlaId"
            "snapshotFids" = @( $SnapshotId ) # API expects an array of snapshot FIDs
            "globalSlaOptionalFid" = $NewSlaId
        }
        
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        } | ConvertTo-Json -Depth 5
        
        try {
           $result = Invoke-WebRequest -Uri $GraphQL_URL -Method POST -Headers $Headers -Body $JSON_BODY
           $data = $result.Content | ConvertFrom-Json
           
           if ($data.errors) {
                Write-Error "  GraphQL returned an error for snapshot $SnapshotId $($data.errors | ConvertTo-Json -Depth 5)"
           }
           elseif ($data.data.assignRetentionSLAToSnapshots -and $data.data.assignRetentionSLAToSnapshots.success) {
               Write-Host "  Successfully updated retention for snapshot $SnapshotId."
           }
           else {
               Write-Warning "  Failed to update snapshot $SnapshotId. API success was not true or response was unexpected."
           }
        }
        catch {
           Write-Error "  Failed to submit update for snapshot $SnapshotId. Error: $($_)"
        }
    }
}

# --- Main Script Logic ---

# Read and parse the service account file
try {
    $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
}
catch {
    Write-Error "Failed to read or parse Service Account JSON file at '$ServiceAccountJson'. Error: $($_)"
    return
}

# Connect to Polaris
$polSession = Connect-Polaris -ServiceAccountObject $serviceAccountObj
if (-not $polSession) {
    Write-Error "Authentication failed. Exiting script."
    return
}

# Set up common variables
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$GraphQL_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$LogoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

Write-Host "Authentication successful."

$snapshotsToUpdate = [System.Collections.ArrayList]::new() # Changed to ArrayList
$allDetailedSnapshots = [System.Collections.ArrayList]::new() # Changed to ArrayList

# --- New Workflow: Import from CSV ---
if ($PSBoundParameters.ContainsKey('ImportCsvPath') -and -not [string]::IsNullOrWhiteSpace($ImportCsvPath)) {
    Write-Host "Importing snapshots from CSV at '$ImportCsvPath'..."
    
    if (-not ($PSBoundParameters.ContainsKey('NewSlaId') -and -not [string]::IsNullOrWhiteSpace($NewSlaId))) {
        Write-Error "Using -ImportCsvPath requires the -NewSlaId parameter to be specified."
        Disconnect-Polaris -Headers $headers -LogoutUrl $LogoutUrl
        return
    }

    try {
        $snapshotsFromCsv = Import-Csv -Path $ImportCsvPath
        
        if ($null -eq $snapshotsFromCsv -or $snapshotsFromCsv.Count -eq 0) {
            Write-Warning "CSV file at '$ImportCsvPath' is empty or could not be read. No snapshots to update."
        }
        elseif (-not ($snapshotsFromCsv[0].PSObject.Properties.Name -contains 'SnapshotId')) {
            Write-Error "CSV file must contain a 'SnapshotId' column. Aborting update."
        }
        else {
            $snapshotIdsFromCsv = $snapshotsFromCsv | ForEach-Object { $_.SnapshotId } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            [void]$snapshotsToUpdate.AddRange($snapshotIdsFromCsv)
            Write-Host "Successfully imported $($snapshotIdsFromCsv.Count) snapshot IDs from CSV for processing."
        }
    }
    catch {
        Write-Error "Failed to read CSV from '$ImportCsvPath'. Error: $($_)"
    }
}
# --- Standard Workflow: Query or Manual IDs ---
else {
    # Add manually provided snapshot IDs
    if ($PSBoundParameters.ContainsKey('SnapshotIds')) {
        [void]$snapshotsToUpdate.AddRange($SnapshotIds) # Use [void] to suppress output
        Write-Host "Added $($SnapshotIds.Count) manually specified snapshot IDs."
    }

    # Check for -RelicObjectsOnly switch
    if ($RelicObjectsOnly.IsPresent) {
        Write-Host "Filtering for Relic Objects only." -ForegroundColor Yellow
        $FilterUnmanagedStatuses = @("RELIC", "REPLICATED_RELIC", "REMOTE_UNPROTECTED")
    }

    # Query for objects if ClusterUuid is provided
    if ($PSBoundParameters.ContainsKey('ClusterUuid')) {
        Write-Host "Querying for objects based on ClusterUuid '$ClusterUuid'..."
        
        $queryArgs = @{
            "ClusterUuid" = $ClusterUuid
            "GUS_RetentionSlaDomainIds" = $FilterRetentionSlaDomainIds
            "GUS_ObjectTypes" = $FilterObjectTypes
            "GUS_UnmanagedStatuses" = $FilterUnmanagedStatuses
            "GUS_SortParam" = $SortParam
            "Headers" = $headers
            "GraphQL_URL" = $GraphQL_URL
        }

        if ($PSBoundParameters.ContainsKey('ObjectId') -and -not [string]::IsNullOrWhiteSpace($ObjectId)) {
            Write-Host "  ...and filtering by ObjectId '$ObjectId'."
            $queryArgs.Add("ObjectId", $ObjectId)
        }
        
        # 1. Get the list of objects
        $foundObjects = Get-UnmanagedSnapshots @queryArgs
        
        if ($foundObjects -and $foundObjects.Count -gt 0) {
            Write-Host "Successfully queried $($foundObjects.Count) object(s)."
            
            $summaryList = [System.Collections.ArrayList]::new()

            # 2. Loop through each object and get its detailed snapshots
            Write-Host "Now fetching detailed snapshots for each object..."
            foreach ($object in $foundObjects) {
                # Pass the parent object's UnmanagedStatus and Name to the snapshot function
                $detailedSnapshots = Get-SnapshotsForObject -SnappableId $object.WorkloadId -Headers $headers -GraphQL_URL $GraphQL_URL -ParentUnmanagedStatus $object.UnmanagedStatus -ParentObjectName $object.Name
                
                $snapshotCount = 0
                if ($detailedSnapshots -and $detailedSnapshots.Count -gt 0) {
                    [void]$allDetailedSnapshots.AddRange(@($detailedSnapshots)) # Use [void] to suppress output
                    $snapshotCount = $detailedSnapshots.Count
                }
                
                # Add to summary
                [void]$summaryList.Add([PSCustomObject]@{
                    ObjectId = $object.ObjectId
                    ObjectName = $object.Name
                    UnmanagedStatus = $object.UnmanagedStatus
                    SnapshotCount = $snapshotCount
                })
            }
            Write-Host "Total detailed snapshots found: $($allDetailedSnapshots.Count)"

            # --- Print Summary ---
            Write-Host "`n--- Snapshot Query Summary ---" -ForegroundColor Green
            $summaryList | Format-Table -AutoSize
            Write-Host "------------------------------`n" -ForegroundColor Green


            # 3. Export to CSV if path is provided
            if ($PSBoundParameters.ContainsKey('ExportCsvPath') -and -not [string]::IsNullOrWhiteSpace($ExportCsvPath)) {
                try {
                    Write-Host "Exporting detailed snapshot list to '$ExportCsvPath'..."
                    $allDetailedSnapshots | Export-Csv -Path $ExportCsvPath -NoTypeInformation -Encoding UTF8
                    Write-Host "Export complete."
                }
                catch {
                    Write-Error "Failed to export CSV file. Error: $($_)"
                }
            }
            
            # 4. Add found snapshot IDs to the list for update
            $foundSnapshotIds = $allDetailedSnapshots | ForEach-Object { $_.SnapshotId }
            [void]$snapshotsToUpdate.AddRange($foundSnapshotIds) # Use [void] to suppress output
        }
        else {
            Write-Host "No objects found matching the query criteria."
        }
    }
}


# Check if there is anything to update
if ($snapshotsToUpdate.Count -eq 0) {
    Write-Warning "No snapshot IDs were provided or found. Nothing to update. Exiting."
}
else {
    # De-duplicate the list
    $uniqueSnapshotIds = $snapshotsToUpdate | Sort-Object -Unique
    
    if ($PSBoundParameters.ContainsKey('NewSlaId') -and -not [string]::IsNullOrWhiteSpace($NewSlaId)) {
        Write-Host "Starting snapshot update process for $($uniqueSnapshotIds.Count) unique snapshot(s)..."

        # Loop through each snapshot and apply the new SLA
        foreach ($snapshotId in $uniqueSnapshotIds) {
            Set-SnapshotRetention -SnapshotId $snapshotId -NewSlaId $NewSlaId -Headers $headers -GraphQL_URL $GraphQL_URL -UserNote $UserNote
        }
        
        Write-Host "All snapshot updates submitted."
    }
    else {
        Write-Host "Snapshots were found, but -NewSlaId was not provided. Skipping retention update."
        if (-not $PSBoundParameters.ContainsKey('ExportCsvPath')) {
            Write-Warning "You did not specify -NewSlaId or -ExportCsvPath, so no action was taken on the found snapshots."
        }
    }
}

Write-Host "Process complete. Logging out..."
# Disconnect from Polaris
Disconnect-Polaris -Headers $headers -LogoutUrl $LogoutUrl
