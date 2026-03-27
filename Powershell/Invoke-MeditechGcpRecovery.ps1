<#
.SYNOPSIS
    Guided mass recovery workflow for Meditech GCP instances via Rubrik Security Cloud.

.DESCRIPTION
    Invoke-MeditechGcpRecovery.ps1 provides an interactive, step-by-step recovery workflow
    for Meditech environments hosted on GCP and protected by Rubrik Security Cloud (RSC).

    Two recovery modes are supported:

      Restore (in-place)     Overwrites the existing GCE instance from the chosen snapshot.
                             The original VM is replaced in-place. No new instance is created.
                             WARNING: This operation is destructive and irreversible.

      Export (out-of-place)  Clones the GCE instance from a snapshot into a target
                             project/zone. The original VM is unaffected. Use for DR testing,
                             side-by-side validation, or building a parallel recovery environment.

    POINT-IN-TIME SELECTION
    -----------------------
    Supply -TargetDateTime to specify a recovery target. The script automatically finds
    the closest snapshot at or before that date/time for each selected VM via RSC.

    If no snapshot exists before the target datetime for a given VM, the script warns and
    presents two options: use the oldest available snapshot, or skip that VM.

    WORKFLOW (RunRecovery mode)
    --------------------------
    Step 1  Authenticate to RSC using the service account from -MbfConfigXml or -ServiceAccountJson.
    Step 2  Retrieve GCE instance inventory from RSC (optionally filtered by -GcpProjectId).
    Step 3  Select target VMs (-VmNames or interactive numbered list).
    Step 4  Determine the recovery target date/time (-TargetDateTime or interactive prompt).
    Step 5  Resolve the closest snapshot at or before the target for each VM.
    Step 6  Display the recovery plan and prompt for confirmation.
    Step 7  Select recovery type: Restore or Export.
    Step 8  For Export: collect target project, zone, machine type, subnet, service account.
    Step 9  Type "CONFIRM" to execute recovery operations.
    Step 10 Report RSC job IDs for tracking in the portal Activity Log.

    LIST MODE
    ---------
    Use -ListSnapshots to inspect available recovery points for one or more VMs without
    initiating recovery. Useful for planning or verifying backup coverage before a recovery.

.PARAMETER MbfConfigXml
    Path to the DPAPI-encrypted credential XML created by New-MeditechCredentials.ps1.
    The RSC service account fields in this file are used for authentication.
    MBF credentials stored in the file are not used by this script.
    Default: C:\ProgramData\Rubrik\MeditechCreds.xml

.PARAMETER ServiceAccountJson
    Alternative to -MbfConfigXml. Path to an RSC service account JSON file containing
    client_id, client_secret, and access_token_uri.

.PARAMETER GcpProjectId
    RSC UUID of the GCP project to filter the VM inventory search.
    Can be embedded in -MbfConfigXml (set via New-MeditechCredentials.ps1).
    Use -ListProjects on MeditechMasterScript.ps1 to find available project UUIDs.
    If omitted and not in the XML, all visible GCP projects are searched.

.PARAMETER VmNames
    One or more GCE instance names to target for recovery.
    If omitted, the script presents the full inventory for interactive selection.

.PARAMETER TargetDateTime
    Recovery point target as a parseable date/time string.
    The script selects the closest snapshot at or before this time for each VM.
    Examples: "2026-03-25 13:00"   "2026-03-25T02:00:00"   "03/25/2026 2:00 AM"
    If omitted, prompted interactively.

.PARAMETER RecoveryType
    Recovery mode: "Restore" (in-place) or "Export" (out-of-place clone).
    If omitted, prompted interactively with descriptions of each mode.

.PARAMETER TargetZone
    [Export only] GCP availability zone for recovered instances (e.g. "us-central1-a").
    Prompted interactively if not provided.

.PARAMETER TargetProjectNativeId
    [Export only] GCP native project ID of the target recovery environment
    (e.g. "my-recovery-project-id"). Used as sharedVpcHostProjectNativeId.
    Prompted interactively (with project list) if not provided.

.PARAMETER TargetProjectRubrikId
    [Export only] RSC UUID of the target GCP project.
    Prompted interactively (with project list) if not provided.

.PARAMETER TargetMachineType
    [Export only] GCP machine type for recovered instances (e.g. "e2-standard-4").
    Prompted interactively if not provided.

.PARAMETER TargetSubnetName
    [Export only] GCP subnet name for recovered instances (e.g. "meditech-uscentral1-a").
    Prompted interactively if not provided.

.PARAMETER ServiceAccountEmail
    [Export only] GCP service account email attached to recovered instances
    (e.g. "compute@my-project.iam.gserviceaccount.com").
    Prompted interactively if not provided.

.PARAMETER InstanceNameSuffix
    [Export only] Suffix appended to each source VM name to form the recovered instance name.
    Must produce a valid GCE instance name after sanitization (lowercase, hyphens, ≤63 chars).
    Default: "-rcv-<YYYYMMDD>" (e.g. MTFS01 → mtfs01-rcv-20260325).

.PARAMETER ListSnapshots
    Switch. Lists available recovery points for selected VMs and exits without recovering.
    Use to verify backup coverage or plan a recovery before committing.

.PARAMETER SnapshotListCount
    Number of snapshots to display per VM in -ListSnapshots mode. Default: 10.

.PARAMETER EnableLogging
    Switch. Appends timestamped log entries to the file at -LogPath.
    Recommended for all recovery operations to preserve an audit trail.

.PARAMETER LogPath
    Path to the log file. The directory is created automatically if it does not exist.
    Log files exceeding 2 MB are rotated with a timestamp suffix.
    Default: C:\ProgramData\Rubrik\Logs\MeditechRecovery.log

.EXAMPLE
    # Fully guided — prompts for all required values interactively.
    .\Invoke-MeditechGcpRecovery.ps1 -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml"

.EXAMPLE
    # List available snapshots for a specific VM before committing to recovery.
    .\Invoke-MeditechGcpRecovery.ps1 `
        -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" `
        -VmNames "MTFS01" `
        -ListSnapshots -SnapshotListCount 20

.EXAMPLE
    # In-place restore of specific VMs to the closest snapshot before 2 AM.
    .\Invoke-MeditechGcpRecovery.ps1 `
        -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" `
        -VmNames "MTFS01","MTFS02" `
        -TargetDateTime "2026-03-25 02:00" `
        -RecoveryType Restore `
        -EnableLogging

.EXAMPLE
    # Export (clone) all Meditech VMs in a project to a DR environment.
    .\Invoke-MeditechGcpRecovery.ps1 `
        -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" `
        -GcpProjectId "478ddd35-fe8f-4818-bddd-09d9b56dfb54" `
        -TargetDateTime "2026-03-24 23:59" `
        -RecoveryType Export `
        -TargetZone "us-central1-a" `
        -TargetProjectNativeId "my-dr-project" `
        -TargetProjectRubrikId "00000000-1111-2222-3333-444444444444" `
        -TargetMachineType "e2-standard-4" `
        -TargetSubnetName "dr-subnet-uscentral1" `
        -ServiceAccountEmail "compute@my-dr-project.iam.gserviceaccount.com" `
        -EnableLogging

.NOTES
    Author  : Marcus Henderson
    Created : March 2026
    Company : Rubrik Inc

    PREREQUISITES
    -------------
    - Windows PowerShell 5.1 or later.
    - RSC service account with GCP inventory read, restore, and export permissions.
    - When using -MbfConfigXml, run as the same Windows account that created the XML
      (DPAPI decryption is bound to the user + machine that created the file).
    - Run New-MeditechCredentials.ps1 once to create the encrypted credential XML.

    RELATED SCRIPTS
    ---------------
    MeditechMasterScript.ps1    — Backup orchestration (quiesce, snapshot, unquiesce).
    New-MeditechCredentials.ps1 — One-time credential setup wizard.
#>
[CmdletBinding(DefaultParameterSetName = "RunRecovery")]
param (
    # --- Authentication ---
    [Parameter(ParameterSetName = "RunRecovery")]
    [Parameter(ParameterSetName = "ListSnapshots")]
    [string]$MbfConfigXml = "C:\ProgramData\Rubrik\MeditechCreds.xml",

    [Parameter(ParameterSetName = "RunRecovery")]
    [Parameter(ParameterSetName = "ListSnapshots")]
    [string]$ServiceAccountJson,

    # --- Targeting ---
    [Parameter(ParameterSetName = "RunRecovery")]
    [Parameter(ParameterSetName = "ListSnapshots")]
    [string]$GcpProjectId,

    [Parameter(ParameterSetName = "RunRecovery")]
    [Parameter(ParameterSetName = "ListSnapshots")]
    [string[]]$VmNames,

    # --- Recovery Point ---
    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetDateTime,

    # --- Recovery Mode ---
    [Parameter(ParameterSetName = "RunRecovery")]
    [ValidateSet("Restore", "Export")]
    [string]$RecoveryType,

    # --- Export Parameters (all optional; prompted if Export selected) ---
    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetZone,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetProjectNativeId,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetProjectRubrikId,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetMachineType,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$TargetSubnetName,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$ServiceAccountEmail,

    [Parameter(ParameterSetName = "RunRecovery")]
    [string]$InstanceNameSuffix,

    # --- List Mode ---
    [Parameter(ParameterSetName = "ListSnapshots", Mandatory = $true)]
    [switch]$ListSnapshots,

    [Parameter(ParameterSetName = "ListSnapshots")]
    [int]$SnapshotListCount = 10,

    # --- Logging ---
    [Parameter()]
    [switch]$EnableLogging,

    [Parameter()]
    [string]$LogPath = "C:\ProgramData\Rubrik\Logs\MeditechRecovery.log"
)

# =============================================================================
# LOGGING HELPERS
# =============================================================================

function Get-Timestamp {
    return Get-Date -Format "yyyy-MM-dd HH:mm:ss"
}

function Write-Log {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$Message,

        [Parameter(Position = 1)]
        [ConsoleColor]$ForegroundColor,

        [string]$Level = "INFO"
    )

    $ts = Get-Timestamp

    if ($script:EnableLogging -and $script:LogPath) {
        $logLine = "[$ts] [$Level] $Message"
        try { Add-Content -Path $script:LogPath -Value $logLine -ErrorAction SilentlyContinue } catch {}
    }

    switch ($Level) {
        "INFO" {
            $consoleMsg = "[$ts] $Message"
            if ($ForegroundColor) { Write-Host $consoleMsg -ForegroundColor $ForegroundColor }
            else { Write-Host $consoleMsg }
        }
        "WARNING" { Write-Warning "[$ts] $Message" }
        "ERROR"   { Write-Error   "[$ts] $Message" }
    }
}

# =============================================================================
# RSC CONNECTION FUNCTIONS
# =============================================================================

function Connect-Rsc {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [PSCustomObject]$ServiceAccount
    )

    $connectionData = [ordered]@{
        'client_id'     = $ServiceAccount.client_id
        'client_secret' = $ServiceAccount.client_secret
    } | ConvertTo-Json

    try {
        $rscSession = Invoke-RestMethod -Method Post -Uri $ServiceAccount.access_token_uri `
            -ContentType application/json -Body $connectionData
        if ($rscSession.access_token) { return $rscSession }
        else { Throw "Unable to obtain access token from RSC." }
    }
    catch { Throw "Failed to connect to RSC: $_" }
}

function Disconnect-Rsc {
    [CmdletBinding()]
    param (
        [string]$LogoutUrl,
        [hashtable]$Headers
    )

    try {
        if ($LogoutUrl) {
            Invoke-WebRequest -Method Delete -Headers $Headers -Uri $LogoutUrl `
                -ContentType "application/json; charset=utf-8" -ErrorAction SilentlyContinue | Out-Null
        }
    }
    catch { Write-Log "Logout failed (session may have already expired): $_" -Level Warning }
}

# =============================================================================
# RSC GRAPHQL FUNCTIONS
# =============================================================================

function Get-RscGcpInventory {
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$GcpProjectId = $null
    )

    $query = @"
query GCPInstancesListQuery(`$first: Int, `$after: String, `$filters: GcpNativeGceInstanceFilters) {
  gcpNativeGceInstances(first: `$first, after: `$after, gceInstanceFilters: `$filters) {
    edges {
      node {
        id
        nativeId
        nativeName
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"@

    $filterObj = @{ "relicFilter" = @{ "relic" = $false } }
    if (-not [string]::IsNullOrEmpty($GcpProjectId)) {
        $filterObj["projectFilter"] = @{ "projectIds" = @($GcpProjectId) }
    }

    $variables    = @{ "first" = 50; "filters" = $filterObj }
    $allInstances = @()
    $hasNextPage  = $true

    while ($hasNextPage) {
        $payload  = @{ query = $query; variables = $variables }
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"

        if ($response.errors) { Throw "GraphQL Error retrieving inventory: $($response.errors.message)" }
        if ($response.data.gcpNativeGceInstances.edges.node) {
            $allInstances += $response.data.gcpNativeGceInstances.edges.node
        }

        $hasNextPage = $response.data.gcpNativeGceInstances.pageInfo.hasNextPage
        if ($hasNextPage) { $variables["after"] = $response.data.gcpNativeGceInstances.pageInfo.endCursor }
    }

    return $allInstances
}

function Get-RscGcpProjects {
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers
    )

    $query = @"
query GCloudProjectsListQuery(`$first: Int!, `$after: String, `$sortBy: GcpNativeProjectSortFields, `$sortOrder: SortOrder!) {
  gcpNativeProjects(first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
    edges {
      node {
        id
        name
        nativeId
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"@

    $variables   = @{ "first" = 50; "sortBy" = "NAME"; "sortOrder" = "ASC" }
    $allProjects = @()
    $hasNextPage = $true

    while ($hasNextPage) {
        $payload  = @{ query = $query; variables = $variables }
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"

        if ($response.errors) { Throw "GraphQL Error retrieving GCP projects: $($response.errors.message)" }
        if ($response.data.gcpNativeProjects.edges.node) {
            $allProjects += $response.data.gcpNativeProjects.edges.node
        }

        $hasNextPage = $response.data.gcpNativeProjects.pageInfo.hasNextPage
        if ($hasNextPage) { $variables["after"] = $response.data.gcpNativeProjects.pageInfo.endCursor }
    }

    return $allProjects | Select-Object name, nativeId, id | Sort-Object name
}

function Get-RscVmSnapshots {
    # Returns up to MaxResults snapshots for a VM, sorted newest first.
    # Used by -ListSnapshots mode.
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$SnappableId,
        [int]$MaxResults = 50
    )

    $query = @"
query GcpVmSnapshotsQuery(`$snappableId: String!, `$first: Int, `$after: String, `$sortBy: SnapshotQuerySortByField, `$sortOrder: SortOrder) {
  snapshotOfASnappableConnection(workloadId: `$snappableId, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder, includeOnlySourceSnapshots: true) {
    edges {
      cursor
      node {
        id
        date
        expirationDate
        isOnDemandSnapshot
        ... on PolarisSnapshot {
          isDeletedFromSource
          isReplica
          slaDomain {
            id
            name
          }
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"@

    $variables    = @{
        "snappableId" = $SnappableId
        "first"       = [Math]::Min(50, $MaxResults)
        "sortBy"      = "CREATION_TIME"
        "sortOrder"   = "DESC"
    }
    $allSnapshots = @()
    $hasNextPage  = $true

    while ($hasNextPage -and ($allSnapshots.Count -lt $MaxResults)) {
        $payload = @{ query = $query; variables = $variables }

        try {
            $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
                -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
        }
        catch { Throw "Failed to retrieve snapshots for '$SnappableId': $_" }

        if ($response.errors) { Throw "GraphQL Error retrieving snapshots: $($response.errors.message)" }
        if ($response.data.snapshotOfASnappableConnection.edges.node) {
            $allSnapshots += $response.data.snapshotOfASnappableConnection.edges.node
        }

        $hasNextPage = $response.data.snapshotOfASnappableConnection.pageInfo.hasNextPage
        if ($hasNextPage) {
            $variables["after"] = $response.data.snapshotOfASnappableConnection.pageInfo.endCursor
        }
    }

    return $allSnapshots
}

function Get-RscBestSnapshot {
    # Returns the single closest snapshot at or before TargetDateTime, or $null if none exists.
    # Uses a server-side timeRange filter so only one page of results is ever fetched.
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$SnappableId,
        [datetime]$TargetDateTime
    )

    $query = @"
query GcpBestSnapshotQuery(`$snappableId: String!, `$first: Int, `$sortBy: SnapshotQuerySortByField, `$sortOrder: SortOrder, `$timeRange: TimeRangeInput) {
  snapshotOfASnappableConnection(workloadId: `$snappableId, first: `$first, sortBy: `$sortBy, sortOrder: `$sortOrder, timeRange: `$timeRange, includeOnlySourceSnapshots: true) {
    edges {
      node {
        id
        date
        expirationDate
        isOnDemandSnapshot
        ... on PolarisSnapshot {
          isDeletedFromSource
          isReplica
          slaDomain {
            id
            name
          }
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"@

    # Server-side time range: everything from epoch up to (and including) the target datetime.
    $variables = @{
        "snappableId" = $SnappableId
        "first"       = 1
        "sortBy"      = "CREATION_TIME"
        "sortOrder"   = "DESC"
        "timeRange"   = @{
            "start" = "2000-01-01T00:00:00.000Z"
            "end"   = $TargetDateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        }
    }

    $payload = @{ query = $query; variables = $variables }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
    }
    catch { Throw "Failed to query best snapshot for '$SnappableId': $_" }

    if ($response.errors) { Throw "GraphQL Error finding best snapshot: $($response.errors.message)" }

    $edges = $response.data.snapshotOfASnappableConnection.edges
    if ($edges -and $edges.Count -gt 0) {
        return $edges[0].node
    }
    return $null
}

function Get-RscOldestSnapshot {
    # Returns the single oldest snapshot for a VM (used as fallback when no snapshot exists before target).
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$SnappableId
    )

    $query = @"
query GcpOldestSnapshotQuery(`$snappableId: String!, `$first: Int, `$sortBy: SnapshotQuerySortByField, `$sortOrder: SortOrder) {
  snapshotOfASnappableConnection(workloadId: `$snappableId, first: `$first, sortBy: `$sortBy, sortOrder: `$sortOrder, includeOnlySourceSnapshots: true) {
    edges {
      node {
        id
        date
        expirationDate
        isOnDemandSnapshot
        ... on PolarisSnapshot {
          isDeletedFromSource
          isReplica
          slaDomain {
            id
            name
          }
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"@

    $variables = @{
        "snappableId" = $SnappableId
        "first"       = 1
        "sortBy"      = "CREATION_TIME"
        "sortOrder"   = "ASC"   # Ascending → first result is the oldest snapshot
    }

    $payload = @{ query = $query; variables = $variables }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
    }
    catch { Throw "Failed to query oldest snapshot for '$SnappableId': $_" }

    if ($response.errors) { Throw "GraphQL Error finding oldest snapshot: $($response.errors.message)" }

    $edges = $response.data.snapshotOfASnappableConnection.edges
    if ($edges -and $edges.Count -gt 0) {
        return $edges[0].node
    }
    return $null
}

# =============================================================================
# RECOVERY MUTATION FUNCTIONS
# =============================================================================

function Invoke-GcpRestore {
    # In-place restore: overwrites the existing GCE instance from the snapshot.
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$SnapshotId,
        [bool]$ShouldRestoreLabels        = $true,
        [bool]$ShouldAddRubrikLabels      = $true,
        [bool]$ShouldStartRestoredInstance = $true
    )

    $mutation = @"
mutation RestoreGCPInstanceMutation(`$input: GcpNativeRestoreGceInstanceInput!) {
  gcpNativeRestoreGceInstance(input: `$input) {
    jobId
    __typename
  }
}
"@

    $variables = @{
        "input" = @{
            "snapshotId"                  = $SnapshotId
            "shouldRestoreLabels"         = $ShouldRestoreLabels
            "shouldAddRubrikLabels"       = $ShouldAddRubrikLabels
            "shouldStartRestoredInstance" = $ShouldStartRestoredInstance
            "snapshotType"                = "SOURCE"
        }
    }

    $payload = @{ query = $mutation; variables = $variables }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
    }
    catch { Throw "Restore mutation failed: $_" }

    if ($response.errors) { Throw "GraphQL Error during restore: $($response.errors.message)" }

    return $response.data.gcpNativeRestoreGceInstance.jobId
}

function Invoke-GcpExport {
    # Out-of-place export: creates a new GCE instance from the snapshot in a target project/zone.
    [CmdletBinding()]
    param (
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string]$SnapshotId,
        [string]$TargetZone,
        [string]$TargetInstanceName,
        [string]$SharedVpcHostProjectNativeId,
        [string]$TargetMachineType,
        [string]$TargetSubnetName,
        [string]$TargetGcpProjectRubrikId,
        [string]$ServiceAccountId,
        [string[]]$TargetNetworkTags    = @(),
        [string]$DiskEncryptionType     = "GOOGLE_MANAGED_KEY",
        [bool]$ShouldCopyLabels         = $true,
        [bool]$ShouldAddRubrikLabels    = $true,
        [bool]$ShouldPowerOff           = $false
    )

    $mutation = @"
mutation ExportGCPInstanceMutation(`$input: GcpNativeExportGceInstanceInput!) {
  gcpNativeExportGceInstance(input: `$input) {
    jobId
    __typename
  }
}
"@

    $variables = @{
        "input" = @{
            "snapshotId"                   = $SnapshotId
            "shouldCopyLabels"             = $ShouldCopyLabels
            "shouldAddRubrikLabels"        = $ShouldAddRubrikLabels
            "shouldPowerOff"               = $ShouldPowerOff
            "targetZone"                   = $TargetZone
            "targetInstanceName"           = $TargetInstanceName
            "sharedVpcHostProjectNativeId" = $SharedVpcHostProjectNativeId
            "targetMachineType"            = $TargetMachineType
            "targetSubnetName"             = $TargetSubnetName
            "targetNetworkTags"            = $TargetNetworkTags
            "diskEncryptionType"           = $DiskEncryptionType
            "kmsCryptoKey"                 = $null
            "kmsCryptoKeyResourceId"       = $null
            "snapshotType"                 = "SOURCE"
            "targetGcpProjectRubrikId"     = $TargetGcpProjectRubrikId
            "serviceAccountId"             = $ServiceAccountId
        }
    }

    $payload = @{ query = $mutation; variables = $variables }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers `
            -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
    }
    catch { Throw "Export mutation failed: $_" }

    if ($response.errors) { Throw "GraphQL Error during export: $($response.errors.message)" }

    return $response.data.gcpNativeExportGceInstance.jobId
}

# =============================================================================
# HELPER: GCP INSTANCE NAME SANITIZATION
# =============================================================================

function Format-GcpInstanceName {
    # GCE instance names: 1–63 chars, lowercase letters/numbers/hyphens,
    # must start with a letter, must not end with a hyphen.
    param ([string]$RawName)

    $name = $RawName.ToLower()
    $name = $name -replace '[^a-z0-9\-]', '-'   # Replace invalid characters with hyphen
    $name = $name -replace '-+', '-'              # Collapse consecutive hyphens
    $name = $name.TrimEnd('-')                    # Remove trailing hyphen

    if ($name.Length -gt 63) {
        $name = $name.Substring(0, 63).TrimEnd('-')
    }

    if ($name -notmatch '^[a-z]') {
        $name = "vm-$name"
        if ($name.Length -gt 63) { $name = $name.Substring(0, 63).TrimEnd('-') }
    }

    return $name
}

# =============================================================================
# MAIN WORKFLOW
# =============================================================================

# --- LOGGING SETUP ---
if ($EnableLogging) {
    $logDir = Split-Path -Parent $LogPath
    if (-not (Test-Path $logDir)) {
        try { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
        catch { Write-Warning "Could not create log directory '$logDir': $_" }
    }
    if (Test-Path $LogPath) {
        $logItem = Get-Item $LogPath
        if ($logItem.Length -gt 2MB) {
            $archiveName = "{0}_{1}{2}" -f $logItem.BaseName, (Get-Date -Format "yyyyMMdd-HHmmss"), $logItem.Extension
            try { Rename-Item -Path $LogPath -NewName $archiveName -Force } catch {}
        }
    }
}

$ErrorActionPreference = "Stop"
$rscHeaders = $null
$graphqlUrl = $null
$logoutUrl  = $null
$exitCode   = 0

try {
    Write-Log ""
    Write-Log "========================================================" -ForegroundColor Cyan
    Write-Log "   Rubrik | Meditech GCP Recovery" -ForegroundColor Cyan
    Write-Log "========================================================" -ForegroundColor Cyan
    Write-Log ""

    # ------------------------------------------------------------------
    # STEP 1: LOAD CREDENTIALS AND CONNECT TO RSC
    # ------------------------------------------------------------------
    Write-Log ">>> Step 1: Connecting to Rubrik Security Cloud..." -ForegroundColor Cyan

    $serviceAccountObj = $null

    if ($MbfConfigXml -and (Test-Path $MbfConfigXml)) {
        Write-Log "    Loading RSC credentials from: $MbfConfigXml" -ForegroundColor DarkGray
        try {
            $mbfStore = Import-Clixml -Path $MbfConfigXml
        }
        catch {
            Throw "Failed to decrypt config XML. Ensure you are running as the user who created the file on this machine. Error: $_"
        }

        if (-not $mbfStore.RscClientId) {
            Throw "RSC credentials not found in '$MbfConfigXml'. Re-run New-MeditechCredentials.ps1 to embed RSC service account details."
        }

        $rscSecret = [System.Net.NetworkCredential]::new('', $mbfStore.RscClientSecret).Password
        $serviceAccountObj = [PSCustomObject]@{
            client_id        = $mbfStore.RscClientId
            client_secret    = $rscSecret
            access_token_uri = $mbfStore.RscTokenUri
            name             = $mbfStore.RscName
        }

        # Pull GcpProjectId from XML only if not explicitly supplied as a parameter.
        if (-not $GcpProjectId -and $mbfStore.GcpProjectId) {
            $GcpProjectId = $mbfStore.GcpProjectId
        }
    }
    elseif ($ServiceAccountJson) {
        if (-not (Test-Path $ServiceAccountJson)) {
            Throw "Service Account JSON not found at '$ServiceAccountJson'."
        }
        $serviceAccountObj = Get-Content -Raw $ServiceAccountJson | ConvertFrom-Json
    }
    else {
        Throw "RSC credentials not provided. Supply -MbfConfigXml (recommended) or -ServiceAccountJson."
    }

    $tokenInfo  = Connect-Rsc -ServiceAccount $serviceAccountObj
    $rscHost    = ([System.Uri]$serviceAccountObj.access_token_uri).Host
    $graphqlUrl = "https://$rscHost/api/graphql"
    $logoutUrl  = "https://$rscHost/api/session"
    $rscHeaders = @{
        "Authorization" = "Bearer $($tokenInfo.access_token)"
        "Content-Type"  = "application/json"
    }

    Write-Log "    Connected to RSC: $rscHost" -ForegroundColor Green

    # ------------------------------------------------------------------
    # STEP 2: RETRIEVE GCE INVENTORY
    # ------------------------------------------------------------------
    Write-Log ">>> Step 2: Retrieving GCE instance inventory from RSC..." -ForegroundColor Cyan

    if ($GcpProjectId) {
        Write-Log "    Filtering by GCP Project ID: $GcpProjectId" -ForegroundColor DarkGray
    }

    $inventory = Get-RscGcpInventory -ApiEndpoint $graphqlUrl -Headers $rscHeaders -GcpProjectId $GcpProjectId

    if ($inventory.Count -eq 0) {
        $projectMsg = if ($GcpProjectId) { " for project '$GcpProjectId'" } else { "" }
        Throw "No GCE instances found in RSC inventory$projectMsg. Verify the GCP project ID and service account permissions."
    }

    Write-Log "    Found $($inventory.Count) GCE instance(s) in inventory." -ForegroundColor Green

    # ------------------------------------------------------------------
    # STEP 3: SELECT TARGET VMs
    # ------------------------------------------------------------------
    Write-Log ">>> Step 3: Selecting target VMs..." -ForegroundColor Cyan

    $targetVms = @()

    if ($VmNames -and $VmNames.Count -gt 0) {
        foreach ($name in $VmNames) {
            $match = $inventory | Where-Object { $_.nativeName -ieq $name }
            if ($match) {
                $targetVms += $match
            }
            else {
                Write-Log "    VM '$name' was not found in RSC inventory — skipping." -Level Warning
            }
        }
        if ($targetVms.Count -eq 0) {
            Throw "None of the specified VM names matched any instance in the RSC inventory."
        }
        Write-Log "    Resolved $($targetVms.Count) of $($VmNames.Count) specified VM(s)." -ForegroundColor Green
    }
    else {
        # Interactive selection from numbered list
        Write-Log ""
        Write-Log "Available GCE instances:" -ForegroundColor Cyan
        Write-Log ""
        for ($i = 0; $i -lt $inventory.Count; $i++) {
            Write-Host ("  [{0,3}]  {1}" -f ($i + 1), $inventory[$i].nativeName)
        }
        Write-Log ""
        Write-Log "Enter instance numbers to recover (e.g. 1,3,5), or 'All' for all instances:" -ForegroundColor Yellow
        $selectionInput = (Read-Host "Selection").Trim()

        if ($selectionInput -match '^[Aa]ll$') {
            $targetVms = $inventory
        }
        else {
            $selectedIndices = $selectionInput -split '[,\s]+' |
                ForEach-Object { $_.Trim() } |
                Where-Object { $_ -match '^\d+$' }

            foreach ($idx in $selectedIndices) {
                $i = [int]$idx - 1
                if ($i -ge 0 -and $i -lt $inventory.Count) {
                    $targetVms += $inventory[$i]
                }
                else {
                    Write-Log "    '$idx' is out of range — ignored." -Level Warning
                }
            }
        }

        if ($targetVms.Count -eq 0) {
            Throw "No valid VMs selected. Aborting."
        }

        Write-Log "    Selected $($targetVms.Count) VM(s): $($targetVms.nativeName -join ', ')" -ForegroundColor Green
    }

    # ------------------------------------------------------------------
    # LIST SNAPSHOTS MODE  (exits here)
    # ------------------------------------------------------------------
    if ($PSCmdlet.ParameterSetName -eq "ListSnapshots") {
        Write-Log ">>> List Snapshots Mode" -ForegroundColor Cyan
        Write-Log ""

        foreach ($vm in $targetVms) {
            Write-Log "Snapshots for: $($vm.nativeName)" -ForegroundColor Yellow
            Write-Log "  RSC ID : $($vm.id)" -ForegroundColor DarkGray

            $snaps = Get-RscVmSnapshots -ApiEndpoint $graphqlUrl -Headers $rscHeaders `
                -SnappableId $vm.id -MaxResults $SnapshotListCount

            if ($snaps.Count -eq 0) {
                Write-Log "  No snapshots found for this VM." -Level Warning
            }
            else {
                $snapTable = $snaps | Select-Object `
                    @{ N = 'Date (UTC)';    E = { ([datetime]$_.date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss") } },
                    @{ N = 'Snapshot ID';   E = { $_.id } },
                    @{ N = 'On-Demand';     E = { $_.isOnDemandSnapshot } },
                    @{ N = 'SLA';           E = { if ($_.slaDomain) { $_.slaDomain.name } else { 'N/A' } } },
                    @{ N = 'Expires (UTC)'; E = { if ($_.expirationDate) { ([datetime]$_.expirationDate).ToUniversalTime().ToString("yyyy-MM-dd") } else { 'Never / N/A' } } } |
                    Format-Table -AutoSize | Out-String
                Write-Log $snapTable
            }
            Write-Log ""
        }

        return
    }

    # ------------------------------------------------------------------
    # STEP 4: DETERMINE TARGET DATETIME
    # ------------------------------------------------------------------
    Write-Log ">>> Step 4: Determining recovery point target..." -ForegroundColor Cyan

    $recoveryTarget = $null

    if ($TargetDateTime) {
        try {
            $recoveryTarget = [datetime]::Parse($TargetDateTime)
        }
        catch {
            Throw "Could not parse -TargetDateTime '$TargetDateTime'. Use a format like '2026-03-25 13:00' or '2026-03-25T13:00:00'."
        }
    }
    else {
        Write-Log ""
        Write-Log "Enter the recovery point target date and time." -ForegroundColor Yellow
        Write-Log "The script selects the closest snapshot at or BEFORE this moment for each VM." -ForegroundColor Gray
        Write-Log ""
        Write-Log "  Format examples:" -ForegroundColor Gray
        Write-Log "    2026-03-25 13:00          (24-hour, local machine time)" -ForegroundColor Gray
        Write-Log "    2026-03-25T13:00:00Z      (UTC, ISO 8601)" -ForegroundColor Gray
        Write-Log "    03/25/2026 2:00 AM        (12-hour, local machine time)" -ForegroundColor Gray
        Write-Log ""

        $dtInput = (Read-Host "Target date/time").Trim().Trim('"').Trim("'")
        try {
            $recoveryTarget = [datetime]::Parse($dtInput)
        }
        catch {
            Throw "Could not parse '$dtInput' as a valid date/time. Aborting."
        }
    }

    Write-Log "    Recovery target : $($recoveryTarget.ToString('yyyy-MM-dd HH:mm:ss')) (local)" -ForegroundColor Green
    Write-Log "    Recovery target : $($recoveryTarget.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor DarkGray

    # ------------------------------------------------------------------
    # STEP 5: RESOLVE BEST SNAPSHOT PER VM
    # ------------------------------------------------------------------
    Write-Log ">>> Step 5: Resolving closest snapshot for each selected VM..." -ForegroundColor Cyan

    $recoveryPlan = @()

    foreach ($vm in $targetVms) {
        Write-Log "    Querying snapshots for: $($vm.nativeName)..." -ForegroundColor DarkGray

        $bestSnap = Get-RscBestSnapshot -ApiEndpoint $graphqlUrl -Headers $rscHeaders `
            -SnappableId $vm.id -TargetDateTime $recoveryTarget

        if (-not $bestSnap) {
            Write-Log "    No snapshot found at or before the target time for '$($vm.nativeName)'." -Level Warning

            # Retrieve the oldest available snapshot to offer as a fallback.
            $oldestSnap = Get-RscOldestSnapshot -ApiEndpoint $graphqlUrl -Headers $rscHeaders `
                -SnappableId $vm.id

            if (-not $oldestSnap) {
                Write-Log "    '$($vm.nativeName)' has no snapshots at all in RSC. Skipping." -Level Warning
                continue
            }

            $oldestDate = ([datetime]$oldestSnap.date).ToString("yyyy-MM-dd HH:mm:ss")
            Write-Log "    Oldest available snapshot is from: $oldestDate" -Level Warning
            Write-Log ""
            Write-Host ("    Options for {0}:" -f $vm.nativeName) -ForegroundColor Yellow
            Write-Host "      [1]  Use oldest available snapshot ($oldestDate)"
            Write-Host "      [2]  Skip this VM"
            Write-Host ""
            $fallbackChoice = (Read-Host "    Choice (1 or 2)").Trim()

            if ($fallbackChoice -eq "1") {
                $bestSnap = $oldestSnap
                Write-Log "    Using oldest snapshot for '$($vm.nativeName)': $oldestDate" -Level Warning
            }
            else {
                Write-Log "    Skipping '$($vm.nativeName)'." -Level Warning
                continue
            }
        }

        $snapDateLocal = [datetime]$bestSnap.date
        $ageHours      = [math]::Round(([datetime]::Now - $snapDateLocal).TotalHours, 1)
        $slaName       = if ($bestSnap.slaDomain) { $bestSnap.slaDomain.name } else { "N/A" }

        $recoveryPlan += [PSCustomObject]@{
            VmName       = $vm.nativeName
            VmId         = $vm.id
            SnapshotId   = $bestSnap.id
            SnapshotDate = $snapDateLocal
            AgeHours     = $ageHours
            SlaName      = $slaName
            IsOnDemand   = $bestSnap.isOnDemandSnapshot
        }
    }

    if ($recoveryPlan.Count -eq 0) {
        Throw "No VMs could be matched to a valid snapshot. Aborting recovery."
    }

    # Display the recovery plan
    Write-Log ""
    Write-Log "==== Recovery Plan ====" -ForegroundColor Cyan
    Write-Log ""

    $planTable = $recoveryPlan | Select-Object `
        @{ N = 'VM Name';       E = { $_.VmName } },
        @{ N = 'Snapshot Date'; E = { $_.SnapshotDate.ToString("yyyy-MM-dd HH:mm:ss") } },
        @{ N = 'Age (hrs)';     E = { $_.AgeHours } },
        @{ N = 'On-Demand';     E = { $_.IsOnDemand } },
        @{ N = 'SLA';           E = { $_.SlaName } },
        @{ N = 'Snapshot ID';   E = { $_.SnapshotId } } |
        Format-Table -AutoSize | Out-String

    Write-Log $planTable

    # ------------------------------------------------------------------
    # STEP 6: SELECT RECOVERY TYPE
    # ------------------------------------------------------------------
    Write-Log ">>> Step 6: Recovery type selection..." -ForegroundColor Cyan

    if (-not $RecoveryType) {
        Write-Log ""
        Write-Log "Select recovery type:" -ForegroundColor Yellow
        Write-Log ""
        Write-Log "  [1]  Restore  (in-place)" -ForegroundColor White
        Write-Log "       Overwrites the existing GCE instance with data from the snapshot." -ForegroundColor Gray
        Write-Log "       The original VM is replaced. No new instance is created." -ForegroundColor Gray
        Write-Log "       WARNING: This operation is destructive and cannot be undone." -ForegroundColor Red
        Write-Log ""
        Write-Log "  [2]  Export  (out-of-place / clone)" -ForegroundColor White
        Write-Log "       Creates a new GCE instance from the snapshot in a target project." -ForegroundColor Gray
        Write-Log "       The original VM is not affected. Use for DR testing or side-by-side" -ForegroundColor Gray
        Write-Log "       validation." -ForegroundColor Gray
        Write-Log ""

        $typeChoice = (Read-Host "Recovery type (1 or 2)").Trim()
        switch ($typeChoice) {
            "1" { $RecoveryType = "Restore" }
            "2" { $RecoveryType = "Export" }
            default { Throw "Invalid selection '$typeChoice'. Aborting." }
        }
    }

    Write-Log "    Recovery type: $RecoveryType" -ForegroundColor Green

    # ------------------------------------------------------------------
    # STEP 7: COLLECT MODE-SPECIFIC PARAMETERS
    # ------------------------------------------------------------------

    # Power state applies to both modes but with opposite semantics.
    $shouldPowerOn  = $true  # Restore: shouldStartRestoredInstance
    $shouldPowerOff = $false # Export:  shouldPowerOff (inverse of shouldPowerOn)

    $exportParams = $null

    if ($RecoveryType -eq "Restore") {
        Write-Log ">>> Step 7: Restore options..." -ForegroundColor Cyan
        Write-Log ""
        $powerInput = (Read-Host "Start instances automatically after restore completes? (Y/N, default: Y)").Trim()
        $shouldPowerOn = -not ($powerInput -match '^[Nn]')
        Write-Log "    Auto-start after restore: $shouldPowerOn" -ForegroundColor DarkGray
    }
    else {
        # --- EXPORT: collect all required target parameters ---
        Write-Log ">>> Step 7: Export parameters..." -ForegroundColor Cyan

        # Target Project
        if (-not $TargetProjectRubrikId -or -not $TargetProjectNativeId) {
            Write-Log ""
            Write-Log "    Retrieving available GCP projects from RSC..." -ForegroundColor DarkGray
            $projects = Get-RscGcpProjects -ApiEndpoint $graphqlUrl -Headers $rscHeaders

            if ($projects.Count -gt 0) {
                Write-Log ""
                Write-Log "    Available GCP Projects:" -ForegroundColor Cyan
                Write-Log ""
                for ($i = 0; $i -lt $projects.Count; $i++) {
                    Write-Host ("      [{0,3}]  {1,-45}  {2}" -f ($i + 1), $projects[$i].name, $projects[$i].nativeId)
                }
                Write-Log ""
                $projInput = (Read-Host "    Target project number (or press Enter to enter IDs manually)").Trim()

                if ($projInput -match '^\d+$') {
                    $projIdx = [int]$projInput - 1
                    if ($projIdx -ge 0 -and $projIdx -lt $projects.Count) {
                        $TargetProjectRubrikId = $projects[$projIdx].id
                        $TargetProjectNativeId = $projects[$projIdx].nativeId
                        Write-Log "    Target project: $($projects[$projIdx].name)" -ForegroundColor Green
                    }
                    else {
                        Throw "Invalid project selection '$projInput'. Aborting."
                    }
                }
                else {
                    if (-not $TargetProjectRubrikId) {
                        $TargetProjectRubrikId = (Read-Host "    Target Project RSC UUID").Trim().Trim('"').Trim("'")
                    }
                    if (-not $TargetProjectNativeId) {
                        $TargetProjectNativeId = (Read-Host "    Target Project GCP Native ID (e.g. my-gcp-project)").Trim().Trim('"').Trim("'")
                    }
                }
            }
            else {
                Write-Log "    No projects returned — entering manually." -Level Warning
                if (-not $TargetProjectRubrikId) {
                    $TargetProjectRubrikId = (Read-Host "    Target Project RSC UUID").Trim().Trim('"').Trim("'")
                }
                if (-not $TargetProjectNativeId) {
                    $TargetProjectNativeId = (Read-Host "    Target Project GCP Native ID").Trim().Trim('"').Trim("'")
                }
            }
        }

        if ([string]::IsNullOrWhiteSpace($TargetProjectRubrikId)) { Throw "Target Project RSC UUID is required for Export. Aborting." }
        if ([string]::IsNullOrWhiteSpace($TargetProjectNativeId)) { Throw "Target Project GCP Native ID is required for Export. Aborting." }

        # Target Zone
        if (-not $TargetZone) {
            Write-Log ""
            $TargetZone = (Read-Host "    Target GCP Zone (e.g. us-central1-a)").Trim().Trim('"').Trim("'")
        }
        if ([string]::IsNullOrWhiteSpace($TargetZone)) { Throw "Target zone is required for Export. Aborting." }

        # Target Machine Type
        if (-not $TargetMachineType) {
            Write-Log ""
            $TargetMachineType = (Read-Host "    Target Machine Type (e.g. e2-standard-4)").Trim().Trim('"').Trim("'")
        }
        if ([string]::IsNullOrWhiteSpace($TargetMachineType)) { Throw "Target machine type is required for Export. Aborting." }

        # Target Subnet
        if (-not $TargetSubnetName) {
            Write-Log ""
            $TargetSubnetName = (Read-Host "    Target Subnet Name (e.g. meditech-uscentral1-a)").Trim().Trim('"').Trim("'")
        }
        if ([string]::IsNullOrWhiteSpace($TargetSubnetName)) { Throw "Target subnet name is required for Export. Aborting." }

        # Service Account Email
        if (-not $ServiceAccountEmail) {
            Write-Log ""
            $ServiceAccountEmail = (Read-Host "    GCP Service Account Email (e.g. compute@project.iam.gserviceaccount.com)").Trim().Trim('"').Trim("'")
        }
        if ([string]::IsNullOrWhiteSpace($ServiceAccountEmail)) { Throw "Service account email is required for Export. Aborting." }

        # Instance Name Suffix
        if (-not $InstanceNameSuffix) {
            $defaultSuffix = "-rcv-$(Get-Date -Format 'yyyyMMdd')"
            Write-Log ""
            $suffixInput = (Read-Host "    Instance name suffix (press Enter for default: '$defaultSuffix')").Trim().Trim('"').Trim("'")
            $InstanceNameSuffix = if ([string]::IsNullOrWhiteSpace($suffixInput)) { $defaultSuffix } else { $suffixInput }
        }

        # Power state after export
        Write-Log ""
        $powerInput     = (Read-Host "    Power on recovered instances after export completes? (Y/N, default: Y)").Trim()
        $shouldPowerOn  = -not ($powerInput -match '^[Nn]')
        $shouldPowerOff = -not $shouldPowerOn

        # Bundle export params for use in the execution loop
        $exportParams = [PSCustomObject]@{
            TargetProjectRubrikId = $TargetProjectRubrikId
            TargetProjectNativeId = $TargetProjectNativeId
            TargetZone            = $TargetZone
            TargetMachineType     = $TargetMachineType
            TargetSubnetName      = $TargetSubnetName
            ServiceAccountEmail   = $ServiceAccountEmail
            InstanceNameSuffix    = $InstanceNameSuffix
            ShouldPowerOff        = $shouldPowerOff
        }

        Write-Log ""
        Write-Log "==== Export Parameters ====" -ForegroundColor Cyan
        Write-Log "  Target Project (RSC) : $TargetProjectRubrikId" -ForegroundColor Gray
        Write-Log "  Target Project (GCP) : $TargetProjectNativeId" -ForegroundColor Gray
        Write-Log "  Target Zone          : $TargetZone" -ForegroundColor Gray
        Write-Log "  Machine Type         : $TargetMachineType" -ForegroundColor Gray
        Write-Log "  Subnet               : $TargetSubnetName" -ForegroundColor Gray
        Write-Log "  Service Account      : $ServiceAccountEmail" -ForegroundColor Gray
        Write-Log "  Instance Suffix      : $InstanceNameSuffix" -ForegroundColor Gray
        Write-Log "  Power On After Export: $shouldPowerOn" -ForegroundColor Gray
        Write-Log ""

        # Preview target instance names
        Write-Log "  Recovered instance names will be:" -ForegroundColor DarkGray
        foreach ($item in $recoveryPlan) {
            $preview = Format-GcpInstanceName -RawName ($item.VmName + $InstanceNameSuffix)
            Write-Log ("    {0,-30} → {1}" -f $item.VmName, $preview) -ForegroundColor DarkGray
        }
    }

    # ------------------------------------------------------------------
    # STEP 8: FINAL CONFIRMATION
    # ------------------------------------------------------------------
    Write-Log ">>> Step 8: Final confirmation..." -ForegroundColor Cyan
    Write-Log ""
    Write-Log "==== Recovery Summary ====" -ForegroundColor Yellow
    Write-Log "  Recovery Type   : $RecoveryType" -ForegroundColor White
    Write-Log "  Target DateTime : $($recoveryTarget.ToString('yyyy-MM-dd HH:mm:ss')) local  /  $($recoveryTarget.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')) UTC" -ForegroundColor White
    Write-Log "  VMs Targeted    : $($recoveryPlan.Count)" -ForegroundColor White
    Write-Log "  Power On        : $shouldPowerOn" -ForegroundColor White
    Write-Log ""

    foreach ($item in $recoveryPlan) {
        if ($RecoveryType -eq "Export") {
            $targetName = Format-GcpInstanceName -RawName ($item.VmName + $exportParams.InstanceNameSuffix)
            Write-Log ("  {0,-25}  →  {1}" -f $item.VmName, $targetName) -ForegroundColor White
        }
        else {
            Write-Log ("  {0,-25}  (in-place restore)" -f $item.VmName) -ForegroundColor White
        }
        Write-Log ("    Snapshot: {0}  ({1} hrs ago)   SLA: {2}" -f `
            $item.SnapshotDate.ToString("yyyy-MM-dd HH:mm:ss"), $item.AgeHours, $item.SlaName) -ForegroundColor DarkGray
    }

    Write-Log ""

    if ($RecoveryType -eq "Restore") {
        Write-Host ""
        Write-Host "  *** WARNING: In-place Restore will OVERWRITE the existing GCE instances. ***" -ForegroundColor Red
        Write-Host "  *** The current VM disk state will be permanently replaced.              ***" -ForegroundColor Red
        Write-Host "  *** This operation cannot be undone.                                    ***" -ForegroundColor Red
        Write-Host ""
    }

    $confirm = (Read-Host "Type CONFIRM to proceed, or press Enter to abort").Trim()
    if ($confirm -ne "CONFIRM") {
        Write-Log "Recovery aborted by user at confirmation prompt." -Level Warning
        exit 0
    }

    # ------------------------------------------------------------------
    # STEP 9: EXECUTE RECOVERY OPERATIONS
    # ------------------------------------------------------------------
    Write-Log ">>> Step 9: Executing recovery operations..." -ForegroundColor Cyan
    Write-Log ""

    $results = @()

    foreach ($item in $recoveryPlan) {
        Write-Log "    Processing: $($item.VmName)..." -ForegroundColor Yellow

        try {
            if ($RecoveryType -eq "Restore") {
                $jobId = Invoke-GcpRestore `
                    -ApiEndpoint $graphqlUrl `
                    -Headers     $rscHeaders `
                    -SnapshotId  $item.SnapshotId `
                    -ShouldStartRestoredInstance $shouldPowerOn

                Write-Log "    $($item.VmName) : Restore job initiated — Job ID: $jobId" -ForegroundColor Green

                $results += [PSCustomObject]@{
                    VmName       = $item.VmName
                    TargetVmName = $item.VmName
                    Operation    = "Restore"
                    SnapshotDate = $item.SnapshotDate.ToString("yyyy-MM-dd HH:mm:ss")
                    Status       = "Initiated"
                    JobId        = $jobId
                }
            }
            else {
                $targetName = Format-GcpInstanceName -RawName ($item.VmName + $exportParams.InstanceNameSuffix)

                $jobId = Invoke-GcpExport `
                    -ApiEndpoint                  $graphqlUrl `
                    -Headers                      $rscHeaders `
                    -SnapshotId                   $item.SnapshotId `
                    -TargetZone                   $exportParams.TargetZone `
                    -TargetInstanceName           $targetName `
                    -SharedVpcHostProjectNativeId $exportParams.TargetProjectNativeId `
                    -TargetMachineType            $exportParams.TargetMachineType `
                    -TargetSubnetName             $exportParams.TargetSubnetName `
                    -TargetGcpProjectRubrikId     $exportParams.TargetProjectRubrikId `
                    -ServiceAccountId             $exportParams.ServiceAccountEmail `
                    -ShouldPowerOff               $exportParams.ShouldPowerOff

                Write-Log "    $($item.VmName) → $targetName : Export job initiated — Job ID: $jobId" -ForegroundColor Green

                $results += [PSCustomObject]@{
                    VmName       = $item.VmName
                    TargetVmName = $targetName
                    Operation    = "Export"
                    SnapshotDate = $item.SnapshotDate.ToString("yyyy-MM-dd HH:mm:ss")
                    Status       = "Initiated"
                    JobId        = $jobId
                }
            }
        }
        catch {
            Write-Log "    FAILED for $($item.VmName): $_" -Level Error
            $exitCode = 1

            $results += [PSCustomObject]@{
                VmName       = $item.VmName
                TargetVmName = if ($RecoveryType -eq "Export") { Format-GcpInstanceName -RawName ($item.VmName + $exportParams.InstanceNameSuffix) } else { $item.VmName }
                Operation    = $RecoveryType
                SnapshotDate = $item.SnapshotDate.ToString("yyyy-MM-dd HH:mm:ss")
                Status       = "FAILED"
                JobId        = "N/A"
            }
        }
    }

    # ------------------------------------------------------------------
    # STEP 10: SUMMARY REPORT
    # ------------------------------------------------------------------
    Write-Log ""
    Write-Log "========================================================" -ForegroundColor Cyan
    Write-Log "   Recovery Job Summary" -ForegroundColor Cyan
    Write-Log "========================================================" -ForegroundColor Cyan
    Write-Log ""

    $summaryTable = $results | Format-Table -AutoSize | Out-String
    Write-Log $summaryTable

    Write-Log "Monitor job progress in the Rubrik Security Cloud portal:" -ForegroundColor Gray
    Write-Log "  Activity Log > Jobs" -ForegroundColor Gray
    Write-Log ""

    if ($exitCode -eq 0) {
        Write-Log "=== Recovery workflow completed successfully. ===" -ForegroundColor Green
    }
    else {
        Write-Log "=== Recovery workflow completed with one or more errors. Review log for details. ===" -Level Error
    }
}
catch {
    Write-Log "Workflow Error: $_" -Level Error
    $exitCode = 1
}
finally {
    if ($rscHeaders -and $logoutUrl) {
        Write-Log "Disconnecting from RSC..." -ForegroundColor DarkGray
        Disconnect-Rsc -LogoutUrl $logoutUrl -Headers $rscHeaders
    }
    exit $exitCode
}
