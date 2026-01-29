 <#
.SYNOPSIS
    Meditech Backup & Rubrik Snapshot Orchestration Script
.DESCRIPTION
    Orchestrates the backup workflow for Meditech on GCP using Rubrik Security Cloud (RSC).
    1. Authenticates to RSC.
    2. Runs MBF Census to dynamically discover active Meditech Servers.
    3. Fetches GCP Inventory and correlates with Census results.
    4. Quiesces Meditech via MBF.exe.
    5. Triggers Rubrik Bulk Snapshot (Mutation) for the discovered VMs.
    6. Unquiesces Meditech immediately.
.EXAMPLE
    # Backup Mode
    .\MeditechMasterWorkflow.ps1 `
        -ServiceAccountJson "C:\creds\rsc_service_account.json" `
        -RetentionSlaId "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" `
        -MbfUser "mbfUser" `
        -MbfPassword "mbfSecret" `
        -MbfIntermediary "MasterServer:XXXX"
    
    # Output:
    # >>> Step 1: Connecting to Rubrik Security Cloud...
    # >>> Step 2: Running Meditech Census to identify targets...
    #     [RAW MBF CENSUS OUTPUT]
    #       MEDITECH Backup Facilitator Version 1.7.3.0
    #       Copyright (C) 2011-2019 Medical Information Technology, Inc.
    #       Server1:Server1 E=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|,
    #       Server2:Server2 E=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX|,
    #       ...
    #     [END RAW OUTPUT]
    #     Census identified 2 unique host(s): RUB-MATFS, RUB-NPRFS
    #     Fetching RSC Inventory...
    #     Resolved X VM(s) to snapshot.
    # >>> Step 3: Quiescing Meditech...
    #     [RAW MBF QUIESCE OUTPUT]
    #       MEDITECH Backup Facilitator Version 1.7.3.0
    #       Copyright (C) 2011-2019 Medical Information Technology, Inc.
    #       Server1=SUCCESS,
    #       Server2=SUCCESS,
    #       ...
    #     [END RAW OUTPUT]
    #     Quiesce Successful (Code 0).
    # >>> Step 4: Initiating Rubrik Snapshots...
    #     Snapshot Request Complete.
    #     Wall Clock Time: 0 ms
    #     Server Exec Time: 0 ms
    # >>> Step 5: Unquiescing Meditech...
    #     [RAW MBF UNQUIESCE OUTPUT]
    #       MEDITECH Backup Facilitator Version 1.7.3.0
    #       Copyright (C) 2011-2019 Medical Information Technology, Inc.
    #       Server1=SUCCESS,
    #       Server2=SUCCESS,
    #       ...
    #     [END RAW OUTPUT]
    #     Unquiesce Successful.
    # >>> Step 6: Disconnecting...

.EXAMPLE
    # Backup Mode (Multiple Intermediaries)
    .\MeditechMasterWorkflow.ps1 `
        -MbfIntermediary "RUB-MBI:2987,RUB-MATFS:2987" `
        ...

.EXAMPLE
    # Backup Mode with Force Enabled (Auto-retries on Code 8/9)
    .\MeditechMasterWorkflow.ps1 `
        -ServiceAccountJson "C:\creds.json" `
        -RetentionSlaId "..." `
        -Force

.EXAMPLE
    # Census Only Mode (No Backup, No RSC Connection)
    .\MeditechMasterWorkflow.ps1 `
        -MbfUser "ISB" `
        -MbfPassword "Secret" `
        -MbfIntermediary "RUB-MBI:2987" `
        -CensusOnly

.EXAMPLE
    # List SLAs Mode
    .\MeditechMasterWorkflow.ps1 -ServiceAccountJson "C:\creds\rsc_service_account.json" -ListSlas
    # Output:
    # >>> Step 1: Connecting to Rubrik Security Cloud...
    # >>> List Mode: Retrieving SLA Domains via GraphQL...
    #
    # name                id
    # ----                --
    # Bronze              00000000-0000-0000-0000-000000000002
    # custom-sla-1        00000000-0000-0000-0000-000000000003
    # Gold                00000000-0000-0000-0000-000000000000
    # Silver              00000000-0000-0000-0000-000000000001

.EXAMPLE
    # List Projects Mode
    .\MeditechMasterWorkflow.ps1 -ServiceAccountJson "C:\creds\rsc_service_account.json" -ListProjects
    # Output:
    # >>> Step 1: Connecting to Rubrik Security Cloud...
    # >>> List Mode: Retrieving GCP Projects via GraphQL...
    #
    # Project Name                  GCP Native ID                 RSC Project ID
    # ------------                  -------------                 --------------
    # gcp-rbrkdev-cnp               gcp-rbrkdev-cnp               00000000-0000-0000-0000-000000000000
    # gcp-rubrikcom-cnp             gcp-rubrikcom-cnp             00000000-0000-0000-0000-000000000001
.NOTES
    Author  : Marcus Henderson 
    Created : January 2026
    Company : Rubrik Inc
#>
[cmdletbinding(DefaultParameterSetName = "RunBackup")]
param (
    # Common Parameters (Required for RSC operations)
    [Parameter(ParameterSetName = "RunBackup", Mandatory=$true)]
    [Parameter(ParameterSetName = "ListSlas", Mandatory=$true)]
    [Parameter(ParameterSetName = "ListProjects", Mandatory=$true)]
    [Parameter(ParameterSetName = "RunCensus", Mandatory=$false)]
    [string]$ServiceAccountJson,

    # List Mode Switches
    [Parameter(ParameterSetName = "ListSlas", Mandatory=$true)]
    [switch]$ListSlas,

    [Parameter(ParameterSetName = "ListProjects", Mandatory=$true)]
    [switch]$ListProjects,

    # Census Only Mode Switch
    [Parameter(ParameterSetName = "RunCensus", Mandatory=$true)]
    [switch]$CensusOnly,

    # Backup Mode Parameters
    [parameter(ParameterSetName = "RunBackup", Mandatory=$true)]
    [string]$RetentionSlaId,

    [parameter(ParameterSetName = "RunBackup", Mandatory=$false)]
    [string]$GcpProjectId, # Optional filter for inventory

    [Parameter(ParameterSetName = "RunBackup")]
    [switch]$Force,

    # MBF Parameters (Required for Backup and Census modes)
    [Parameter(ParameterSetName = "RunBackup", Mandatory=$true)]
    [Parameter(ParameterSetName = "RunCensus", Mandatory=$true)]
    [string]$MbfUser,

    [Parameter(ParameterSetName = "RunBackup", Mandatory=$true)]
    [Parameter(ParameterSetName = "RunCensus", Mandatory=$true)]
    [string]$MbfPassword,

    [Parameter(ParameterSetName = "RunBackup", Mandatory=$true)]
    [Parameter(ParameterSetName = "RunCensus", Mandatory=$true)]
    [string[]]$MbfIntermediary,

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [string]$MbfPath = "C:\Program Files (x86)\MEDITECH\MBI\mbf.exe",

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [int]$MbfTimeout = 90
)

# -----------------------------------------------------------------------------
# INTERNAL HELPER FUNCTIONS (MBF)
# -----------------------------------------------------------------------------

function Invoke-MbfCommand {
    param (
        [string]$ExecutablePath,
        [string[]]$ArgumentList
    )

    if (-not (Test-Path $ExecutablePath)) {
        Throw "MBF Executable not found at $ExecutablePath"
    }

    $workDir = Split-Path -Parent $ExecutablePath
    
    $processArgs = $ArgumentList -join " "
    
    Write-Verbose "Executing: $ExecutablePath $processArgs"
    Write-Verbose "Working Directory: $workDir"

    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = $ExecutablePath
    $pinfo.WorkingDirectory = $workDir
    $pinfo.RedirectStandardOutput = $true
    $pinfo.RedirectStandardError = $true
    $pinfo.UseShellExecute = $false
    $pinfo.Arguments = $processArgs
    $pinfo.CreateNoWindow = $true
    
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    
    $output = $p.StandardOutput.ReadToEnd()
    $err = $p.StandardError.ReadToEnd()
    
    $p.WaitForExit()
    $exitCode = $p.ExitCode

    if ($err) {
        Write-Warning "MBF StdErr: $err"
    }
    
    return @{
        Output   = $output -split '\r?\n'
        ExitCode = $exitCode
    }
}

# -----------------------------------------------------------------------------
# PUBLIC MBF FUNCTIONS
# -----------------------------------------------------------------------------

function Invoke-MbfCensus {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)][string]$User,
        [Parameter(Mandatory=$true)][string]$Password,
        [Parameter(Mandatory=$true)][string[]]$Intermediary,
        [string]$PathToMBF,
        [int]$Timeout = 60
    )

    $argsList = @()
    $argsList += "C=Census"
    $argsList += "U=""$User"""
    $argsList += "P=""$Password"""
    $argsList += "T=$Timeout"
    
    $flatIntermediaries = $Intermediary | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    foreach ($i in $flatIntermediaries) { $argsList += "I=$i" }

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList
    
    $lunObjects = @()
    foreach ($line in $result.Output) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line -match "^(.+?):(.+?)[=\s]+([^|]*)\|?(.*?),?\s*$") {
            $lunObjects += [PSCustomObject]@{
                Server    = $matches[1].Trim()
                Drive     = $matches[2].Trim()
                LunSerial = $matches[3].Trim()
                LunWWN    = $matches[4].Trim()
            }
        }
    }
    
    return [PSCustomObject]@{
        Command   = "Census"
        ExitCode  = $result.ExitCode
        Luns      = $lunObjects
        RawOutput = $result.Output
    }
}

function Invoke-MbfQuiesce {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)][string]$User,
        [Parameter(Mandatory=$true)][string]$Password,
        [Parameter(Mandatory=$true)][string[]]$Intermediary,
        [string]$PathToMBF,
        [int]$Timeout,
        [string]$SessionFilename,
        [switch]$Force
    )

    $argsList = @()
    $argsList += "C=Quiesce"
    $argsList += "U=""$User"""
    $argsList += "P=""$Password"""
    $argsList += "T=$Timeout"
    
    $flatIntermediaries = $Intermediary | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    foreach ($i in $flatIntermediaries) { $argsList += "I=$i" }
    
    if ($SessionFilename) { $argsList += "F=""$SessionFilename""" }
    if ($Force) { $argsList += "M=force" }

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList
    
    $serverStatus = @()
    foreach ($line in $result.Output) {
        if ($line -match "^(.+?)[=-](.+?),?\s*$") {
            $serverStatus += [PSCustomObject]@{
                Server = $matches[1].Trim()
                Status = $matches[2].Trim()
            }
        }
    }

    $isReadyForBackup = ($result.ExitCode -lt 2) -or ($result.ExitCode -eq 10)

    return [PSCustomObject]@{
        Command      = "Quiesce"
        ExitCode     = $result.ExitCode
        ReadyToSnap  = $isReadyForBackup
        ServerStatus = $serverStatus
        RawOutput    = $result.Output
    }
}

function Invoke-MbfUnquiesce {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)][string]$User,
        [Parameter(Mandatory=$true)][string]$Password,
        [Parameter(Mandatory=$true)][string[]]$Intermediary,
        [string]$PathToMBF,
        [string]$SessionFilename
    )

    $argsList = @()
    $argsList += "C=Unquiesce"
    $argsList += "U=""$User"""
    $argsList += "P=""$Password"""
    
    $flatIntermediaries = $Intermediary | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    foreach ($i in $flatIntermediaries) { $argsList += "I=$i" }
    
    if ($SessionFilename) { $argsList += "F=""$SessionFilename""" }

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList

    $serverStatus = @()
    foreach ($line in $result.Output) {
        if ($line -match "^(.+?)[=-](.+?),?\s*$") {
            $serverStatus += [PSCustomObject]@{
                Server = $matches[1].Trim()
                Status = $matches[2].Trim()
            }
        }
    }

    return [PSCustomObject]@{
        Command      = "Unquiesce"
        ExitCode     = $result.ExitCode
        ServerStatus = $serverStatus
        RawOutput    = $result.Output
    }
}

# -----------------------------------------------------------------------------
# RSC FUNCTIONS
# -----------------------------------------------------------------------------

function Connect-Rsc {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [PSCustomObject]$ServiceAccount
    )
    
    $connectionData = [ordered]@{
        'client_id'     = $ServiceAccount.client_id
        'client_secret' = $ServiceAccount.client_secret
    } | ConvertTo-Json

    try {
        $rscSession = Invoke-RestMethod -Method Post -uri $ServiceAccount.access_token_uri -ContentType application/json -body $connectionData
        if ($rscSession.access_token) {
            return $rscSession
        } else {
            Throw "Unable to obtain access token from RSC."
        }
    }
    catch {
        Throw "Failed to connect to RSC: $_"
    }
}

function Disconnect-Rsc {
    [CmdletBinding()]
    param (
        [string]$LogoutUrl,
        [hashtable]$Headers
    )
    
    try {
        if ($LogoutUrl) {
            Invoke-WebRequest -Method Delete -Headers $Headers -ContentType "application/json; charset=utf-8" -Uri $LogoutUrl -ErrorAction SilentlyContinue | Out-Null
            Write-Verbose "Logged out from RSC."
        }
    }
    catch {
        Write-Warning "Logout failed (session may have already expired): $_"
    }
}

function Get-RscSlaDomains {
    [CmdletBinding()]
    param(
        [string]$ApiEndpoint,
        [hashtable]$Headers
    )
    
    $query = @"
query SLAListQuery(`$after: String, `$first: Int, `$filter: [GlobalSlaFilterInput!], `$sortBy: SlaQuerySortByField, `$sortOrder: SortOrder, `$shouldShowProtectedObjectCount: Boolean, `$shouldShowPausedClusters: Boolean = false) {
  slaDomains(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, shouldShowProtectedObjectCount: `$shouldShowProtectedObjectCount, shouldShowPausedClusters: `$shouldShowPausedClusters) {
    edges {
      node {
        name
        id
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
        "after"                                    = $null
        "first"                                    = 50
        "filter"                                   = @()
        "sortBy"                                   = "NAME"
        "sortOrder"                                = "ASC"
        "shouldShowProtectedObjectCount"           = $true
        "shouldShowPausedClusters"                 = $true
    }
    
    $allSlas = @()
    $hasNextPage = $true
    
    while ($hasNextPage) {
        $payload = @{ query = $query; variables = $variables }
        
        try {
            $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
        }
        catch {
            Throw "Failed to retrieve SLA domains via GraphQL: $_"
        }

        if ($response.data.slaDomains.edges.node) {
            $allSlas += $response.data.slaDomains.edges.node
        }
        
        $hasNextPage = $response.data.slaDomains.pageInfo.hasNextPage
        if ($hasNextPage) { $variables.after = $response.data.slaDomains.pageInfo.endCursor }
    }
    
    return $allSlas | Select-Object name, id | Sort-Object name
}

function Get-RscGcpProjects {
    [CmdletBinding()]
    param(
        [string]$ApiEndpoint,
        [hashtable]$Headers
    )

    $query = @"
query GCloudProjectsListQuery(`$first: Int!, `$after: String, `$sortBy: GcpNativeProjectSortFields, `$sortOrder: SortOrder!, `$filters: GcpNativeProjectFilters) {
  gcpNativeProjects(first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder, projectFilters: `$filters) {
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
    $variables = @{
        "first"     = 50
        "sortBy"    = "NAME"
        "sortOrder" = "ASC"
        "filters"   = @{ 
            "effectiveSlaFilter" = $null
            "nameOrNumberSubstringFilter" = $null 
        }
    }

    $allProjects = @()
    $hasNextPage = $true

    while ($hasNextPage) {
        $payload = @{ query = $query; variables = $variables }
        
        try {
            $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
        }
        catch {
            Throw "Failed to retrieve GCP Projects: $_"
        }

        if ($response.data.gcpNativeProjects.edges.node) {
            $allProjects += $response.data.gcpNativeProjects.edges.node
        }
        
        $hasNextPage = $response.data.gcpNativeProjects.pageInfo.hasNextPage
        if ($hasNextPage) { $variables.after = $response.data.gcpNativeProjects.pageInfo.endCursor }
    }

    return $allProjects | Select-Object name, nativeId, id | Sort-Object name
}

function Get-RscGcpInventory {
    [CmdletBinding()]
    param(
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
    
    # Construct filters
    $filterObj = @{ "relicFilter" = @{ "relic" = $false } }
    
    if (-not [string]::IsNullOrEmpty($GcpProjectId)) {
        $filterObj["projectFilter"] = @{ "projectIds" = @($GcpProjectId) }
    }

    $variables = @{
        "first"   = 50
        "filters" = $filterObj
    }

    $allInstances = @()
    $hasNextPage = $true

    Write-Verbose "Fetching RSC Inventory..."
    if ($GcpProjectId) { Write-Verbose "  Filtering by Project ID: $GcpProjectId" }

    while ($hasNextPage) {
        $payload = @{ query = $query; variables = $variables }
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
        
        # Error handling for invalid filters
        if ($response.errors) {
            Throw "GraphQL Error: $($response.errors.message)"
        }

        if ($response.data.gcpNativeGceInstances.edges.node) {
            $allInstances += $response.data.gcpNativeGceInstances.edges.node
        }
        
        $hasNextPage = $response.data.gcpNativeGceInstances.pageInfo.hasNextPage
        if ($hasNextPage) { $variables.after = $response.data.gcpNativeGceInstances.pageInfo.endCursor }
    }

    return $allInstances
}

function New-RscGcpSnapshot {
    [CmdletBinding()]
    param(
        [string]$ApiEndpoint,
        [hashtable]$Headers,
        [string[]]$SnappableIds,
        [string]$RetentionSlaId
    )

    $mutation = @"
mutation TakeOnDemandSnapshotSyncMutation(`$retentionSlaId: UUID!, `$snappableIds: [UUID!]!) {
  takeOnDemandSnapshotSync(
    input: {slaId: `$retentionSlaId, workloadIds: `$snappableIds}
  ) {
    workloadDetails {
      workloadId
      snapshotCreationTimestamp
      error
    }
  }
}
"@
    $variables = @{ "retentionSlaId" = $RetentionSlaId; "snappableIds" = $SnappableIds }
    $payload = @{ query = $mutation; variables = $variables }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
    }
    catch {
        $sw.Stop()
        Throw "Snapshot Failed: $_"
    }
    $sw.Stop()
    
    $serverDuration = 0
    if ($response.extensions.metrics.executionMs) { $serverDuration = $response.extensions.metrics.executionMs }

    return [PSCustomObject]@{
        WallClockDurationMs = $sw.ElapsedMilliseconds
        ServerExecutionMs   = $serverDuration
        Details             = $response.data.takeOnDemandSnapshotSync.workloadDetails
    }
}

# -----------------------------------------------------------------------------
# MAIN WORKFLOW EXECUTION
# -----------------------------------------------------------------------------

$ErrorActionPreference = "Stop"
$isQuiesced = $false

try {
    if ($PSCmdlet.ParameterSetName -eq "RunCensus") {
        Write-Host ">>> Running Census Only (No Backup, No RSC Connection)..." -ForegroundColor Cyan
        
        $censusResult = Invoke-MbfCensus `
            -User $MbfUser `
            -Password $MbfPassword `
            -Intermediary $MbfIntermediary `
            -PathToMBF $MbfPath `
            -Timeout $MbfTimeout

        # PRINT RAW OUTPUT
        Write-Host "    [RAW MBF CENSUS OUTPUT]" -ForegroundColor Gray
        $censusResult.RawOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
        Write-Host "    [END RAW OUTPUT]" -ForegroundColor Gray

        if ($censusResult.Luns.Count -gt 0) {
            Write-Host "`nParsed LUNs/Servers:" -ForegroundColor Green
            $censusResult.Luns | Format-Table -AutoSize
        } else {
            Write-Warning "Census completed but returned no parsed LUNs."
        }
        return
    }

    # 1. SETUP RSC CONNECTION
    Write-Host ">>> Step 1: Connecting to Rubrik Security Cloud..." -ForegroundColor Cyan
    if (-not (Test-Path $ServiceAccountJson)) { Throw "Service Account JSON not found." }
    
    $serviceAccountObj = Get-Content -Raw $ServiceAccountJson | ConvertFrom-Json
    $tokenInfo = Connect-Rsc -ServiceAccount $serviceAccountObj
    
    $rscHeaders = @{
        "Authorization" = "Bearer $($tokenInfo.access_token)"
        "Content-Type"  = "application/json"
    }
    $uriParts = $serviceAccountObj.access_token_uri -split "/"
    $rscHost = $uriParts[2]
    $graphqlUrl = "https://$rscHost/api/graphql"
    $restBaseUrl = "https://$rscHost"

    if ($PSCmdlet.ParameterSetName -eq "ListSlas") {
        Write-Host ">>> List Mode: Retrieving SLA Domains via GraphQL..." -ForegroundColor Cyan
        $slas = Get-RscSlaDomains -ApiEndpoint $graphqlUrl -Headers $rscHeaders
        if ($slas) {
            $slas | Format-Table -AutoSize
            Write-Host "SLA List Complete." -ForegroundColor Green
        } else {
            Write-Warning "No SLAs found or permission denied."
        }
        return 
    }

    if ($PSCmdlet.ParameterSetName -eq "ListProjects") {
        Write-Host ">>> List Mode: Retrieving GCP Projects via GraphQL..." -ForegroundColor Cyan
        $projects = Get-RscGcpProjects -ApiEndpoint $graphqlUrl -Headers $rscHeaders
        if ($projects) {
            $projects | Select-Object @{N='Project Name';E={$_.name}}, @{N='GCP Native ID';E={$_.nativeId}}, @{N='RSC Project ID';E={$_.id}} | Format-Table -AutoSize
            Write-Host "Project List Complete." -ForegroundColor Green
        } else {
            Write-Warning "No GCP Projects found."
        }
        return
    }

    # 2. DISCOVERY: CENSUS & INVENTORY
    Write-Host ">>> Step 2: Running Meditech Census to identify targets..." -ForegroundColor Cyan
    
    $censusResult = Invoke-MbfCensus `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath `
        -Timeout $MbfTimeout

    Write-Host "    [RAW MBF CENSUS OUTPUT]" -ForegroundColor Gray
    $censusResult.RawOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    Write-Host "    [END RAW OUTPUT]" -ForegroundColor Gray

    if ($censusResult.Luns.Count -eq 0) {
        Throw "Meditech Census returned no LUNs/Servers. Check MBF configuration or connectivity."
    }

    $meditechHosts = $censusResult.Luns.Server | Select-Object -Unique
    Write-Host "    Census identified $($meditechHosts.Count) unique host(s): $($meditechHosts -join ', ')" -ForegroundColor Gray

    if ($GcpProjectId) {
        Write-Host "    Filtering Inventory by Project ID: $GcpProjectId" -ForegroundColor Cyan
    }
    Write-Host "    Fetching RSC Inventory..." -ForegroundColor Cyan
    $inventory = Get-RscGcpInventory -ApiEndpoint $graphqlUrl -Headers $rscHeaders -GcpProjectId $GcpProjectId
    
    # Match Census Hosts to RSC Inventory (Case-insensitive match on Native Name)
    $targetWorkloads = $inventory | Where-Object { 
        $meditechHosts -contains $_.nativeName 
    }

    $foundHosts = $targetWorkloads.nativeName
    $missingHosts = $meditechHosts | Where-Object { $foundHosts -notcontains $_ }
    
    if ($missingHosts) {
        Write-Warning "The following Meditech servers were NOT found in the Rubrik Inventory:"
        Write-Warning ($missingHosts -join ", ")
    }

    if ($targetWorkloads.Count -eq 0) {
        Throw "No matching GCE Instances found in RSC Inventory corresponding to Meditech Census."
    }
    
    $snappableIds = $targetWorkloads.id
    Write-Host "    Resolved $($snappableIds.Count) VM(s) to snapshot." -ForegroundColor Green

    # 3. QUIESCE MEDITECH
    Write-Host ">>> Step 3: Quiescing Meditech..." -ForegroundColor Yellow
    
    $quiesceResult = Invoke-MbfQuiesce `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath `
        -Timeout $MbfTimeout `
        -Force:$Force

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Host "    [RAW MBF QUIESCE OUTPUT]" -ForegroundColor Gray
    $quiesceResult.RawOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    Write-Host "    [END RAW OUTPUT]" -ForegroundColor Gray

    if (-not $quiesceResult.ReadyToSnap) {
        # CRITICAL FAILURE PATH
        Write-Error "Quiesce Failed (Exit Code: $($quiesceResult.ExitCode))."
        
        # Suggest Force if applicable and not already used
        if (-not $Force -and ($quiesceResult.ExitCode -eq 8 -or $quiesceResult.ExitCode -eq 9)) {
            Write-Warning "MBF indicates this operation might succeed with -Force."
        }

        # Handle Partial Failures (Code 2) or Force Failures (Code 9 requires unquiesce before retry)
        if ($quiesceResult.ExitCode -eq 2 -or $quiesceResult.ExitCode -eq 9) {
            Write-Warning "Partial failure detected. Attempting immediate Unquiesce cleanup..."
            Invoke-MbfUnquiesce -User $MbfUser -Password $MbfPassword -Intermediary $MbfIntermediary -PathToMBF $MbfPath
        }
        Throw "Aborting Workflow due to Quiesce Failure."
    }

    # SAFETY FLAG ON: System is now frozen
    $isQuiesced = $true
    Write-Host "    Quiesce Successful (Code $($quiesceResult.ExitCode))." -ForegroundColor Green

    # 4. SNAPSHOT (CRITICAL TIMING)
    Write-Host ">>> Step 4: Initiating Rubrik Snapshots..." -ForegroundColor Yellow
    try {
        $snapResult = New-RscGcpSnapshot `
            -ApiEndpoint $graphqlUrl `
            -Headers $rscHeaders `
            -SnappableIds $snappableIds `
            -RetentionSlaId $RetentionSlaId
        
        Write-Host "    Snapshot Request Complete." -ForegroundColor Green
        Write-Host "    Wall Clock Time: $($snapResult.WallClockDurationMs) ms" -ForegroundColor Cyan
        Write-Host "    Server Exec Time: $($snapResult.ServerExecutionMs) ms" -ForegroundColor Cyan
    }
    catch {
        Write-Error "Snapshot failed! $_"
    }

    # 5. UNQUIESCE (IMMEDIATE)
    Write-Host ">>> Step 5: Unquiescing Meditech..." -ForegroundColor Yellow
    $uqResult = Invoke-MbfUnquiesce `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Host "    [RAW MBF UNQUIESCE OUTPUT]" -ForegroundColor Gray
    $uqResult.RawOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    Write-Host "    [END RAW OUTPUT]" -ForegroundColor Gray

    if ($uqResult.ExitCode -lt 2) {
        Write-Host "    Unquiesce Successful." -ForegroundColor Green
        $isQuiesced = $false
    } else {
        Write-Error "    Unquiesce Failed! (Code $($uqResult.ExitCode)). Check System Immediately."
    }

}
catch {
    Write-Error "Workflow Error: $_"
}
finally {
    # 6. EMERGENCY SAFETY NET
    if ($isQuiesced) {
        Write-Warning "EMERGENCY: Script exited while Meditech was still Quiesced."
        Write-Warning "Attempting Emergency Unquiesce..."
        
        try {
            $emgResult = Invoke-MbfUnquiesce `
                -User $MbfUser `
                -Password $MbfPassword `
                -Intermediary $MbfIntermediary `
                -PathToMBF $MbfPath
            
            Write-Host "Emergency Unquiesce Output:"
            $emgResult.RawOutput | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        }
        catch {
            Write-Error "FATAL: Failed to execute Emergency Unquiesce. Manual intervention required immediately."
        }
    }

    # 7. DISCONNECT
    if ($rscHeaders) {
        Write-Host ">>> Step 6: Disconnecting..." -ForegroundColor DarkGray
        Disconnect-Rsc -Headers $rscHeaders
    }
}
