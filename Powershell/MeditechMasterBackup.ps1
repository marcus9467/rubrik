 <#
.SYNOPSIS
    Meditech Backup & Rubrik Snapshot Orchestration Script. For use with Meditech Expanse in GCP.
.DESCRIPTION
    Orchestrates the backup workflow for Meditech on GCP using Rubrik Security Cloud (RSC).
    1. Authenticates to RSC.
    2. Runs MBF Census to dynamically discover active Meditech Servers.
    3. Fetches GCP Inventory and correlates with Census results.
    4. Quiesces Meditech via MBF.exe.
    5. Triggers Rubrik Bulk Snapshot (Mutation) for the discovered VMs.
    6. Unquiesces Meditech immediately.
.EXAMPLE
    .\MeditechMasterWorkflow.ps1 `
        -ServiceAccountJson "C:\creds\rsc_service_account.json" `
        -RetentionSlaId "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" `
        -MbfUser "ISB" `
        -MbfPassword "Secret" `
        -MbfIntermediary "RUB-MBI:2987"
.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : January 2026
    Company : Rubrik Inc
#>
[cmdletbinding()]
param (
    # RSC Parameters
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,

    # Removed -GceInstanceIds as we now use Census for discovery

    [parameter(Mandatory=$true)]
    [string]$RetentionSlaId,

    # MBF Parameters
    [Parameter(Mandatory=$true)]
    [string]$MbfUser,

    [Parameter(Mandatory=$true)]
    [string]$MbfPassword,

    [Parameter(Mandatory=$true)]
    [string[]]$MbfIntermediary,

    [string]$MbfPath = "C:\Program Files (x86)\MEDITECH\MBI\mbf.exe",
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

    # Set Working Directory (Essential for older binaries relying on local config/DLLs)
    $workDir = Split-Path -Parent $ExecutablePath
    
    # Construct proper argument string
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
    
    # Read streams BEFORE waiting for exit to prevent deadlocks
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
    foreach ($i in $Intermediary) { $argsList += "I=$i" }

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList
    
    $lunObjects = @()
    foreach ($line in $result.Output) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        # Regex: Server:Drive=Serial|WWN (Permissive)
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
    foreach ($i in $Intermediary) { $argsList += "I=$i" }
    
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

    # Success criteria: Exit Code < 2 or Code 10 (Success with warnings)
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
    foreach ($i in $Intermediary) { $argsList += "I=$i" }
    
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
# Rubrik Security Cloud Functions
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

function Get-RscGcpInventory {
    [CmdletBinding()]
    param(
        [string]$ApiEndpoint,
        [hashtable]$Headers
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
    # Reduced variables to minimal requirement for ID resolution
    $variables = @{
        "first"   = 50
        "filters" = @{ "relicFilter" = @{ "relic" = $false } }
    }

    $allInstances = @()
    $hasNextPage = $true

    Write-Verbose "Fetching RSC Inventory..."
    while ($hasNextPage) {
        $payload = @{ query = $query; variables = $variables }
        $response = Invoke-RestMethod -Method Post -Uri $ApiEndpoint -Headers $Headers -Body ($payload | ConvertTo-Json -Depth 5) -ContentType "application/json"
        
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

try {
    # 1. SETUP RSC CONNECTION
    Write-Host ">>> Step 1: Connecting to Rubrik Security Cloud..." -ForegroundColor Cyan
    if (-not (Test-Path $ServiceAccountJson)) { Throw "Service Account JSON not found." }
    
    $serviceAccountObj = Get-Content -Raw $ServiceAccountJson | ConvertFrom-Json
    $tokenInfo = Connect-Rsc -ServiceAccount $serviceAccountObj
    
    $rscHeaders = @{
        "Authorization" = "Bearer $($tokenInfo.access_token)"
        "Content-Type"  = "application/json"
    }
    
    # Extract API URL hostname from token URI (e.g., https://mycluster.rubrik.com/...)
    $baseUri = $serviceAccountObj.access_token_uri -replace "/.*", "" 
    $uriParts = $serviceAccountObj.access_token_uri -split "/"
    $graphqlUrl = "https://$($uriParts[2])/api/graphql"

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

    # Extract unique server names from Census (Case-insensitive)
    $meditechHosts = $censusResult.Luns.Server | Select-Object -Unique
    Write-Host "    Census identified $($meditechHosts.Count) unique host(s): $($meditechHosts -join ', ')" -ForegroundColor Gray

    Write-Host "    Fetching RSC Inventory..." -ForegroundColor Cyan
    $inventory = Get-RscGcpInventory -ApiEndpoint $graphqlUrl -Headers $rscHeaders
    
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
        -Timeout $MbfTimeout

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Host "    [RAW MBF QUIESCE OUTPUT]" -ForegroundColor Gray
    $quiesceResult.RawOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    Write-Host "    [END RAW OUTPUT]" -ForegroundColor Gray

    if (-not $quiesceResult.ReadyToSnap) {
        Write-Error "Quiesce Failed (Exit Code: $($quiesceResult.ExitCode))."
        
        if ($quiesceResult.ExitCode -eq 2) {
            Write-Warning "Partial failure detected. Attempting immediate Unquiesce cleanup..."
            Invoke-MbfUnquiesce -User $MbfUser -Password $MbfPassword -Intermediary $MbfIntermediary -PathToMBF $MbfPath
        }
        Throw "Aborting Workflow due to Quiesce Failure."
    }

    Write-Host "    Quiesce Successful (Code $($quiesceResult.ExitCode))." -ForegroundColor Green

    # 4. SNAPSHOT 
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

    # 5. UNQUIESCE 
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
    } else {
        Write-Error "    Unquiesce Failed! (Code $($uqResult.ExitCode)). Check System Immediately."
    }

}
catch {
    Write-Error "Workflow Error: $_"
}
finally {
    # 6. DISCONNECT
    if ($rscHeaders) {
        Write-Host ">>> Step 6: Disconnecting..." -ForegroundColor DarkGray
        Disconnect-Rsc -Headers $rscHeaders
    }
} 
