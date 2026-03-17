<#
.SYNOPSIS
    Orchestrates Meditech GCP backup: MBF quiesce, Rubrik snapshot, MBF unquiesce.

.DESCRIPTION
    MeditechMasterScript.ps1 automates the full crash-consistent backup cycle for
    Meditech environments hosted on GCP and protected by Rubrik Security Cloud (RSC).

    WORKFLOW (RunBackup mode — default)
    ------------------------------------
    Step 1  Authenticate to RSC using a service account.
    Step 2  Run MBF Census to discover active Meditech servers and resolve them to
            GCP VM IDs in the RSC inventory.
    Step 3  Quiesce Meditech via mbf.exe (freeze I/O for a consistent snapshot).
    Step 4  Trigger a Rubrik bulk snapshot for all resolved VMs, using the specified
            retention SLA domain. Unquiesce is issued immediately after the API call
            regardless of snapshot outcome.
    Step 5  Unquiesce Meditech.
    Step 6  Disconnect from RSC.

    CREDENTIAL MODEL
    ----------------
    Recommended: use -MbfConfigXml pointing to the file created by
    New-MeditechCredentials.ps1. That file is DPAPI-encrypted and contains:
      - MBF username and password
      - MBF intermediary host(s)
      - RSC service account (client_id, client_secret, access_token_uri)
      - Retention SLA ID
      - Optional GCP project ID filter

    Alternative: supply -ServiceAccountJson, -RetentionSlaId, -MbfUser, -MbfPassword,
    and -MbfIntermediary directly on the command line (not recommended for scheduled
    tasks — credentials appear in process audit logs).

    PARAMETER SETS
    --------------
    RunBackup    (default) Full quiesce-snapshot-unquiesce cycle.
    RunCensus    (-CensusOnly) MBF Census only — no RSC connection, no snapshot.
    ListSlas     (-ListSlas)   Print RSC SLA domains — use to find -RetentionSlaId.
    ListProjects (-ListProjects) Print RSC GCP projects — use to find -GcpProjectId.

    MBF EXIT CODE REFERENCE
    -----------------------
    Code  Meaning
    ----  -------
    0     Success — all servers responded successfully.
    1     Partial success — some servers did not respond (treated as success by this
          script; individual server status is logged).
    2     MBF is already in the quiesced state (cleanup unquiesce is triggered).
    3     Unrecognised command or syntax error.
    4     Authentication failure (bad username/password).
    5     Connection failure — could not reach the intermediary.
    6     Invalid intermediary specification.
    7     Timeout waiting for server response.
    8     Meditech servers are busy; retry recommended (-Force retries automatically).
    9     Internal MBF error; retry recommended (-Force retries automatically).
    10    M-AT platform — partial success (treated as success by this script).
    11    M-AT platform — all servers responded (treated as success by this script).
    12    Already quiesced (alternate code); cleanup unquiesce is triggered.

.PARAMETER MbfConfigXml
    Path to the DPAPI-encrypted XML credential file created by New-MeditechCredentials.ps1.
    When supplied, this file provides MBF credentials, RSC service account details,
    retention SLA ID, and optional GCP project ID — making all other credential
    parameters optional.
    The file is decryptable only by the Windows user account and machine that created it.
    Default path if you used New-MeditechCredentials.ps1 without -OutputPath:
      C:\ProgramData\Rubrik\MeditechCreds.xml

.PARAMETER ServiceAccountJson
    Path to an RSC service account JSON file. Used when -MbfConfigXml is not supplied
    or does not contain embedded RSC credentials.
    The JSON must contain: client_id, client_secret, access_token_uri.
    Use New-MeditechCredentials.ps1 to embed the service account into the encrypted
    XML instead of leaving client_secret in a plaintext JSON file.

.PARAMETER RetentionSlaId
    UUID of the RSC SLA domain to apply to snapshots taken in RunBackup mode.
    Can be embedded in -MbfConfigXml (recommended) or supplied here.
    Use -ListSlas to retrieve available SLA domain IDs.

.PARAMETER GcpProjectId
    Optional. RSC GCP project ID to filter the VM inventory search.
    When omitted, all GCP projects visible to the service account are searched.
    Can be embedded in -MbfConfigXml or supplied here.
    Use -ListProjects to retrieve available project IDs.

.PARAMETER Force
    When specified, automatically retries the quiesce operation if MBF returns exit
    code 8 (servers busy) or 9 (internal error). Without -Force, the script stops
    and reports the failure.

.PARAMETER MbfUser
    MBF username. Used when -MbfConfigXml is not supplied.
    This credential is passed to mbf.exe at runtime and is NOT written to disk.

.PARAMETER MbfPassword
    MBF password (plain text). Used when -MbfConfigXml is not supplied.
    Avoid supplying this on the command line for scheduled tasks; use -MbfConfigXml
    instead so the password is never visible in process/audit logs.

.PARAMETER MbfIntermediary
    One or more MBF intermediary host:port strings.
    Single:   "RUB-MBI:2987"
    Multiple: "RUB-MBI:2987,RUB-MATFS:2987"   (comma-separated string or string array)
    Used when -MbfConfigXml is not supplied.

.PARAMETER MbfPath
    Full path to the mbf.exe binary.
    Default: C:\Program Files (x86)\MEDITECH\MBI\mbf.exe

.PARAMETER MbfTimeout
    Seconds to allow each mbf.exe invocation (Census, Quiesce, Unquiesce) to run
    before the process is forcibly terminated.
    Default: 90 seconds.

.PARAMETER CensusOnly
    Switch. Runs MBF Census and prints the discovered server list, then exits.
    Does not connect to RSC, does not quiesce, does not take snapshots.
    Useful for validating MBF connectivity before scheduling a full backup.

.PARAMETER ListSlas
    Switch. Connects to RSC and lists all visible SLA domains (name + UUID).
    Use the UUID as -RetentionSlaId (or embed it via New-MeditechCredentials.ps1).

.PARAMETER ListProjects
    Switch. Connects to RSC and lists all visible GCP projects
    (project name, GCP native ID, RSC project UUID).
    Use the RSC project UUID as -GcpProjectId (or embed it via
    New-MeditechCredentials.ps1).

.PARAMETER EnableLogging
    Switch. Appends timestamped log entries to the file specified by -LogPath.
    All console output is also written to the log file.
    Recommended for scheduled task deployments.

.PARAMETER LogPath
    Path to the log file used when -EnableLogging is specified.
    The directory is created automatically if it does not exist.
    Default: C:\ProgramData\Rubrik\Logs\MeditechBackup.log

.EXAMPLE
    # Standard scheduled-task invocation using the encrypted credential file.
    # Run New-MeditechCredentials.ps1 once as the service account to create the XML.
    .\MeditechMasterScript.ps1 -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" -EnableLogging

.EXAMPLE
    # Discover available SLA domains to find the correct RetentionSlaId.
    .\MeditechMasterScript.ps1 -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" -ListSlas

.EXAMPLE
    # Discover available GCP projects to find the correct GcpProjectId.
    .\MeditechMasterScript.ps1 -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" -ListProjects

.EXAMPLE
    # Validate MBF connectivity without connecting to RSC or taking any snapshots.
    .\MeditechMasterScript.ps1 -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" -CensusOnly

.EXAMPLE
    # Census only using inline MBF credentials (no XML required).
    .\MeditechMasterScript.ps1 `
        -MbfUser "ISB" `
        -MbfPassword "Secret" `
        -MbfIntermediary "RUB-MBI:2987" `
        -CensusOnly

.EXAMPLE
    # Full backup with inline credentials (not recommended for scheduled tasks).
    # Use -MbfConfigXml to avoid exposing secrets in process audit logs.
    .\MeditechMasterScript.ps1 `
        -ServiceAccountJson "C:\creds\rsc_service_account.json" `
        -RetentionSlaId "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" `
        -MbfUser "ISB" `
        -MbfPassword "Secret" `
        -MbfIntermediary "RUB-MBI:2987" `
        -EnableLogging

.EXAMPLE
    # Full backup with multiple MBF intermediaries and Force retry enabled.
    .\MeditechMasterScript.ps1 `
        -MbfConfigXml "C:\ProgramData\Rubrik\MeditechCreds.xml" `
        -MbfIntermediary "RUB-MBI:2987,RUB-MATFS:2987" `
        -Force `
        -EnableLogging

.NOTES
    Author  : Marcus Henderson
    Created : January 2026
    Company : Rubrik Inc

    PREREQUISITES
    -------------
    - Windows PowerShell 5.1 or later.
    - mbf.exe installed at -MbfPath (default: C:\Program Files (x86)\MEDITECH\MBI\mbf.exe).
    - RSC service account with GCP inventory read permissions and snapshot permissions.
    - When using -MbfConfigXml, this script must run as the same Windows user account
      that created the XML file (DPAPI decryption is user + machine bound).
    - Run New-MeditechCredentials.ps1 once before scheduling this script.

    LOGGING
    -------
    With -EnableLogging, each run appends to the log file. The log file is never
    rotated by this script — implement external log rotation if needed.

    RELATED SCRIPTS
    ---------------
    New-MeditechCredentials.ps1 — one-time setup wizard that creates the encrypted
    XML credential file consumed by this script.
#>
[cmdletbinding(DefaultParameterSetName = "RunBackup")]
param (
    # RSC service account JSON path. Optional when -MbfConfigXml is supplied and contains
    # RSC credentials (generated by New-MeditechCredentials.ps1).
    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "ListSlas")]
    [Parameter(ParameterSetName = "ListProjects")]
    [Parameter(ParameterSetName = "RunCensus")]
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
    # RetentionSlaId is optional when -MbfConfigXml contains an embedded SLA ID.
    [parameter(ParameterSetName = "RunBackup")]
    [string]$RetentionSlaId,

    [parameter(ParameterSetName = "RunBackup")]
    [string]$GcpProjectId, # Optional filter for inventory; can also be embedded in -MbfConfigXml

    [Parameter(ParameterSetName = "RunBackup")]
    [switch]$Force,

    # Encrypted credential + config file (recommended for Task Scheduler).
    # Run New-MeditechCredentials.ps1 once to create this file. It is DPAPI-encrypted
    # and can only be decrypted by the same Windows user account on the same machine.
    # When provided, replaces the need for -ServiceAccountJson, -RetentionSlaId,
    # -MbfUser, -MbfPassword, and -MbfIntermediary.
    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [Parameter(ParameterSetName = "ListSlas")]
    [Parameter(ParameterSetName = "ListProjects")]
    [string]$MbfConfigXml,

    # MBF Parameters (alternative to -MbfConfigXml; required when -MbfConfigXml is not used)
    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [string]$MbfUser,

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [string]$MbfPassword,

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [string[]]$MbfIntermediary,

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [string]$MbfPath = "C:\Program Files (x86)\MEDITECH\MBI\mbf.exe",

    [Parameter(ParameterSetName = "RunBackup")]
    [Parameter(ParameterSetName = "RunCensus")]
    [int]$MbfTimeout = 90,

    # Logging Parameters
    [Parameter()]
    [switch]$EnableLogging,

    [Parameter()]
    [string]$LogPath = "C:\ProgramData\Rubrik\Logs\MeditechBackup.log"
)

# -----------------------------------------------------------------------------
# LOGGING HELPER FUNCTIONS
# -----------------------------------------------------------------------------

function Get-Timestamp {
    return Get-Date -Format "yyyy-MM-dd HH:mm:ss"
}

function Write-Log {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$Message,
        
        [Parameter(Position=1)]
        [ConsoleColor]$ForegroundColor,
        
        [string]$Level = "INFO" # INFO, WARNING, ERROR
    )
    
    $ts = Get-Timestamp
    
    # 1. File Output
    if ($script:EnableLogging -and $script:LogPath) {
        $logLine = "[$ts] [$Level] $Message"
        try {
            Add-Content -Path $script:LogPath -Value $logLine -ErrorAction SilentlyContinue
        } catch {
        }
    }
    
    # 2. Console Output
    switch ($Level) {
        "INFO" {
            $consoleMsg = "[$ts] $Message"
            if ($ForegroundColor) {
                Write-Host $consoleMsg -ForegroundColor $ForegroundColor
            } else {
                Write-Host $consoleMsg
            }
        }
        "WARNING" {
            # Write-Warning automatically adds "WARNING: " prefix and yellow color
            Write-Warning "[$ts] $Message"
        }
        "ERROR" {
            # Write-Error automatically adds error stream formatting
            Write-Error "[$ts] $Message"
        }
    }
}

# -----------------------------------------------------------------------------
# INTERNAL HELPER FUNCTIONS (MBF)
# -----------------------------------------------------------------------------

function Invoke-MbfCommand {
    param (
        [string]$ExecutablePath,
        [string[]]$ArgumentList,
        [int]$ProcessTimeoutSeconds = 120
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

    # Read both streams concurrently via async tasks to prevent pipe buffer deadlock.
    # Synchronously reading one stream before the other can deadlock if the process
    # fills the opposite stream's OS buffer before finishing.
    $stdoutTask = $p.StandardOutput.ReadToEndAsync()
    $stderrTask  = $p.StandardError.ReadToEndAsync()

    $completed = $p.WaitForExit($ProcessTimeoutSeconds * 1000)

    if (-not $completed) {
        try { $p.Kill() } catch {}
        Throw "MBF process did not complete within $ProcessTimeoutSeconds seconds and was terminated. Check MBF connectivity and server health."
    }

    $output   = $stdoutTask.Result
    $err      = $stderrTask.Result
    $exitCode = $p.ExitCode

    if ($err) {
        Write-Log "MBF StdErr: $err" -Level Warning
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

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList -ProcessTimeoutSeconds ($Timeout + 30)

    $lunObjects = @()
    foreach ($line in $result.Output) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        # Regex matches MBF Census output format: SERVERNAME:SERVER DRIVE=SERIAL|WWN,
        # e.g. MTFS01:MTFS01 E=363031...|60000970...,
        # Using non-lazy character classes to correctly isolate each field boundary.
        if ($line -match "^([^:]+):([^=]+)=([^|]*)\|?(.*?),?\s*$") {
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

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList -ProcessTimeoutSeconds ($Timeout + 30)

    $serverStatus = @()
    foreach ($line in $result.Output) {
        if ($line -match "^([^=]+)=([^,]+),?\s*$") {
            $serverStatus += [PSCustomObject]@{
                Server = $matches[1].Trim()
                Status = $matches[2].Trim()
            }
        }
    }

    # Success criteria per MBF docs:
    #   0  = Complete success
    #   1  = Some servers unavailable, presumed OK
    #   10 = All ready, some M-AT servers not quiesced at transaction boundary
    #   11 = Some unavailable AND some M-AT not at transaction boundary (same semantics as 1)
    $isReadyForBackup = ($result.ExitCode -lt 2) -or ($result.ExitCode -eq 10) -or ($result.ExitCode -eq 11)

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

    $result = Invoke-MbfCommand -ExecutablePath $PathToMBF -ArgumentList $argsList -ProcessTimeoutSeconds 60

    $serverStatus = @()
    foreach ($line in $result.Output) {
        if ($line -match "^([^=]+)=([^,]+),?\s*$") {
            $serverStatus += [PSCustomObject]@{
                Server = $matches[1].Trim()
                Status = $matches[2].Trim()
            }
        }
    }

    # Success criteria per MBF docs (mirrors quiesce logic):
    #   0  = Complete success
    #   1  = Some servers unavailable, presumed OK
    #   10 = All ready, some M-AT servers not quiesced at transaction boundary
    #   11 = Some unavailable AND some M-AT not at transaction boundary (same semantics as 1)
    $unquiesceSuccess = ($result.ExitCode -lt 2) -or ($result.ExitCode -eq 10) -or ($result.ExitCode -eq 11)

    return [PSCustomObject]@{
        Command          = "Unquiesce"
        ExitCode         = $result.ExitCode
        UnquiesceSuccess = $unquiesceSuccess
        ServerStatus     = $serverStatus
        RawOutput        = $result.Output
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
        Write-Log "Logout failed (session may have already expired): $_" -Level Warning
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
        "after"                          = $null
        "first"                          = 50
        "filter"                         = @()
        "sortBy"                         = "NAME"
        "sortOrder"                      = "ASC"
        "shouldShowProtectedObjectCount" = $true
        "shouldShowPausedClusters"       = $true
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

# --- LOGGING SETUP ---
if ($EnableLogging) {
    # 1. Ensure Directory Exists
    $logDir = Split-Path -Parent $LogPath
    if (-not (Test-Path $logDir)) {
        try {
            New-Item -ItemType Directory -Path $logDir -Force | Out-Null
            Write-Log "Created log directory: $logDir" -ForegroundColor DarkGray
        } catch {
            Write-Warning "Could not create log directory: $_"
        }
    }

    # 2. Check Rotation (2MB Limit)
    if (Test-Path $LogPath) {
        $logFileItem = Get-Item $LogPath
        # 2MB in bytes = 2 * 1024 * 1024 = 2097152
        if ($logFileItem.Length -gt 2MB) {
            $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
            $archiveName = "{0}_{1}{2}" -f $logFileItem.BaseName, $timestamp, $logFileItem.Extension
            $archivePath = Join-Path $logDir $archiveName
            
            try {
                Rename-Item -Path $LogPath -NewName $archiveName -Force
                Write-Log "Log file rotated: $archiveName" -ForegroundColor DarkGray
            } catch {
                Write-Warning "Failed to rotate log file: $_"
            }
        }
    }
    
}

$ErrorActionPreference = "Stop"
$isQuiesced = $false
$exitCode   = 0

try {
    # --- LOAD ENCRYPTED CONFIG XML (if provided) ---
    # Loads once here; both MBF and RSC resolution blocks consume it below.
    $mbfStore = $null
    if ($MbfConfigXml) {
        if (-not (Test-Path $MbfConfigXml)) {
            Throw "Config XML not found at '$MbfConfigXml'. Run New-MeditechCredentials.ps1 to create it."
        }
        Write-Log "    Loading credentials from encrypted config: $MbfConfigXml" -ForegroundColor DarkGray
        try {
            $mbfStore = Import-Clixml -Path $MbfConfigXml
        } catch {
            Throw "Failed to decrypt config XML. Ensure you are running as the user who created the file on this machine. Error: $_"
        }
    }

    # --- RESOLVE MBF CREDENTIALS ---
    $needsMbfCreds = ($PSCmdlet.ParameterSetName -eq "RunBackup" -or $PSCmdlet.ParameterSetName -eq "RunCensus")
    if ($needsMbfCreds) {
        if ($mbfStore) {
            $MbfUser         = $mbfStore.MbfCredential.UserName
            $MbfPassword     = $mbfStore.MbfCredential.GetNetworkCredential().Password
            $MbfIntermediary = @($mbfStore.MbfIntermediary -split ',\s*' | Where-Object { $_ })
        } elseif (-not $MbfUser -or -not $MbfPassword -or -not $MbfIntermediary) {
            Throw "MBF credentials not provided. Supply either -MbfConfigXml (recommended) or all three of -MbfUser, -MbfPassword, and -MbfIntermediary."
        }
    }

    # --- RESOLVE RSC SERVICE ACCOUNT ---
    $serviceAccountObj = $null
    $needsRsc = ($PSCmdlet.ParameterSetName -ne "RunCensus")
    if ($needsRsc) {
        if ($mbfStore -and $mbfStore.RscClientId) {
            # Decrypt the RSC client secret from its DPAPI-protected SecureString.
            # NetworkCredential is the PS 5.1-compatible way to convert SecureString to plaintext.
            $rscSecret = [System.Net.NetworkCredential]::new('', $mbfStore.RscClientSecret).Password
            $serviceAccountObj = [PSCustomObject]@{
                client_id        = $mbfStore.RscClientId
                client_secret    = $rscSecret
                access_token_uri = $mbfStore.RscTokenUri
                name             = $mbfStore.RscName
            }
            # Pull config values from XML only if not already supplied as explicit parameters.
            if (-not $RetentionSlaId -and $mbfStore.RetentionSlaId) { $RetentionSlaId = $mbfStore.RetentionSlaId }
            if (-not $GcpProjectId  -and $mbfStore.GcpProjectId)    { $GcpProjectId   = $mbfStore.GcpProjectId  }
        } elseif ($ServiceAccountJson) {
            if (-not (Test-Path $ServiceAccountJson)) { Throw "Service Account JSON not found at '$ServiceAccountJson'." }
            $serviceAccountObj = Get-Content -Raw $ServiceAccountJson | ConvertFrom-Json
        } else {
            Throw "RSC credentials not provided. Supply either -MbfConfigXml (with RSC credentials embedded) or -ServiceAccountJson."
        }
    }

    # For backup mode, RetentionSlaId must be resolved by now.
    if ($PSCmdlet.ParameterSetName -eq "RunBackup" -and [string]::IsNullOrEmpty($RetentionSlaId)) {
        Throw "RetentionSlaId not provided. Supply -RetentionSlaId or embed it in -MbfConfigXml via New-MeditechCredentials.ps1."
    }

    # --- CHECK FOR CENSUS ONLY MODE ---
    if ($PSCmdlet.ParameterSetName -eq "RunCensus") {
        Write-Log ">>> Running Census Only (No Backup, No RSC Connection)..." -ForegroundColor Cyan
        
        $censusResult = Invoke-MbfCensus `
            -User $MbfUser `
            -Password $MbfPassword `
            -Intermediary $MbfIntermediary `
            -PathToMBF $MbfPath `
            -Timeout $MbfTimeout

        # PRINT RAW OUTPUT
        Write-Log "    [RAW MBF CENSUS OUTPUT]" -ForegroundColor Gray
        $censusResult.RawOutput | ForEach-Object { Write-Log "      $_" -ForegroundColor Gray }
        Write-Log "    [END RAW OUTPUT]" -ForegroundColor Gray

        if ($censusResult.Luns.Count -gt 0) {
            Write-Log "`nParsed LUNs/Servers:" -ForegroundColor Green
            $lunTable = $censusResult.Luns | Format-Table -AutoSize | Out-String
            Write-Log $lunTable
        } else {
            Write-Log "Census completed but returned no parsed LUNs." -Level Warning
        }
        return
    }

    # 1. SETUP RSC CONNECTION
    Write-Log ">>> Step 1: Connecting to Rubrik Security Cloud..." -ForegroundColor Cyan
    $tokenInfo = Connect-Rsc -ServiceAccount $serviceAccountObj
    
    $rscHeaders = @{
        "Authorization" = "Bearer $($tokenInfo.access_token)"
        "Content-Type"  = "application/json"
    }
    
    $rscHost    = ([System.Uri]$serviceAccountObj.access_token_uri).Host
    $graphqlUrl = "https://$rscHost/api/graphql"

    # --- CHECK FOR LIST MODE: SLAs ---
    if ($PSCmdlet.ParameterSetName -eq "ListSlas") {
        Write-Log ">>> List Mode: Retrieving SLA Domains via GraphQL..." -ForegroundColor Cyan
        $slas = Get-RscSlaDomains -ApiEndpoint $graphqlUrl -Headers $rscHeaders
        if ($slas) {
            $slaTable = $slas | Format-Table -AutoSize | Out-String
            Write-Log $slaTable
            Write-Log "SLA List Complete." -ForegroundColor Green
        } else {
            Write-Log "No SLAs found or permission denied." -Level Warning
        }
        return 
    }

    if ($PSCmdlet.ParameterSetName -eq "ListProjects") {
        Write-Log ">>> List Mode: Retrieving GCP Projects via GraphQL..." -ForegroundColor Cyan
        $projects = Get-RscGcpProjects -ApiEndpoint $graphqlUrl -Headers $rscHeaders
        if ($projects) {
            $projTable = $projects | Select-Object @{N='Project Name';E={$_.name}}, @{N='GCP Native ID';E={$_.nativeId}}, @{N='RSC Project ID';E={$_.id}} | Format-Table -AutoSize | Out-String
            Write-Log $projTable
            Write-Log "Project List Complete." -ForegroundColor Green
        } else {
            Write-Log "No GCP Projects found." -Level Warning
        }
        return
    }

    # 2. DISCOVERY: CENSUS & INVENTORY
    Write-Log ">>> Step 2: Running Meditech Census to identify targets..." -ForegroundColor Cyan
    
    $censusResult = Invoke-MbfCensus `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath `
        -Timeout $MbfTimeout

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Log "    [RAW MBF CENSUS OUTPUT]" -ForegroundColor Gray
    $censusResult.RawOutput | ForEach-Object { Write-Log "      $_" -ForegroundColor Gray }
    Write-Log "    [END RAW OUTPUT]" -ForegroundColor Gray

    if ($censusResult.Luns.Count -eq 0) {
        Throw "Meditech Census returned no LUNs/Servers. Check MBF configuration or connectivity."
    }

    # Extract unique server names from Census (Case-insensitive)
    $meditechHosts = $censusResult.Luns.Server | Select-Object -Unique
    Write-Log "    Census identified $($meditechHosts.Count) unique host(s): $($meditechHosts -join ', ')" -ForegroundColor Gray

    if ($GcpProjectId) {
        Write-Log "    Filtering Inventory by Project ID: $GcpProjectId" -ForegroundColor Cyan
    }
    Write-Log "    Fetching RSC Inventory..." -ForegroundColor Cyan
    $inventory = Get-RscGcpInventory -ApiEndpoint $graphqlUrl -Headers $rscHeaders -GcpProjectId $GcpProjectId
    
    # Match Census Hosts to RSC Inventory (Case-insensitive match on Native Name)
    $targetWorkloads = $inventory | Where-Object { 
        $meditechHosts -contains $_.nativeName 
    }

    $foundHosts = $targetWorkloads.nativeName
    $missingHosts = $meditechHosts | Where-Object { $foundHosts -notcontains $_ }
    
    if ($missingHosts) {
        Write-Log "The following Meditech servers were NOT found in the Rubrik Inventory:" -Level Warning
        Write-Log ($missingHosts -join ", ") -Level Warning
    }

    if ($targetWorkloads.Count -eq 0) {
        Throw "No matching GCE Instances found in RSC Inventory corresponding to Meditech Census."
    }
    
    $snappableIds = $targetWorkloads.id
    Write-Log "    Resolved $($snappableIds.Count) VM(s) to snapshot." -ForegroundColor Green

    # 3. QUIESCE MEDITECH
    Write-Log ">>> Step 3: Quiescing Meditech..." -ForegroundColor Yellow
    
    if ($Force) {
        Write-Log "    [FORCE MODE ACTIVE]: Attempting Quiesce with M=force..." -ForegroundColor Magenta
    }

    $quiesceResult = Invoke-MbfQuiesce `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath `
        -Timeout $MbfTimeout `
        -Force:$Force

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Log "    [RAW MBF QUIESCE OUTPUT]" -ForegroundColor Gray
    $quiesceResult.RawOutput | ForEach-Object { Write-Log "      $_" -ForegroundColor Gray }
    Write-Log "    [END RAW OUTPUT]" -ForegroundColor Gray

    if (-not $quiesceResult.ReadyToSnap) {
        # CRITICAL FAILURE PATH
        Write-Log "Quiesce Failed (Exit Code: $($quiesceResult.ExitCode))." -Level Error
        
        # Suggest Force if applicable and not already used
        if (-not $Force -and ($quiesceResult.ExitCode -eq 8 -or $quiesceResult.ExitCode -eq 9)) {
            Write-Log "MBF indicates this operation might succeed with -Force." -Level Warning
        }

        # Handle Partial Failures (Code 2/12) or Force Failures (Code 9 requires unquiesce before retry)
        # Docs state codes 2 and 12 both explicitly require a subsequent unquiesce.
        # Code 9 (tryforce with some quiesced) also requires unquiesce before retrying with -Force.
        if ($quiesceResult.ExitCode -eq 2 -or $quiesceResult.ExitCode -eq 9 -or $quiesceResult.ExitCode -eq 12) {
            Write-Log "Partial failure detected. Attempting immediate Unquiesce cleanup..." -Level Warning
            Invoke-MbfUnquiesce -User $MbfUser -Password $MbfPassword -Intermediary $MbfIntermediary -PathToMBF $MbfPath
        }
        Throw "Aborting Workflow due to Quiesce Failure."
    }

    # SAFETY FLAG ON: System is now frozen
    $isQuiesced = $true
    Write-Log "    Quiesce Successful (Code $($quiesceResult.ExitCode))." -ForegroundColor Green

    # 4. SNAPSHOT (CRITICAL TIMING)
    Write-Log ">>> Step 4: Initiating Rubrik Snapshots..." -ForegroundColor Yellow
    try {
        $snapResult = New-RscGcpSnapshot `
            -ApiEndpoint $graphqlUrl `
            -Headers $rscHeaders `
            -SnappableIds $snappableIds `
            -RetentionSlaId $RetentionSlaId
        
        Write-Log "    Snapshot Request Complete." -ForegroundColor Green
        Write-Log "    Wall Clock Time: $($snapResult.WallClockDurationMs) ms" -ForegroundColor Cyan
        Write-Log "    Server Exec Time: $($snapResult.ServerExecutionMs) ms" -ForegroundColor Cyan
    }
    catch {
        Write-Log "Snapshot failed! $_" -Level Error
        $exitCode = 1
    }

    # 5. UNQUIESCE (IMMEDIATE)
    Write-Log ">>> Step 5: Unquiescing Meditech..." -ForegroundColor Yellow
    $uqResult = Invoke-MbfUnquiesce `
        -User $MbfUser `
        -Password $MbfPassword `
        -Intermediary $MbfIntermediary `
        -PathToMBF $MbfPath

    # PRINT RAW OUTPUT FOR DEBUG/LOGGING
    Write-Log "    [RAW MBF UNQUIESCE OUTPUT]" -ForegroundColor Gray
    $uqResult.RawOutput | ForEach-Object { Write-Log "      $_" -ForegroundColor Gray }
    Write-Log "    [END RAW OUTPUT]" -ForegroundColor Gray

    if ($uqResult.UnquiesceSuccess) {
        Write-Log "    Unquiesce Successful (Code $($uqResult.ExitCode))." -ForegroundColor Green
        # SAFETY FLAG OFF: System is thawed
        $isQuiesced = $false
    } else {
        Write-Log "    Unquiesce Failed! (Code $($uqResult.ExitCode)). Check System Immediately." -Level Error
    }

}
catch {
    Write-Log "Workflow Error: $_" -Level Error
    $exitCode = 1
}
finally {
    # 6. EMERGENCY SAFETY NET
    if ($isQuiesced) {
        Write-Log "EMERGENCY: Script exited while Meditech was still Quiesced." -Level Warning
        Write-Log "Attempting Emergency Unquiesce..." -Level Warning

        try {
            $emgResult = Invoke-MbfUnquiesce `
                -User $MbfUser `
                -Password $MbfPassword `
                -Intermediary $MbfIntermediary `
                -PathToMBF $MbfPath

            Write-Log "Emergency Unquiesce Output:"
            $emgResult.RawOutput | ForEach-Object { Write-Log "  $_" -ForegroundColor Red }
        }
        catch {
            Write-Log "FATAL: Failed to execute Emergency Unquiesce. Manual intervention required immediately." -Level Error
            $exitCode = 1
        }
    }

    # Final summary and exit code (RunBackup only — list/census modes are interactive)
    if ($PSCmdlet.ParameterSetName -eq "RunBackup") {
        if ($exitCode -eq 0) {
            Write-Log "=== Backup workflow completed successfully. ===" -ForegroundColor Green
        } else {
            Write-Log "=== Backup workflow completed with errors. Review log for details. ===" -Level Error
        }
    }

    exit $exitCode
}
