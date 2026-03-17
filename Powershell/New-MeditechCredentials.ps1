<#
.SYNOPSIS
    One-time setup: creates an encrypted credential file and optionally registers a
    Windows Scheduled Task for MeditechMasterScript.ps1.

.DESCRIPTION
    New-MeditechCredentials.ps1 is an interactive wizard that walks through five steps:

      Step 1 — MBF username and password (DPAPI-encrypted in output file).
      Step 2 — MBF Intermediary host(s) (e.g. RUB-MBI:2987).
      Step 3 — RSC service account. Load from an existing JSON file or enter manually.
               The client_secret is stored as a DPAPI-encrypted SecureString.
      Step 4 — RSC Retention SLA ID (UUID). Use -ListSlas on MeditechMasterScript.ps1
               to find this value.
      Step 5 — GCP Project ID filter (optional). Use -ListProjects on
               MeditechMasterScript.ps1 to find this value.

    All data is exported to a single DPAPI-encrypted XML file via Export-Clixml.
    The file can only be decrypted by the same Windows user account on the same machine.

    After saving credentials, the wizard optionally registers a Windows Scheduled Task
    that runs MeditechMasterScript.ps1 on a daily schedule or every X hours.

    WHEN TO RE-RUN THIS SCRIPT
    --------------------------
    - MBF password rotated.
    - RSC service account rotated.
    - SLA domain or GCP project changed.
    - Migrating to a new machine or service account.
    - Updating the scheduled task schedule.

.PARAMETER OutputPath
    Full path for the encrypted credential XML output file.
    Defaults to C:\ProgramData\Rubrik\MeditechCreds.xml.
    The directory is created automatically if it does not exist.
    This path must match the -MbfConfigXml value used with MeditechMasterScript.ps1.

.EXAMPLE
    # Run as the Task Scheduler service account (standard usage).
    # The wizard prompts for all required values interactively.
    .\New-MeditechCredentials.ps1

.EXAMPLE
    # Write the credential file to a non-default path.
    .\New-MeditechCredentials.ps1 -OutputPath "D:\Rubrik\MeditechCreds.xml"

    # Reference the same path when running the main script:
    .\MeditechMasterScript.ps1 -MbfConfigXml "D:\Rubrik\MeditechCreds.xml" -EnableLogging

.EXAMPLE
    # Re-run to update credentials or re-register the scheduled task.
    # The existing XML file will be overwritten — have all credentials ready.
    .\New-MeditechCredentials.ps1

.NOTES
    Author  : Marcus Henderson
    Created : March 2026
    Company : Rubrik Inc

    PREREQUISITES
    -------------
    - Run as the Windows account that Task Scheduler will use to execute
      MeditechMasterScript.ps1. DPAPI ties the encrypted output file to this specific
      user + machine combination. Running as the wrong account means the main script
      will fail to decrypt the file.
    - Run as Administrator if you want to register a Scheduled Task. Task registration
      under \Rubrik\ in Task Scheduler Library requires elevated rights.
    - The RSC service account JSON file (if using the file import option in Step 3)
      must be readable from this machine at the time this script runs.

    SECURITY MODEL
    --------------
    - MBF password and RSC client_secret are stored as SecureString in the XML.
      Export-Clixml encrypts these fields using Windows DPAPI — they cannot be read
      by any other user or on any other machine.
    - All other fields (client_id, access_token_uri, SLA ID, GCP project, etc.) are
      stored as plain strings. They are configuration values, not secrets.
    - Consider restricting the XML file's ACL after setup so only the service account
      can read it:
        icacls "C:\ProgramData\Rubrik\MeditechCreds.xml" /inheritance:r /grant:r "${env:USERNAME}:(R)"

    RELATED SCRIPTS
    ---------------
    MeditechMasterScript.ps1 — Operational backup script that consumes this credential file.
#>
param (
    [Parameter()]
    [string]$OutputPath = "C:\ProgramData\Rubrik\MeditechCreds.xml"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "=== Meditech Credential Setup ===" -ForegroundColor Cyan
Write-Host "All credentials will be DPAPI-encrypted and stored at:"
Write-Host "  $OutputPath" -ForegroundColor Yellow
Write-Host ""
Write-Host "IMPORTANT: Run this script as the same user account that Task Scheduler" -ForegroundColor Magenta
Write-Host "           will use to execute MeditechMasterScript.ps1." -ForegroundColor Magenta
Write-Host ""

# -----------------------------------------------------------------------------
# STEP 1: MBF CREDENTIALS
# -----------------------------------------------------------------------------
Write-Host "Step 1 of 5: MBF Credentials" -ForegroundColor Cyan
Write-Host "        The password will be masked and DPAPI-encrypted in the output file." -ForegroundColor Gray

$mbfCredential = Get-Credential -Message "Enter MBF Username and Password"

if (-not $mbfCredential) {
    Write-Error "No MBF credential provided. Aborting."
    exit 1
}

# -----------------------------------------------------------------------------
# STEP 2: MBF INTERMEDIARY
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "Step 2 of 5: MBF Intermediary host(s)" -ForegroundColor Cyan
Write-Host "        For a single intermediary : RUB-MBI:2987" -ForegroundColor Gray
Write-Host "        For multiple intermediaries: RUB-MBI:2987,RUB-MATFS:2987" -ForegroundColor Gray
Write-Host ""

$intermediaryInput = (Read-Host "MBF Intermediary").Trim().Trim('"').Trim("'")

if ([string]::IsNullOrWhiteSpace($intermediaryInput)) {
    Write-Error "Intermediary cannot be empty. Aborting."
    exit 1
}

$intermediaryNormalized = (($intermediaryInput -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }) -join ","

# -----------------------------------------------------------------------------
# STEP 3: RSC SERVICE ACCOUNT
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "Step 3 of 5: RSC Service Account" -ForegroundColor Cyan
Write-Host "        Load from an existing service account JSON file, or enter values manually." -ForegroundColor Gray
Write-Host "        The client secret will be DPAPI-encrypted in the output file." -ForegroundColor Gray
Write-Host ""

$rscJsonPath = (Read-Host "Path to RSC service account JSON (press Enter to enter manually)").Trim().Trim('"').Trim("'")

$rscClientId     = $null
$rscTokenUri     = $null
$rscName         = $null
$rscClientSecret = $null

if (-not [string]::IsNullOrWhiteSpace($rscJsonPath)) {
    if (-not (Test-Path $rscJsonPath)) {
        Write-Error "File not found: '$rscJsonPath'. Aborting."
        exit 1
    }
    try {
        $rscJson = Get-Content -Raw $rscJsonPath | ConvertFrom-Json
    } catch {
        Write-Error "Failed to parse RSC service account JSON: $_"
        exit 1
    }
    $rscClientId     = $rscJson.client_id
    $rscTokenUri     = $rscJson.access_token_uri
    $rscName         = $rscJson.name
    $rscClientSecret = $rscJson.client_secret | ConvertTo-SecureString -AsPlainText -Force

    Write-Host "    Loaded from file." -ForegroundColor Gray
    Write-Host "    Name      : $rscName" -ForegroundColor Gray
    Write-Host "    Client ID : $rscClientId" -ForegroundColor Gray
    Write-Host "    Token URI : $rscTokenUri" -ForegroundColor Gray
} else {
    Write-Host ""
    $rscClientId     = (Read-Host "RSC Client ID").Trim().Trim('"').Trim("'")
    $rscTokenUri     = (Read-Host "RSC Access Token URI").Trim().Trim('"').Trim("'")
    $rscName         = (Read-Host "RSC Service Account Name (optional, for reference)").Trim().Trim('"').Trim("'")
    $rscClientSecret = Read-Host "RSC Client Secret" -AsSecureString
}

if ([string]::IsNullOrWhiteSpace($rscClientId) -or [string]::IsNullOrWhiteSpace($rscTokenUri)) {
    Write-Error "RSC Client ID and Access Token URI are required. Aborting."
    exit 1
}

# -----------------------------------------------------------------------------
# STEP 4: RETENTION SLA ID
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "Step 4 of 5: RSC Retention SLA ID" -ForegroundColor Cyan
Write-Host "        The UUID of the SLA domain snapshots will be retained under." -ForegroundColor Gray
Write-Host "        Run MeditechMasterScript.ps1 with -ListSlas to find this value." -ForegroundColor Gray
Write-Host ""

$retentionSlaId = (Read-Host "Retention SLA ID (UUID)").Trim().Trim('"').Trim("'")

if ([string]::IsNullOrWhiteSpace($retentionSlaId)) {
    Write-Error "Retention SLA ID cannot be empty. Aborting."
    exit 1
}

# -----------------------------------------------------------------------------
# STEP 5: GCP PROJECT ID (OPTIONAL)
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "Step 5 of 5: GCP Project ID Filter (optional)" -ForegroundColor Cyan
Write-Host "        Limits the RSC inventory search to a specific GCP project." -ForegroundColor Gray
Write-Host "        Run MeditechMasterScript.ps1 with -ListProjects to find this value." -ForegroundColor Gray
Write-Host "        Press Enter to skip (all projects will be searched)." -ForegroundColor Gray
Write-Host ""

$gcpProjectIdInput = (Read-Host "GCP Project ID (or press Enter to skip)").Trim().Trim('"').Trim("'")
$gcpProjectId = if ([string]::IsNullOrWhiteSpace($gcpProjectIdInput)) { $null } else { $gcpProjectIdInput }

# -----------------------------------------------------------------------------
# ENSURE OUTPUT DIRECTORY EXISTS
# -----------------------------------------------------------------------------
$outputDir = Split-Path -Parent $OutputPath
if (-not (Test-Path $outputDir)) {
    try {
        New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
        Write-Host "Created directory: $outputDir" -ForegroundColor Gray
    } catch {
        Write-Error "Could not create output directory '$outputDir': $_"
        exit 1
    }
}

# -----------------------------------------------------------------------------
# BUILD AND EXPORT
# -----------------------------------------------------------------------------
# Export-Clixml DPAPI-encrypts SecureString fields (MBF password, RSC client secret).
# All other fields are stored as plain strings — they are not sensitive secrets.
$credStore = [PSCustomObject]@{
    # MBF
    MbfCredential   = $mbfCredential           # PSCredential  — DPAPI encrypted
    MbfIntermediary = $intermediaryNormalized   # plain string

    # RSC Service Account
    RscClientId     = $rscClientId             # plain string
    RscClientSecret = $rscClientSecret         # SecureString  — DPAPI encrypted
    RscTokenUri     = $rscTokenUri             # plain string
    RscName         = $rscName                 # plain string

    # Backup Configuration
    RetentionSlaId  = $retentionSlaId          # plain string
    GcpProjectId    = $gcpProjectId            # plain string or $null
}

try {
    $credStore | Export-Clixml -Path $OutputPath -Force
} catch {
    Write-Error "Failed to write credential file: $_"
    exit 1
}

Write-Host ""
Write-Host "=== Credentials Saved ===" -ForegroundColor Green
Write-Host "Credential file written to : $OutputPath" -ForegroundColor Green
Write-Host ""
Write-Host "Encrypted for user    : $env:USERDOMAIN\$env:USERNAME" -ForegroundColor Yellow
Write-Host "Encrypted for machine : $env:COMPUTERNAME" -ForegroundColor Yellow

# -----------------------------------------------------------------------------
# TASK SCHEDULER REGISTRATION (OPTIONAL)
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "=== Windows Task Scheduler ===" -ForegroundColor Cyan
Write-Host "Would you like to register a scheduled task to run the backup automatically?" -ForegroundColor Cyan
Write-Host "(You can always do this manually later.)" -ForegroundColor Gray
Write-Host ""

$createTask = (Read-Host "Create scheduled task? (Y/N)").Trim()

if ($createTask -match '^[Yy]') {

    # --- SCRIPT PATH ---
    Write-Host ""
    $masterScriptPath = (Read-Host "Full path to MeditechMasterScript.ps1").Trim().Trim('"').Trim("'")

    if ([string]::IsNullOrWhiteSpace($masterScriptPath) -or -not (Test-Path $masterScriptPath)) {
        Write-Warning "Script not found at '$masterScriptPath'. Skipping task registration."
        Write-Warning "You can register the task manually — see instructions at the end of this output."
    } else {

        # --- TASK NAME ---
        Write-Host ""
        $taskNameInput = (Read-Host "Task name (press Enter for default: 'Rubrik - Meditech Backup')").Trim().Trim('"').Trim("'")
        $taskName = if ([string]::IsNullOrWhiteSpace($taskNameInput)) { "Rubrik - Meditech Backup" } else { $taskNameInput }

        # --- SCHEDULE TYPE ---
        Write-Host ""
        Write-Host "Schedule options:" -ForegroundColor Cyan
        Write-Host "  1  Daily at a specific time (e.g. nightly at 2:00 AM)" -ForegroundColor Gray
        Write-Host "  2  Every X hours (e.g. every 6 hours)" -ForegroundColor Gray
        Write-Host ""
        $scheduleChoice = (Read-Host "Schedule type (1 or 2, press Enter for 1)").Trim()

        $trigger = $null
        $scheduleDescription = ""

        if ($scheduleChoice -eq "2") {
            $hoursInput = (Read-Host "Run every how many hours? (e.g. 4, 6, 12)").Trim()
            [int]$intervalHours = 0
            if (-not [int]::TryParse($hoursInput, [ref]$intervalHours) -or $intervalHours -lt 1 -or $intervalHours -gt 23) {
                Write-Warning "Invalid value — defaulting to every 6 hours."
                $intervalHours = 6
            }
            # Start at the next clean hour boundary
            $startAt = (Get-Date).Date.AddHours([math]::Ceiling((Get-Date).TimeOfDay.TotalHours))
            $trigger = New-ScheduledTaskTrigger -Once -At $startAt `
                -RepetitionInterval (New-TimeSpan -Hours $intervalHours)
            $scheduleDescription = "Every $intervalHours hour(s), starting at $($startAt.ToString('yyyy-MM-dd HH:mm'))"
        } else {
            $timeInput = (Read-Host "Daily run time in 24-hour HH:MM format (press Enter for 02:00)").Trim().Trim('"').Trim("'")
            $runTime   = if ([string]::IsNullOrWhiteSpace($timeInput)) { "02:00" } else { $timeInput }
            # Validate the time format loosely
            if ($runTime -notmatch '^\d{1,2}:\d{2}$') {
                Write-Warning "Unrecognised time format '$runTime' — defaulting to 02:00."
                $runTime = "02:00"
            }
            $trigger = New-ScheduledTaskTrigger -Daily -At $runTime
            $scheduleDescription = "Daily at $runTime"
        }

        # --- RUN-AS ACCOUNT ---
        # The task must run as the account that created the encrypted XML, because
        # DPAPI binds the decryption key to that specific user account on this machine.
        $runAsUser = "$env:USERDOMAIN\$env:USERNAME"
        Write-Host ""
        Write-Host "The task will run as: $runAsUser" -ForegroundColor Yellow
        Write-Host "(This must be the same account used to create the encrypted credential file.)" -ForegroundColor Gray
        Write-Host "Enter the password for this account so the task can run when no one is logged in." -ForegroundColor Gray
        Write-Host ""
        $taskPasswordSec = Read-Host "Account password" -AsSecureString
        $taskPasswordPlain = [System.Net.NetworkCredential]::new('', $taskPasswordSec).Password

        # --- BUILD AND REGISTER ---
        $psArgs  = "-NonInteractive -ExecutionPolicy Bypass -File `"$masterScriptPath`" -MbfConfigXml `"$OutputPath`" -EnableLogging"
        $action  = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $psArgs

        $settings = New-ScheduledTaskSettingsSet `
            -ExecutionTimeLimit  (New-TimeSpan -Hours 2) `
            -StartWhenAvailable `
            -MultipleInstances   IgnoreNew `
            -RunOnlyIfNetworkAvailable

        try {
            Register-ScheduledTask `
                -TaskName    $taskName `
                -TaskPath    "\Rubrik\" `
                -Description "Rubrik Meditech GCP backup orchestration — quiesce, snapshot, unquiesce." `
                -Trigger     $trigger `
                -Action      $action `
                -Settings    $settings `
                -RunLevel    Highest `
                -User        $runAsUser `
                -Password    $taskPasswordPlain `
                -Force | Out-Null

            Write-Host ""
            Write-Host "=== Scheduled Task Registered ===" -ForegroundColor Green
            Write-Host "  Task path  : \Rubrik\$taskName" -ForegroundColor Gray
            Write-Host "  Schedule   : $scheduleDescription" -ForegroundColor Gray
            Write-Host "  Run as     : $runAsUser" -ForegroundColor Gray
            Write-Host "  Log file   : C:\ProgramData\Rubrik\Logs\MeditechBackup.log" -ForegroundColor Gray
            Write-Host ""
            Write-Host "View or edit in Task Scheduler under: Task Scheduler Library > Rubrik" -ForegroundColor Cyan
        }
        catch {
            Write-Warning "Failed to register the scheduled task: $_"
            Write-Host "You may need to run this script as Administrator to register scheduled tasks." -ForegroundColor Yellow
            Write-Host "See the manual instructions below." -ForegroundColor Yellow
        }
        finally {
            # Clear the plaintext password from memory as soon as it is no longer needed.
            $taskPasswordPlain = $null
        }
    }
}

# -----------------------------------------------------------------------------
# FINAL SUMMARY
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Manual Task Scheduler invocation (if needed):" -ForegroundColor Cyan
Write-Host "  powershell.exe -NonInteractive -ExecutionPolicy Bypass ``" -ForegroundColor Gray
Write-Host "      -File `"<path to MeditechMasterScript.ps1>`" ``" -ForegroundColor Gray
Write-Host "      -MbfConfigXml `"$OutputPath`" ``" -ForegroundColor Gray
Write-Host "      -EnableLogging" -ForegroundColor Gray
Write-Host ""
Write-Host "Useful one-off commands:" -ForegroundColor Cyan
Write-Host "  # List available SLA domains:" -ForegroundColor Gray
Write-Host "  .\MeditechMasterScript.ps1 -MbfConfigXml `"$OutputPath`" -ListSlas" -ForegroundColor Gray
Write-Host ""
Write-Host "  # List GCP projects:" -ForegroundColor Gray
Write-Host "  .\MeditechMasterScript.ps1 -MbfConfigXml `"$OutputPath`" -ListProjects" -ForegroundColor Gray
Write-Host ""
Write-Host "  # Run a Census only (no backup, no RSC connection):" -ForegroundColor Gray
Write-Host "  .\MeditechMasterScript.ps1 -MbfConfigXml `"$OutputPath`" -CensusOnly" -ForegroundColor Gray
Write-Host ""
