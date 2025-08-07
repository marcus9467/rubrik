############################################################################################
# Start of the script - Description, Requirements & Legal Disclaimer
############################################################################################
# Written by: Joshua Stenhouse joshuastenhouse@gmail.com
# Updated by: Marcus Henderson - 2025/08/06
##################################
# Change Log:
# Overhauled script to be more modular and use functions
# Updated the authentication logic to use RSC service accounts
# Updated the certificate handling to be more inclusive to newer versions of Powershell and alternative OSes
################################## 
# Description:
# This script creates multiple SQL live mounts from the CSV specified
################################## 
# Requirements:
# - Run PowerShell as administrator with command "Set-ExecutionPolcity unrestricted" on the host running the script
# - A Rubrik cluster or EDGE appliance, network access to it and credentials to login
# - At least 1 sql database protected in Rubrik and therefore 1 windows host
# - A CSV with the following fields: SourceSQLHostName,SourceInstanceName,SourceDatabaseName,TargetSQLHostName,TargetInstanceName,TargetDatabaseName
# - Example CSV Line = 172.17.60.69,SQLEXPRESS,SE-JSTENHOUSE-AdventureWorks2016,172.17.60.69,SQLEXPRESS,AdventureWorks2016-LiveMount1
# - Example CSV Line = AvailabilityGroup,SQLAG1,vSphereChangeControlv1,SQL16-VM01.lab.local,MSSQLSERVER,DemoDBLiveMount2
# - All options specified are tested to see if they exist in Rubrik
# - This script always mounts the latest snapshot available
##################################
# Legal Disclaimer:
# This script is written by Joshua Stenhouse is not supported under any support program or service. 
# All scripts are provided AS IS without warranty of any kind. 
# The author further disclaims all implied warranties including, without limitation, any implied warranties of merchantability or of fitness for a particular purpose. 
# The entire risk arising out of the use or performance of the sample scripts and documentation remains with you. 
# In no event shall its authors, or anyone else involved in the creation, production, or delivery of the scripts be liable for any damages whatsoever 
# (including, without limitation, damages for loss of business profits, business interruption, loss of business information, or other pecuniary loss) arising out of the use of or inability to use the sample scripts or documentation, 
# even if the author has been advised of the possibility of such damages.
##################################
function Configure-ScriptVariables {
    <#
    .SYNOPSIS
        Configures script variables for the Rubrik cluster and file paths.
    .DESCRIPTION
        This function defines and returns a PSCustomObject containing all the necessary
        configuration variables, making them easy to manage in one place.
    #>
    param()
    
    $config = [PSCustomObject]@{
        RubrikCluster   = ""
        ApiEndpoint     = "/api/v1/service_account/session"
        ScriptDirectory = ""
        SQLLiveMountCSV = ""
        LogDirectory    = ""
        JsonFilePath    = ""
    }

    return $config
}

function Invoke-RubrikAuthentication {
    <#
    .SYNOPSIS
        Authenticates with the Rubrik cluster using a service account.
    .DESCRIPTION
        This function reads the service account credentials from a JSON file,
        handles certificate validation based on the OS and PowerShell version,
        and authenticates with the Rubrik cluster's API. It returns a session
        token and the required headers for subsequent API calls.
    .PARAMETER RubrikCluster
        The IP address or hostname of the Rubrik cluster.
    .PARAMETER ApiEndpoint
        The API endpoint for service account authentication.
    .PARAMETER JsonFilePath
        The path to the JSON file containing the service account credentials.
    .PARAMETER SslSkipCheck
        A boolean to determine if SSL certificate checks should be skipped.
        This is typically set based on OS and PowerShell version.
    #>
    param(
        [string]$RubrikCluster,
        [string]$ApiEndpoint,
        [string]$JsonFilePath,
        [switch]$SslSkipCheck
    )

    Write-Host "Attempting to authenticate with Rubrik cluster $RubrikCluster..."

    $apiParams = @{}
    if ($SslSkipCheck) {
        Write-Host "Skipping SSL certificate check."
        $apiParams.Add("SkipCertificateCheck", $true)
    }

    try {
        $credentials = Get-Content -Path $JsonFilePath | ConvertFrom-Json
        $body = @{
            serviceAccountId = $credentials.client_id
            secret = $credentials.client_secret
        } | ConvertTo-Json
        
        $authUri = "https://$RubrikCluster$ApiEndpoint"
        $authParams = @{
            Uri         = $authUri
            Method      = "Post"
            Body        = $body
            ContentType = "application/json"
            ErrorAction = "Stop"
        }
        
        $authParams = $authParams + $apiParams
        $RubrikSessionResponse = Invoke-RestMethod @authParams
        
        $token = $RubrikSessionResponse.token
        $headers = @{'Authorization' = "Bearer $token"}
        
        Write-Host "Authentication successful. Session token retrieved."
        return [PSCustomObject]@{
            Headers = $headers
            ApiParams = $apiParams
        }
    }
    catch {
        Write-Error "Authentication failed. Error: $($_.Exception.Message)"
        return $null
    }
}

function Get-RubrikApiData {
    <#
    .SYNOPSIS
        Fetches all necessary SQL information from the Rubrik API.
    .DESCRIPTION
        This function retrieves lists of SQL databases, instances, and availability groups
        from the Rubrik cluster's API endpoints. It returns a PSCustomObject
        containing all the API data.
    .PARAMETER BaseUrl
        The base URL for the Rubrik cluster API.
    .PARAMETER InternalUrl
        The internal URL for the Rubrik cluster API.
    .PARAMETER ApiParams
        The parameters required for all API calls (headers, content type, etc.).
    #>
    param(
        [string]$BaseUrl,
        [string]$InternalUrl,
        [hashtable]$ApiParams
    )

    $apiEndpoints = @{
        'SQLDBList'             = "$BaseUrl/mssql/db?limit=5000&is_relic=false"
        'SQLInstanceList'       = "$BaseUrl/mssql/instance?limit=5000"
        'SQLAvailabilityGroupList' = "$InternalUrl/mssql/availability_group"
    }

    $apiData = [PSCustomObject]@{
        SQLDBList             = $null
        SQLInstanceList       = $null
        SQLAvailabilityGroupList = $null
    }

    foreach ($key in $apiEndpoints.Keys) {
        try {
            Write-Host "Fetching $($key)..."
            $url = $apiEndpoints[$key]
            $callParams = @{
                Uri = $url
                TimeoutSec = 100
                ErrorAction = "Stop"
            }
            $callParams = $callParams + $ApiParams

            $response = Invoke-RestMethod @callParams
            $apiData.$key = $response.data
            Write-Host "Successfully fetched $($apiData.$key.Count) records for $($key)."
        }
        catch {
            Write-Error "Failed to fetch $key. Error: $($_.Exception.Message)"
            return $null
        }
    }
    return $apiData
}

function Process-SqlData {
    <#
    .SYNOPSIS
        Processes raw Rubrik API data into structured arrays.
    .DESCRIPTION
        This function takes the raw API output for SQL databases, instances, and
        availability groups and creates two user-friendly arrays: one for SQL instances
        and one for SQL databases, including host information for easier lookups.
    .PARAMETER ApiData
        A PSCustomObject containing the raw API data (databases, instances, AGs).
    #>
    param(
        [PSCustomObject]$ApiData
    )

    $SQLInstanceArray = @()
    foreach ($instance in $ApiData.SQLInstanceList) {
        $SQLInstanceArray += [PSCustomObject]@{
            InstanceName = $instance.name
            InstanceID   = $instance.id
            HostName     = $instance.rootProperties.rootName
            HostNameID   = $instance.rootProperties.rootId
            DBCount      = ($ApiData.SQLDBList | Where-Object { $_.instanceID -eq $instance.id }).Count
        }
    }
    
    $SQLDBArray = @()
    foreach ($db in $ApiData.SQLDBList) {
        $hostName = ""
        $instanceName = $db.instanceName

        if ($db.isInAvailabilityGroup) {
            $hostName = "AvailabilityGroup"
            $ag = $ApiData.SQLAvailabilityGroupList | Where-Object { $_.id -eq $db.availabilityGroupId }
            if ($ag) { $instanceName = $ag.name }
        } else {
            $instanceInfo = $SQLInstanceArray | Where-Object { $_.InstanceID -eq $db.instanceId }
            if ($instanceInfo) { $hostName = $instanceInfo.HostName }
        }
        
        $SQLDBArray += [PSCustomObject]@{
            HostName     = $hostName
            InstanceName = $instanceName
            InstanceID   = $db.instanceId
            DatabaseName = $db.name
            DatabaseID   = $db.id
        }
    }
    return [PSCustomObject]@{
        SQLInstanceArray = $SQLInstanceArray
        SQLDBArray       = $SQLDBArray
    }
}

function New-SQLLiveMount {
    <#
    .SYNOPSIS
        Performs a single SQL live mount operation.
    .DESCRIPTION
        This function takes the details for a single live mount from the CSV, validates
        that the source and target exist in Rubrik, retrieves the latest snapshot,
        and initiates the live mount via the API. It polls the job status until completion.
    .PARAMETER MountInfo
        A PSCustomObject representing a single row from the CSV.
    .PARAMETER RubrikData
        A PSCustomObject containing the processed SQL instance and database data.
    .PARAMETER BaseUrl
        The base URL for the Rubrik cluster API.
    .PARAMETER ApiParams
        The parameters required for all API calls (headers, content type, etc.).
    .PARAMETER UseLatestRecoveryPoint
        An optional switch to use the '.latestRecoveryPoint' parameter from the
        database object instead of querying all snapshots.
    #>
    param(
        [Parameter(Mandatory=$true)]
        [PSCustomObject]$MountInfo,
        [Parameter(Mandatory=$true)]
        [PSCustomObject]$RubrikData,
        [Parameter(Mandatory=$true)]
        [string]$BaseUrl,
        [Parameter(Mandatory=$true)]
        [hashtable]$ApiParams,
        
        [switch]$UseLatestRecoveryPoint
    )

    Write-Host "--------------------------------------------"
    Write-Host "Processing Live Mount Request..."
    Write-Host "  Source: DB:`"$($MountInfo.SourceDatabaseName)`" | Instance:`"$($MountInfo.SourceInstanceName)`" | Host:`"$($MountInfo.SourceSQLHostName)`""
    Write-Host "  Target: DB:`"$($MountInfo.TargetDatabaseName)`" | Instance:`"$($MountInfo.TargetInstanceName)`" | Host:`"$($MountInfo.TargetSQLHostName)`""
    
    $sourceDb  = $RubrikData.SQLDBArray | Where-Object { ($_.HostName -eq $MountInfo.SourceSQLHostName) -and ($_.InstanceName -eq $MountInfo.SourceInstanceName) -and ($_.DatabaseName -eq $MountInfo.SourceDatabaseName) } | Select-Object -First 1
    $targetInstance = $RubrikData.SQLInstanceArray | Where-Object { ($_.HostName -eq $MountInfo.TargetSQLHostName) -and ($_.InstanceName -eq $MountInfo.TargetInstanceName) } | Select-Object -First 1

    if (-not $sourceDb) {
        Write-Warning "Source database not found in Rubrik: Host:`"$($MountInfo.SourceSQLHostName)`", Instance:`"$($MountInfo.SourceInstanceName)`", DB:`"$($MountInfo.SourceDatabaseName)`"."
        return
    }
    
    if (-not $targetInstance) {
        Write-Warning "Target instance not found in Rubrik: Host:`"$($MountInfo.TargetSQLHostName)`", Instance:`"$($MountInfo.TargetInstanceName)`"."
        return
    }

    Write-Host "Validation successful. IDs found."
    
    # Logic to get the latest snapshot/recovery point
    $recoveryPointDate = $null
    
    if ($UseLatestRecoveryPoint) {
        Write-Host "Using Get-RubrikMssqlDatabase to retrieve latest recovery point."
        try {
            # Call the new function to get the database object
            $dbDetails = Get-RubrikMssqlDatabase -MssqlDbId $sourceDb.DatabaseID -BaseUrl $BaseUrl -ApiParams $ApiParams
            
            if (-not $dbDetails.latestRecoveryPoint) {
                Write-Warning "No latest recovery point found for database $($MountInfo.SourceDatabaseName)."
                return
            }

            $recoveryPointDate = $dbDetails.latestRecoveryPoint
            Write-Host "Using latestRecoveryPoint: $($recoveryPointDate)"
            
        } catch {
            Write-Error "Failed to retrieve latest recovery point from database details. Error: $($_.Exception.Message)"
            return
        }

    } else {
        Write-Host "Using the default method to list snapshots to retrieve the latest recovery point."
        try {
            $snapshotUrl = "$BaseUrl/mssql/db/$($sourceDb.DatabaseID)/snapshot"
            $snapshotResponse = Invoke-RestMethod -Uri $snapshotUrl @ApiParams
            $latestSnapshot = $snapshotResponse.data | Sort-Object date -Descending | Select-Object -First 1
            
            if (-not $latestSnapshot) {
                Write-Warning "No snapshots found for source database $($MountInfo.SourceDatabaseName)."
                return
            }
            
            $recoveryPointDate = $latestSnapshot.date
            Write-Host "Using latest snapshot from: $($recoveryPointDate)"

        } catch {
            Write-Error "Failed to retrieve snapshots for $($MountInfo.SourceDatabaseName). Error: $($_.Exception.Message)"
            return
        }
    }

    # Prepare and send the live mount request
    try {
        $mountBody = @{
            recoveryPoint = @{ date = $recoveryPointDate }
            targetInstanceId = $targetInstance.InstanceID
            mountedDatabaseName = $MountInfo.TargetDatabaseName
        } | ConvertTo-Json -Depth 5

        $mountUrl = "$BaseUrl/mssql/db/$($sourceDb.DatabaseID)/mount"
        $mountParams = @{
            Method = "Post"
            Uri    = $mountUrl
            Body   = $mountBody
            ErrorAction = "Stop"
        }
        $mountParams = $mountParams + $ApiParams
        
        Write-Host "Submitting live mount request..."
        $mountResponse = Invoke-RestMethod @mountParams
        
        $jobStatusUrl = $mountResponse.links.href
        $jobStatus = "QUEUED"
        Write-Host "Live mount job started. Polling status..."

        # Poll the job status
        $maxAttempts = 300
        $attempt = 0
        do {
            $attempt++
            Start-Sleep -Seconds 15
            $statusResponse = Invoke-RestMethod -Uri $jobStatusUrl @ApiParams
            $jobStatus = $statusResponse.status
            Write-Host "Job status: $jobStatus (Attempt $attempt/$maxAttempts)"
        } until (($jobStatus -eq "SUCCEEDED") -or ($jobStatus -eq "FAILED") -or ($jobStatus -eq "CANCELED") -or ($attempt -ge $maxAttempts))

        if ($jobStatus -eq "SUCCEEDED") {
            Write-Host "Live mount successfully completed for $($MountInfo.TargetDatabaseName)."
        } else {
            Write-Error "Live mount job for $($MountInfo.TargetDatabaseName) failed with status: $jobStatus."
        }

    } catch {
        Write-Error "Live mount API call failed. Error: $($_.Exception.Message)"
    }
}

function Invoke-SQLLiveMounts {
    <#
    .SYNOPSIS
        Main function to orchestrate the entire live mount process.
    .DESCRIPTION
        This is the main entry point of the script. It calls all other functions
        in sequence to configure, authenticate, fetch data, and perform live mounts
        for all entries in the specified CSV file.
    .PARAMETER CsvFilePath
        The path to the CSV file containing the live mount requests. If not provided,
        it uses the path from `Configure-ScriptVariables`.
    #>
    param(
        [string]$CsvFilePath = ""
    )

    # 1. Configure variables and logging
    $config = Configure-ScriptVariables
    if (-not $config) { Write-Error "Configuration failed. Exiting."; return }
    if ($CsvFilePath) { $config.SQLLiveMountCSV = $CsvFilePath }
    
    if (-not (Test-Path $config.LogDirectory -PathType Container)) {
        New-Item -Path $config.LogDirectory -ItemType Directory | Out-Null
    }
    
    $logPath = "$($config.LogDirectory)\Rubrik-SQLLiveMountLog-$(Get-Date -Format 'yyyy-MM-dd@HH-mm-ss').log"
    Start-Transcript -Path $logPath -NoClobber
    
    # Handle PowerShell version and OS for certificate checks
    $sslSkipCheck = $false
if ($env:OS -eq 'Windows_NT') {
    # It's a Windows system. Now check the PowerShell version.
    if ($PSVersionTable.PSVersion.Major -ge 6) {
        Write-Host "Detected PowerShell Core version $($PSVersionTable.PSVersion) on Windows. Skipping certificate check."
        $sslSkipCheck = $true
    } else {
        # This is Windows PowerShell 5.1 or older
        Write-Host "Detected Windows PowerShell version $($PSVersionTable.PSVersion). Applying self-signed certificate policy."
        
        # Original logic to apply the certificate policy
        add-type @"
            using System.Net;
            using System.Security.Cryptography.X509Certificates;
            public class TrustAllCertsPolicy : ICertificatePolicy {
                public bool CheckValidationResult(ServicePoint srvPoint, X509Certificate certificate, WebRequest request, int certificateProblem) {
                    return $true;
                }
            }
"@
        [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    }
} else {
    # It's not Windows (e.g., macOS, Linux).
    Write-Host "Detected non-Windows OS (e.g., macOS, Linux). Skipping certificate check."
    $sslSkipCheck = $true
    }
    # 2. Authenticate
    $session = Invoke-RubrikAuthentication -RubrikCluster $config.RubrikCluster -ApiEndpoint $config.ApiEndpoint -JsonFilePath $config.JsonFilePath -SslSkipCheck:$sslSkipCheck
    if (-not $session) { Stop-Transcript; return }

    # 3. Get all SQL data from Rubrik
    $apiParams = $session.ApiParams
    $apiParams.Add("Headers", $session.Headers)
    $apiParams.Add("ContentType", "application/json")

    $baseUrl = "https://$($config.RubrikCluster)/api/v1"
    $internalUrl = "https://$($config.RubrikCluster)/api/internal"

    $apiData = Get-RubrikApiData -BaseUrl $baseUrl -InternalUrl $internalUrl -ApiParams $apiParams
    if (-not $apiData) { Stop-Transcript; return }

    # 4. Process API data for easier lookups
    $rubrikData = Process-SqlData -ApiData $apiData
    if (-not $rubrikData) { Stop-Transcript; return }

    # 5. Read CSV and perform live mounts
    if (-not (Test-Path $config.SQLLiveMountCSV)) {
        Write-Error "CSV file not found at $($config.SQLLiveMountCSV). Exiting."
        Stop-Transcript
        return
    }

    $liveMounts = Import-Csv $config.SQLLiveMountCSV
    if (-not $liveMounts) {
        Write-Warning "No data found in CSV file. Exiting."
        Stop-Transcript
        return
    }

    foreach ($mount in $liveMounts) {
        # Check for same source and target, and auto-rename if needed
        $targetDbName = $mount.TargetDatabaseName
        if (($mount.SourceSQLHostName -eq $mount.TargetSQLHostName) -and
            ($mount.SourceInstanceName -eq $mount.TargetInstanceName) -and
            ($mount.SourceDatabaseName -eq $mount.TargetDatabaseName)) {
            Write-Host "Source and target are identical. Appending '-live' to the target database name."
            $targetDbName = $mount.TargetDatabaseName + "-live"
        }
        $mount.TargetDatabaseName = $targetDbName
        New-SQLLiveMount -MountInfo $mount -RubrikData $rubrikData -BaseUrl $baseUrl -ApiParams $apiParams -UseLatestRecoveryPoint
    }

    Write-Host "--------------------------------------------"
    Write-Host "End of LiveMount Script"
    Stop-Transcript
}

Invoke-SQLLiveMounts
