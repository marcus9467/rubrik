############################################################################################
# Start of the script - Description, Requirements & Legal Disclaimer
############################################################################################
# Written by: Joshua Stenhouse joshuastenhouse@gmail.com
# Updated by: Marcus Henderson - 2025/08/06
##################################
# Change Log:
# Updated the authentication logic to use RSC service accounts
# Updated the certificate handling to be more inclusive to newer versions of Powershell and alternative OSes
################################## 
# Description:
# This script creates multiple SQL live mounts from the CSV specified
################################## 
# Requirements:
# - Run the RubrikSQLLiveMountsv2-Auth.ps1 to secure your Rubrik credentials, this script won't work without this being run first
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
# Configure the variables below for the Rubrik Cluster
##################################
$RubrikCluster = "10.8.49.104"
$apiEndpoint = "/api/v1/service_account/session"
$ScriptDirectory = ""
# The below are loaded from the above directory
$SQLLiveMountCSV = ""
$LogDirectory =  ""
$jsonFilePath = ""


# Read the JSON file
$credentials = Get-Content -Path $jsonFilePath | ConvertFrom-Json

# Create the body for the API request
$body = @{
    serviceAccountId = $credentials.client_id
    secret = $credentials.client_secret
} | ConvertTo-Json
############################################################################################
# Nothing to configure below this line - Starting the main function of the script
############################################################################################

##################################
# Conditional OS and PowerShell version check for certificate handling
##################################
$apiParams = @{}

if ($IsWindows) {
    if ($PSVersionTable.PSVersion.Major -ge 6) {
        Write-Host "Detected PowerShell version $($PSVersionTable.PSVersion). Using -SkipCertificateCheck parameter."
        $apiParams.Add("SkipCertificateCheck", $true)
    } else {
        Write-Host "Detected Windows PowerShell version $($PSVersionTable.PSVersion). Applying self-signed certificate policy."
        add-type @"
            using System.Net;
            using System.Security.Cryptography.X509Certificates;
            public class TrustAllCertsPolicy : ICertificatePolicy {
                public bool CheckValidationResult(
                    ServicePoint srvPoint, X509Certificate certificate,
                    WebRequest request, int certificateProblem) {
                    return $true; 
                }
            }
"@
        [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    }
} else {
    Write-Host "Detected non-Windows OS (e.g., macOS, Linux). Using -SkipCertificateCheck parameter."
    $apiParams.Add("SkipCertificateCheck", $true)
}

##################################
# Starting logging & importing the CSV
##################################
$Now = get-date
$Log = $LogDirectory + "\Rubrik-SQLLiveMountLog-" + $Now.ToString("yyyy-MM-dd") + "@" + $Now.ToString("HH-mm-ss") + ".log"
Start-Transcript -Path $Log -NoClobber 
# Importing the CSV of DBs to live mount
$SQLiveMounts = Import-csv $SQLLiveMountCSV
##################################
# Building Rubrik API string & invoking REST API
##################################
$BaseURL = "https://" + $RubrikCluster + "/api/v1/"
$InternalURL = "https://" + $RubrikCluster + "/api/internal/"
$Type = "application/json"

# Authenticating with API
Try 
{
    $authParams = @{
        Uri = ("https://" + "$rubrikCluster" + "$apiEndpoint")
        Method = "Post"
        Body = $body
        ContentType = "application/json"
        ErrorAction = "Stop"
    }
    $authParams = $authParams + $apiParams
    $RubrikSessionResponse = Invoke-RestMethod @authParams
}
Catch 
{
    $_ | Format-List -Force
    Exit
}

$token = $RubrikSessionResponse.token
$RubrikSessionHeader = @{'Authorization' = "Bearer $($token)"}

# Note: We must also add the headers and content type to apiParams for subsequent calls
$apiParams.Add("Headers", $RubrikSessionHeader)
$apiParams.Add("ContentType", $Type)

###############################################
# Getting list of SQL Databases
###############################################
$SQLDBListURL = $baseURL+"mssql/db?limit=5000&is_relic=false"
Try 
{
    $dbListParams = @{
        Uri = $SQLDBListURL
        TimeoutSec = 100
        ErrorAction = "Stop"
    }
    $dbListParams = $dbListParams + $apiParams
    $SQLDBListJSON = Invoke-RestMethod @dbListParams
    $SQLDBList = $SQLDBListJSON.data
}
Catch 
{
    Write-Host $_.Exception.ToString()
    $_ | Format-List -Force
}

###############################################
# Getting list of SQL Instances
###############################################
$SQLInstanceListURL = $baseURL+"mssql/instance?limit=5000"
Try 
{
    $instanceListParams = @{
        Uri = $SQLInstanceListURL
        TimeoutSec = 100
        ErrorAction = "Stop"
    }
    $instanceListParams = $instanceListParams + $apiParams
    $SQLInstanceJSON = Invoke-RestMethod @instanceListParams
    $SQLInstanceList = $SQLInstanceJSON.data
}
Catch 
{
    Write-Host $_.Exception.ToString()
    $_ | Format-List -Force
}

###############################################
# Getting list of SQL Availability groups
###############################################
$SQLAvailabilityGroupListURL = $InternalURL+"mssql/availability_group"
Try 
{
    $agListParams = @{
        Uri = $SQLAvailabilityGroupListURL
        TimeoutSec = 100
        ErrorAction = "Stop"
    }
    $agListParams = $agListParams + $apiParams
    $SQLAvailabilityGroupListJSON = Invoke-RestMethod @agListParams
    $SQLAvailabilityGroupList = $SQLAvailabilityGroupListJSON.data
}
Catch 
{
    Write-Host $_.Exception.ToString()
    $_ | Format-List -Force
}

###############################################
# Building a list of SQL instances by hostname, needed to enable selection of the correct instance
###############################################
$SQLInstanceArray = @()
ForEach ($SQLInstance in $SQLInstanceList)
{
    $SQLInstanceName = $SQLInstance.name
    $SQLInstanceID = $SQLInstance.id
    $SQLInstanceHostName = $SQLInstance.rootProperties.rootName
    $SQLInstanceHostID = $SQLInstance.rootProperties.rootId
    $SQLInstanceDBs = $SQLDBList | Where-Object {$_.instanceID -eq $SQLInstanceID} | Select -ExpandProperty Name
    $SQLInstanceDBCount = $SQLInstanceDBs.Count
    $SQLInstanceArrayLine = new-object PSObject
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "InstanceName" -Value "$SQLInstanceName"
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "InstanceID" -Value "$SQLInstanceID"
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "HostName" -Value "$SQLInstanceHostName"
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "HostNameID" -Value "$SQLInstanceHostID"
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "DBCount" -Value "$SQLInstanceDBCount"
    $SQLInstanceArrayLine | Add-Member -MemberType NoteProperty -Name "DBs" -Value "$SQLInstanceDBs"
    $SQLInstanceArray += $SQLInstanceArrayLine
}

###############################################
# Building a list of SQL DBS with hostname
###############################################
$SQLDBArray = @()
ForEach ($SQLDB in $SQLDBList)
{
    $SQLDBName = $SQLDB.name
    $SQLDBID = $SQLDB.id
    $SQLDBInstanceName = $SQLDB.instanceName
    $SQLDBInstanceID = $SQLDB.instanceId
    $SQLDBInAvailabilityGroup = $SQLDB.isInAvailabilityGroup
    IF ($SQLDBInAvailabilityGroup -eq "True")
    {
        $SQLDBHostName = "AvailabilityGroup"
        $SQLAvailabilityGroupID = $SQLDB.availabilityGroupId
        $SQLDBInstanceName = $SQLAvailabilityGroupList | Where-Object {$_.id -eq $SQLAvailabilityGroupID} | Select -ExpandProperty name
    }
    ELSE
    {
        $SQLDBHostName = $SQLInstanceArray | Where-Object {$_.InstanceID -eq $SQLDBInstanceID} | Select -ExpandProperty Hostname
    }
    $SQLDBArrayLine = new-object PSObject
    $SQLDBArrayLine | Add-Member -MemberType NoteProperty -Name "HostName" -Value "$SQLDBHostName"
    $SQLDBArrayLine | Add-Member -MemberType NoteProperty -Name "InstanceName" -Value "$SQLDBInstanceName"
    $SQLDBArrayLine | Add-Member -MemberType NoteProperty -Name "InstanceID" -Value "$SQLDBInstanceID"
    $SQLDBArrayLine | Add-Member -MemberType NoteProperty -Name "DatabaseName" -Value "$SQLDBName"
    $SQLDBArrayLine | Add-Member -MemberType NoteProperty -Name "DatabaseID" -Value "$SQLDBID"
    $SQLDBArray += $SQLDBArrayLine
}

###############################################
# Start For Each SQL DB below
###############################################
ForEach ($SQLiveMount in $SQLiveMounts)
{
    $SourceSQLHostName = $SQLiveMount.SourceSQLHostName
    $SourceInstanceName = $SQLiveMount.SourceInstanceName
    $SourceDatabaseName = $SQLiveMount.SourceDatabaseName
    $TargetSQLHostName = $SQLiveMount.TargetSQLHostName
    $TargetInstanceName = $SQLiveMount.TargetInstanceName
    $TargetDatabaseName = $SQLiveMount.TargetDatabaseName
    $SourceDatabaseID = $SQLiveMount.SourceDatabaseID
    
    IF (($SourceSQLHostName -eq $TargetSQLHostName) -AND ($SourceInstanceName -eq $TargetInstanceName) -AND ($SourceDatabaseName -eq $TargetDatabaseName))
    {
        $TargetDatabaseName = $TargetDatabaseName + " 1"
    }

    "--------------------------------------------"
    "Performing Live Mount For:
    DB:$SourceDatabaseName Instance:$SourceInstanceName Host:$SourceSQLHostName
    To
    DB:$TargetDatabaseName Instance:$TargetInstanceName Host:$TargetSQLHostName"

    IF($SourceDatabaseID -eq $null)
    {
        Write-Host "running checks"
        $SourceSQLHostNameCheck = $SQLDBArray | Where-Object {$_.HostName -contains $SourceSQLHostName}
        $SourceInstanceNameCheck = $SQLDBArray | Where-Object {(($_.HostName -eq $SourceSQLHostName) -AND ($_.InstanceName -eq $SourceInstanceName))}
        $SourceDatabaseNameCheck = $SQLDBArray | Where-Object {(($_.HostName -eq $SourceSQLHostName) -AND ($_.InstanceName -eq $SourceInstanceName) -AND ($_.DatabaseName -eq $SourceDatabaseName))}
        $TargetSQLHostNameCheck = $SQLInstanceArray | Where-Object {$_.HostName -eq $TargetSQLHostName}
        $TargetInstanceNameCheck = $SQLInstanceArray | Where-Object {(($_.HostName -eq $TargetSQLHostName) -AND ($_.InstanceName -eq $TargetInstanceName))}
        
        "SourceSQLHostNameCheck:"
        IF ($SourceSQLHostNameCheck) {"PASS"} ELSE {"FAIL - SQLHostName Not Found"} 
        "SourceInstanceNameCheck:"
        IF ($SourceInstanceNameCheck) {"PASS"} ELSE {"FAIL - InstanceName Not Found"} 
        "SourceDatabaseNameCheck:"
        IF ($SourceDatabaseNameCheck) {"PASS"} ELSE {"FAIL - DatabaseName Not Found"} 
        "TargetSQLHostNameCheck:"
        IF ($TargetSQLHostNameCheck) {"PASS"} ELSE {"FAIL - SQLHostName Not Found"} 
        "TargetInstanceNameCheck:"
        IF ($TargetInstanceNameCheck) {"PASS"} ELSE {"FAIL - InstanceName Not Found"} 
    }
    ELSE
    {
        Write-Host "bypassing checks"
        $SourceSQLHostNameCheck = "PASS"
        $SourceInstanceNameCheck = "PASS"
        $SourceDatabaseNameCheck = "PASS"
        $TargetSQLHostNameCheck = "PASS"
        $TargetInstanceNameCheck = "PASS"
    }

    IF (($SourceSQLHostNameCheck) -AND ($SourceInstanceNameCheck) -AND ($SourceDatabaseNameCheck) -AND ($TargetSQLHostNameCheck) -AND ($TargetInstanceNameCheck))
    {
        $SourceInstanceID = $SQLDBArray | Where-Object {(($_.HostName -eq $SourceSQLHostName) -AND ($_.InstanceName -eq $SourceInstanceName))} | Select -ExpandProperty InstanceID -First 1
        "SourceInstanceID:$SourceInstanceID"
        IF($SourceDatabaseID -eq $null)
        {
            $SourceDatabaseID = $SQLDBArray | Where-Object {(($_.InstanceID -eq $SourceInstanceID) -AND ($_.DatabaseName -eq $SourceDatabaseName))} | Select -ExpandProperty DatabaseID -First 1
        }
        IF ($SourceDatabaseID  -eq $null)
        {
            $SourceDatabaseID = $SQLDBList | Where-Object {(($_.InstanceName -eq $SourceInstanceID) -AND ($_.DatabaseName -eq $SourceDatabaseName))} | Select -ExpandProperty DatabaseID -First 1
            "SourceDatabaseID:$SourceDatabaseID"
        }
        ELSE
        {
            "SourceDatabaseID:$SourceDatabaseID"
        }
        $TargetInstanceID = $SQLInstanceArray | Where-Object {(($_.HostName -eq $TargetSQLHostName) -AND ($_.InstanceName -match $TargetInstanceName))} | Select -ExpandProperty InstanceID -First 1
        "TargetInstanceID:$TargetInstanceID"
        
        $SQLDBSnapshotURL = $baseURL+"mssql/db/"+$SourceDatabaseID+"/snapshot"
        Try 
        {
            $snapshotParams = @{
                Uri = $SQLDBSnapshotURL
                TimeoutSec = 100
                ErrorAction = "Stop"
            }
            $snapshotParams = $snapshotParams + $apiParams
            $SQLDBSnapshotJSON = Invoke-RestMethod @snapshotParams
            $SQLDBSnapshots = $SQLDBSnapshotJSON.data
        }
        Catch 
        {
            Write-Host $_.Exception.ToString()
            $_ | Format-List -Force
        }
        
        $SQLDBSnapshotDate = $SQLDBSnapshots | Sort-Object Date -Descending | Select -ExpandProperty date -First 1
        "Using Snapshot:$SQLDBSnapshotDate"
        
        $SQLDBSnapshotDateAsDateTime = [DateTime]::Parse($SQLDBSnapshotDate)
        $epoch = (Get-Date "1970-01-01 00:00:00Z")
        $SQLDBSnapshotTimeStampMS = [math]::Floor(($SQLDBSnapshotDateAsDateTime - $epoch).TotalMilliseconds)
        
        "SQLDBSnapshotTimeStampMS:$SQLDBSnapshotTimeStampMS"
        
        $SQLDBLiveMountURL = $baseURL+"mssql/db/"+$SourceDatabaseID+"/mount"
        $bodyObject = @{
            recoveryPoint = @{
                timestampMs = $SQLDBSnapshotTimeStampMS
            }
            targetInstanceId = $TargetInstanceID
            mountedDatabaseName = $TargetDatabaseName
        }
        $SQLDBLiveMountJSON = $bodyObject | ConvertTo-Json -Depth 5
        
        Try 
        {
            $mountParams = @{
                Method = "Post"
                Uri = $SQLDBLiveMountURL
                Body = $SQLDBLiveMountJSON
                ErrorAction = "Stop"
            }
            $mountParams = $mountParams + $apiParams
            $SQLDBLiveMountPOST = Invoke-RestMethod @mountParams
            $SQLDBLiveMountSuccess = $TRUE
        }
        Catch 
        {
            $SQLDBLiveMountSuccess = $FALSE
            Write-Host $_.Exception.ToString()
            $_ | Format-List -Force
        }
        "SQLDBLiveMountSuccess:$SQLDBLiveMountSuccess"
        
        $SQLJobStatusURL = $SQLDBLiveMountPOST.links.href
        
        $SQLJobStatusCount = 0
        DO
        {
            $statusParams = @{
                Uri = $SQLJobStatusURL
                TimeoutSec = 100
                ErrorAction = "Stop"
            }
            $statusParams = $statusParams + $apiParams
            $SQLJobStatusCount ++
            Try 
            {
                $SQLJobStatusResponse = Invoke-RestMethod @statusParams
                $SQLJobStatus = $SQLJobStatusResponse.status
            }
            Catch 
            {
                $ErrorMessage = $_.ErrorDetails; "ERROR: $ErrorMessage"
                $SQLJobStatus = "FAILED"
            }
            "SQLJobStatus: $SQLJobStatus"
            IF ($SQLJobStatus -ne "SUCCEEDED")
            {
                sleep 1
            }
        } Until (($SQLJobStatus -eq "SUCCEEDED") -OR ($SQLJobStatus -eq "FAILED") -OR ($SQLJobStatus -eq "CANCELED") -OR  ($SQLJobStatusCount -eq 300))
    }
    ELSE
    {
        "Skipping Live Mount for DB:$SourceDatabaseName Instance:$SourceInstanceName Host:$SourceSQLHostName
        Misconfigured, one or more elements were not found in Rubrik to perform the operation."
    }
}

sleep 30
"--------------------------------------------"
"End of LiveMount Script"
Stop-Transcript
