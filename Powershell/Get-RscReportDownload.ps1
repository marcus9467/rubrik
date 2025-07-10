<#
.SYNOPSIS
This script will initate a prepare file job so that a CSV can be downloaded of a specific report specified within the script.

.EXAMPLE
./Get-RscReportDownload.ps1

This will download the requested report to the specified location. Note that the service account json and output location are expected to be customized per environment and specified in the script below.

.NOTES
    Author  : Marcus Henderson <marcus.henterson@rubrik.com>
    Created : September 19, 2023
    Updated : July 10, 2025
    Company : Rubrik Inc
#>

[cmdletbinding()]
param (
)

################################################################################################################################################################################################
#USER INPUTS
$ServiceAccountJson = "<path to service account JSON here>"
$reportId = "<report ID here>"
$Output_directory = (Get-Location).path
################################################################################################################################################################################################
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

# Extract the base RSC URL from the access_token_uri
# Example: https://<instance>.my.rubrik.com/api/client_token -> https://<instance>.my.rubrik.com
$RSC_Base_URL = ($serviceAccountObj.access_token_uri.Split("/api")[0])

function connect-polaris {
    [CmdletBinding()]
    param ()

    begin {
        $connectionData = [ordered]@{
            'client_id' = $serviceAccountObj.client_id
            'client_secret' = $serviceAccountObj.client_secret
        } | ConvertTo-Json
    }

    process {
        try{
            $polaris = Invoke-RestMethod -Method Post -uri $serviceAccountObj.access_token_uri -ContentType application/json -body $connectionData
        }
        catch [System.Management.Automation.ParameterBindingException]{
            Write-Error("The provided JSON has null or empty fields, try the command again with the correct file or redownload the service account JSON from Polaris")
            exit 1
        }
    }

    end {
        if($polaris.access_token){
            Write-Output $polaris
        } else {
            Write-Error("Unable to connect")
            exit 1
        }
    }
}

function disconnect-polaris {
    [CmdletBinding()]
    param ()

    begin {}

    process {
        try{
            $closeStatus = $(Invoke-WebRequest -Method Delete -Headers $headers -ContentType "application/json; charset=utf-8" -Uri $logoutUrl).StatusCode
        }
        catch [System.Management.Automation.ParameterBindingException]{
            Write-Error("Failed to logout. Error $($_)")
        }
    }

    end {
        if($closeStatus -eq 204){
            Write-Output("Successfully logged out")
        } else {
            Write-Error("Error during logout: Status code $closeStatus")
        }
    }
}

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")


# Kick off the file generation for the selected report.
$variables = "{
    `"input`": {
      `"fileType`": `"CSV`",
      `"reportId`": $reportId,
      `"timezone`": `"America/New_York`"
    }
  }"
$query = "mutation ngReportDownloadFileMutation(`$input: DownloadReportFileInput!) {
    downloadFile(input: `$input) {
      jobId
      referenceId
      externalId
      __typename
    }
  }"
$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json

try {
    $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $responseContentInitial = $info.Content | ConvertFrom-Json

    $initiatedExternalId = $responseContentInitial.data.downloadFile.externalId
    $initiatedJobId = $responseContentInitial.data.downloadFile.jobId

    if (-not $initiatedExternalId) {
        Write-Error "Failed to initiate report download or get an externalId. Check initial response."
        $responseContentInitial | ConvertTo-Json | Write-Error
        disconnect-polaris
        exit 1
    }
    Write-Host "Initiated download job with internal jobId: $initiatedJobId and externalId: $initiatedExternalId"
}
catch {
    Write-Error "Error initiating download request: $($_.Exception.Message)"
    disconnect-polaris
    exit 1
}


# Check Download Status by polling allUserFiles.downloads
$filePrepStatus = ""
$downloadFilename = "" # To store the actual filename from allUserFiles.downloads

Write-Host "Awaiting file preparation process for externalId: $initiatedExternalId"

$timeoutSeconds = 600
$startTime = Get-Date

while ($filePrepStatus -ne "READY") {
    if (((Get-Date) - $startTime).TotalSeconds -gt $timeoutSeconds) {
        Write-Error "Timeout: Report preparation did not complete within $($timeoutSeconds) seconds for externalId: $initiatedExternalId."
        disconnect-polaris
        exit 1
    }

    # Only need allUserFiles for status and filename now
    $query = "query DownloadBarQuery {
        allUserFiles {
            downloads {
                externalId
                createdAt
                expiresAt
                completedAt
                creator
                filename
                type
                state
                __typename
            }
            __typename
        }
    }"

    $JSON_BODY = @{
        "operationName" = "DownloadBarQuery"
        "variables" = @{}
        "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json

    try {
        $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $responseContent = $info.Content | ConvertFrom-Json
    }
    catch {
        Write-Warning "Error checking download status: $($_.Exception.Message). Retrying..."
        Start-Sleep -Seconds 10
        continue
    }

    $foundDownloadInAllUserFiles = $null
    if ($responseContent.data.allUserFiles.downloads) {
        $foundDownloadInAllUserFiles = $responseContent.data.allUserFiles.downloads | Where-Object {
            $_.externalId -eq $initiatedExternalId -and $_.type -eq "REPORT"
        } | Select-Object -First 1
    }

    if ($foundDownloadInAllUserFiles) {
        $filePrepStatus = $foundDownloadInAllUserFiles.state
        $downloadFilename = $foundDownloadInAllUserFiles.filename
        Write-Host "Found download with externalId '$initiatedExternalId'. Current state: $($filePrepStatus)."

        if ($filePrepStatus -eq "READY") {
            Write-Host "File is READY. Proceeding to download."
            break # Exit the loop as file is ready
        }
    } else {
        Write-Host "Still waiting for download with externalId '$initiatedExternalId' to appear in 'allUserFiles.downloads' list or change state. Retrying..."
    }

    if ($filePrepStatus -ne "READY") {
        Start-Sleep -Seconds 10
    }
}

Write-Host "File preparation completed for externalId: $initiatedExternalId."

$downloadURL = "$RSC_Base_URL/file-downloads/$initiatedExternalId"
Write-Host "Constructed download URL: $downloadURL"


# Derive the report name from the retrieved filename or use a generic name
if ($downloadFilename) {
    # Assuming format like "ReportName_YYYYMMDDHHMMSS_CSV"
    $reportName = ($downloadFilename -split '_')[0]
} else {
    Write-Warning "Could not retrieve specific filename from API. Using generic report name."
    $reportName = "RubrikReport"
}

# Download the file
$outputFilePath = Join-Path -Path $Output_directory -ChildPath ("$reportName" + "-" + "$mdate" + ".csv")
Write-Host ("Downloading the requested report to " + $outputFilePath)

try {
    Invoke-WebRequest -Uri "$downloadURL" -OutFile $outputFilePath -Headers $headers
    Write-Host "Report successfully downloaded to $outputFilePath"
}
catch {
    Write-Error "Error downloading the file: $($_.Exception.Message). Check network and file path permissions."
}

disconnect-polaris
