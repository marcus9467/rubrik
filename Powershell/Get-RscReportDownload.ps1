<#

.SYNOPSIS
This script will initate a prepare file job so that a CSV can be downloaded of a specific report specified within the script. 

.EXAMPLE
./Get-RscReportDownload.ps1 

This will download the requested report to the specified location. Note that the service account json and output location are expected to be customized per environment and specified in the script below.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : September 19, 2023
    Company : Rubrik Inc
#>

[cmdletbinding()]
param (
)

################################################################################################################################################################################################
#USER INPUTS

#These must be input specific to your environment. 
$ServiceAccountJson = "/Users/User/Documents/serviceAccountJson.json"
$reportId = "18"
$Output_directory = (Get-Location).path
################################################################################################################################################################################################
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$mdate = (Get-Date).tostring("yyyyMMddHHmm")
function connect-polaris {

    # Function that uses the Polaris/RSC Service Account JSON and opens a new session, and returns the session temp token

    [CmdletBinding()]

    param (

        # Service account JSON file

    )

   

    begin {

        # Parse the JSON and build the connection string

        #$serviceAccountObj 

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

        }

    }

   

    end {

            if($polaris.access_token){

                Write-Output $polaris

            } else {

                Write-Error("Unable to connect")

            }

           

        }

}
function disconnect-polaris {

    # Closes the session with the session token passed here

    [CmdletBinding()]

    param (
    )

   

    begin {

 

    }

   

    process {

        try{

            $closeStatus = $(Invoke-WebRequest -Method Delete -Headers $headers -ContentType "application/json; charset=utf-8" -Uri $logoutUrl).StatusCode

        }

        catch [System.Management.Automation.ParameterBindingException]{

            Write-Error("Failed to logout. Error $($_)")

        }

    }

   

    end {

            if({$closeStatus -eq 204}){

                Write-Output("Successfully logged out")

            } else {

                Write-Error("Error $($_)")

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


#Kick off the file generation for the selected report. 
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
      __typename
    }
  }"
$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json

$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
(($info.Content | ConvertFrom-Json).data).downloadFile

#Check Download Status
$query = "query GetUserDownloadsQuery {
    getUserDownloads {
      id
      name
      status
      progress
      identifier
      createTime
      completeTime
    }
  }"
$variables = ""
$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json

$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$filePrepStatus = (((($info.Content | convertFrom-Json).data).getUserDownloads)[0]).status
while($filePrepStatus -ne "COMPLETED"){
    Write-Host "Awaiting file preperation process"
    (($info.Content | convertFrom-Json).data).getUserDownloads[0]
    sleep 10
    $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $filePrepStatus = (((($info.Content | convertFrom-Json).data).getUserDownloads)[0]).status
}
$downloadId = ((($info.Content | convertFrom-Json).data).getUserDownloads)[0].id

<#
Potential for later improvement in the section above. Right now the logic is to simply pull information regarding the latest download request for the specific user triggering the API call. This needs to be further enhanced to track the exact request later.
#>


#Generate the Download URL
$query = "mutation generateDownloadUrlMutation(`$downloadId: Long!) {
    getDownloadUrl(downloadId: `$downloadId) {
      url
      __typename
    }
  }"

$variables = "{
    `"downloadId`": $downloadId
}"
$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$downloadURL = ((($info.content | convertFrom-Json).data).getDownloadURL).url
$reportName = ($downloadURL.split("/")[4]).split("_")[0]
#Download the file:
Write-Host ("Downloading the requested report to " + $Output_directory + "/" + $reportName + "-" + $mdate + ".csv")
Invoke-WebRequest -Uri "$downloadURL" -OutFile ($Output_directory + "/DownloadedReport-" + $mdate + ".csv" )
disconnect-polaris
