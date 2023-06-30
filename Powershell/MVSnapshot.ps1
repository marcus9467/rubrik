<#

.SYNOPSIS
This script is meant to open and closing managed volumes using RSC the intention is to insert application code into the script to dump to the MV while it is open, or to split this into a pre/post process between the begin and end MV calls.
.EXAMPLE
./MVSnapshot.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -mvId "e27ca4f4-d765-538c-a30f-392eebdcf974" -slaId "b5594276-e5d5-4986-b0e9-009840d0a67e"

This will open and close the specified MV and save the resulting data to the specified SLA Domain.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : June 30, 2023
    Company : Rubrik Inc

#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$slaId,
    [parameter(Mandatory=$true)]
    [string]$mvId
)


function connect-RSC {

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
function disconnect-RSC {

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
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = connect-RSC
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")


$query = "mutation ManagedVolumeBeginSnapshotMutation(`$input: BeginManagedVolumeSnapshotInput!) {
    beginManagedVolumeSnapshot(input: `$input) {
      asyncRequestStatus {
        id
      }
    }
  }"
$variables = "{
    `"input`": {
      `"id`": `"${mvId}`",
      `"config`": {
        `"isAsync`": true
      }
    }
  }"

$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
#Open MV for Writes
$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$jobId = ((($info.content | convertFrom-Json).data).beginmanagedVolumesnapshot).asyncRequestStatus
Write-Host ("Opening Managed Volume with job Id " + $jobId)
<#

Application Code Here

#>






$query = "mutation ManagedVolumeOnDemandSnapshotMutation(`$input: EndManagedVolumeSnapshotInput!) {
    endManagedVolumeSnapshot(input: `$input) {
      asyncRequestStatus {
        id
      }
    }
  }"

$variables = "{
    `"input`": {
      `"id`": `"${mvId}`",
      `"params`": {
        `"isAsync`": true,
        `"retentionConfig`": {
          `"slaId`": `"${slaId}`"
        }
      }
    }
  }"

$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
#Close MV and set to RO
$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$jobId = ((($info.content | convertFrom-Json).data).endManagedVolumeSnapshot).asyncRequestStatus
Write-Host ("Closing Managed Volume with job Id " + $jobId)

disconnect-RSC
