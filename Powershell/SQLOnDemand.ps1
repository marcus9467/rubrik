<#

.SYNOPSIS
This script will trigger an ondemand backup right after the dataloader process is initiated as part of a pre-script process.

.EXAMPLE
./SQLOnDemand.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -DatabaseId1 "9a818023-9f9b-5a91-9228-2ccf18e85926" -DatabaseId2 "210289af-5158-55e9-b23f-4de7727c8995" -SlaId "b5336bdc-6c9b-4784-a848-5d96992ace33"

This will initate an ondemand backup after the dataloader process. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : November 13, 2023
    Company : Rubrik Inc
#>


[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$SlaId,
    [parameter(Mandatory=$true)]
    [string]$DatabaseId1,
    [parameter(Mandatory=$true)]
    [string]$DatabaseId2,
    [parameter(Mandatory=$false)]
    [switch]$logBackup
)

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json

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
function Start-MSSQLBackup{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$databaseId,
        [parameter(Mandatory=$true)]
        [string]$SLAId

    )
    process{
        try{
            $query = "mutation MssqlTakeOnDemandSnapshotMutation(`$input: CreateOnDemandMssqlBackupInput!) {
                createOnDemandMssqlBackup(input: `$input) {
                  links {
                    href
                    __typename
                  }
                  __typename
                }
            }"
            $variables = "{
                `"input`": {
                  `"config`": {
                    `"baseOnDemandSnapshotConfig`": {
                      `"slaId`": `"${SLAId}`"
                    }
                  },
                  `"id`": `"${databaseId}`",
                  `"userNote`": `"`"
                }
            }"
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        }
        catch{
            Write-Error("Error $($_)")
        }
    }
    end{
        Write-Host ("Backup Successfully initiated. Please see " + (((($result.Content | convertFrom-Json).data).createOnDemandMssqlBackup).links).href + " for progress information.")
    }
}
function TakeMssqlLogBackup{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string[]]$snappableId
    )
    process{
      try{
        $query = "mutation TakeMssqlLogBackupMutation(`$input: TakeMssqlLogBackupInput!) {takeMssqlLogBackup(input: `$input) {id}}"
        $variables = @{
          input = @{
              id = $snappableId
          }
        }
        $JSON_BODY = @{
          "variables" = $variables
          "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $APIResult = (($result.content | ConvertFrom-Json).data).takeMssqlLogBackup
        Write-Host ("Starting MSSQL Log Job " + $APIResult)
      }
      catch{
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

if($logBackup){
    TakeMssqlLogBackup -snappableId $DatabaseId1, $DatabaseId2
    disconnect-polaris
    Exit 0
}
<#
Database Unload Process Here

#>


Start-MSSQLBackup -databaseId $databaseId1 -SLAId $SLAId
Start-MSSQLBackup -databaseId $databaseId2 -SLAId $SLAId


disconnect-polaris
