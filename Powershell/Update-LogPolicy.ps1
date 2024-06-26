<#
.SYNOPSIS
The purpose of this script is to update the log policy on existing databases to inherit the policy defined within the assigned SLA. 

.EXAMPLE
./Update-LogPolicy.ps1 -ServiceAccountJson $serviceAccountJson -CSV DatabaseList.csv

This will update the log policy to reflect what is defined in the SLA for any databases identified in the supplied CSV. The script expects the headers "databaseId" and "clusterUuid" within the CSV file. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : April 24, 2024
    Company : Rubrik Inc

#>

[CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$ServiceAccountJson,
        [parameter(Mandatory=$true)]
        [string]$CSV
    )

function connect-rsc {

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
function disconnect-rsc {

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

function Update-LogPolicy{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$batchedDatabases
    )
    try{
        $variables = $batchedDatabases
        $query = "mutation BulkUpdateMssqlDbsMutation(`$input: BulkUpdateMssqlDbsInput!) {
            bulkUpdateMssqlDbs(input: `$input) {
              items {
                mssqlDbSummary {
                  id
                  logBackupFrequencyInSeconds
                  logBackupRetentionHours
                  __typename
                }
                __typename
              }
              __typename
            }
          }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $result = (((($result.Content |ConvertFrom-Json).data).bulkUpdateMssqlDbs).items).mssqlDbSummary
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $result
    }
        
}
function Group-Records {
    param(
        [Parameter(Mandatory = $true)]
        [psobject[]]$Records,

        [int]$BatchSize = 100
    )

    $groupedRecords = @{}
    # Group records by clusterUuid
    foreach ($record in $Records) {
        $uuid = $record.clusterUuid
        if (-not $groupedRecords.ContainsKey($uuid)) {
            $groupedRecords[$uuid] = [System.Collections.ArrayList]@()
        }
        $groupedRecords[$uuid].Add($record)
    }

    $batches = @()
    # Batch groups into chunks of BatchSize
    foreach ($group in $groupedRecords.Values) {
        $index = 0
        while ($index -lt $group.Count) {
            $endIndex = [math]::Min($index + $BatchSize, $group.Count) - 1
            $batch = $group[$index..$endIndex]
            # Debug output
            #Write-Host "About to add batch: $batch"
            $batches += ,@($batch)
            $index += $BatchSize
        }
    }
    return $batches
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = connect-rsc
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$records = Import-Csv -Path $CSV 

$batches = Group-Records -Records $records
#remove any unwanted indexes from the batches
$batches = $batches | where-Object {$_.getType().fullName -ne "System.Int32"}

$graphQLData = foreach ($batch in $batches) {
    $dbsUpdateProperties = foreach ($record in $batch) {
        @{
            databaseId = $record.databaseId
            updateProperties = @{
                mssqlSlaRelatedProperties = @{
                    hasLogConfigFromSla = $true
                }
            }
        }
    }

    @{
        input = @{
            clusterUuid = $batch[0].clusterUuid  
            dbsUpdateProperties = $dbsUpdateProperties
        }
    } | ConvertTo-Json -Depth 5
}

ForEach($grouping in $graphQLData){
    Update-LogPolicy -batchedDatabases $grouping
}

Disconnect-Rsc
