<#
.SYNOPSIS
This script takes an on-demand, ad-hoc snapshot for a list of GCE instances
using a specified Retention SLA in Rubrik Security Cloud (RSC).

.EXAMPLE
./Invoke-RSCGCEBulkSnapshot.ps1 -ServiceAccountJson $serviceAccountJson -GceInstanceIds "id-1", "id-2" -RetentionSlaId "sla-id-123"

This will trigger an on-demand snapshot for the two specified GCE instances.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : November 05, 2025
    Company : Rubrik Inc
#>


[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string[]]$GceInstanceIds,
    [parameter(Mandatory=$true)]
    [string]$RetentionSlaId
)
function connect-rsc {
    <#
    .SYNOPSIS
    Function that uses the RSC Service Account JSON and opens a new session, and returns the session temp token
    #>
    [CmdletBinding()]
    param ()
   
    begin {
        # Parse the JSON and build the connection string
        $connectionData = [ordered]@{
            'client_id' = $serviceAccountObj.client_id
            'client_secret' = $serviceAccountObj.client_secret
        } | ConvertTo-Json
    }
   
    process {
        try{
            $rscSession = Invoke-RestMethod -Method Post -uri $serviceAccountObj.access_token_uri -ContentType application/json -body $connectionData
        }
        catch [System.Management.Automation.ParameterBindingException]{
            Write-Error("The provided JSON has null or empty fields, try the command again with the correct file or redownload the service account JSON from RSC")
        }
    }
   
    end {
            if($rscSession.access_token){
                Write-Output $rscSession
            } else {
                Write-Error("Unable to connect to RSC")
            }
           
        }
}
function disconnect-rsc {
    <#
    .SYNOPSIS
    Closes the RSC session with the session token passed here
    #>
    [CmdletBinding()]
    param ()
   
    begin {
 
    }
   
    process {
        try{
            # $headers and $logoutUrl are assumed to be in the parent script scope
            $closeStatus = $(Invoke-WebRequest -Method Delete -Headers $headers -ContentType "application/json; charset=utf-8" -Uri $logoutUrl).StatusCode
        }
        catch [System.Management.Automation.ParameterBindingException]{
            Write-Error("Failed to logout from RSC. Error $($_)")
        }
    }
   
    end {
            if({$closeStatus -eq 204}){
                Write-Output("Successfully logged out from RSC")
            } else {
                Write-Error("Error $($_)")
            }
        }
}

function Invoke-GCEBulkSnapshot {
    <#
    .SYNOPSIS
    Takes an on-demand snapshot for one or more GCE Instance IDs using a specific retention SLA.
    This function is based on the takeOnDemandSnapshotSync mutation.
    .EXAMPLE
    $instanceIds = "389f05b8-6c97-4c61-894c-ed9861233234", "9fecbacd-e7d4-46d9-9ce8-347cfadebd97"
    $slaId = "43dd3f3a-fc50-4b87-a1a8-4a0a6ce2bf11"
    Invoke-GCEBulkSnapshot -retentionSlaId $slaId -GceInstanceIds $instanceIds
    .NOTES
    This function assumes that the $RSC_URL and $headers variables (containing the auth token)
    are available in the script's scope, as defined by the main script body after connect-rsc.
    #>
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$retentionSlaId,
        [parameter(Mandatory=$true)]
        [string[]]$GceInstanceIds
    )
    
    process {
        try {
            $query = "mutation TakeOnDemandSnapshotSyncMutation(`$retentionSlaId: UUID!, `$snappableIds: [UUID!]!) {
                takeOnDemandSnapshotSync(
                    input: {slaId: `$retentionSlaId, workloadIds: `$snappableIds}
                ) {
                    workloadDetails {
                        workloadId
                        taskchainUuid
                        snapshotCreationTimestamp
                        error
                        __typename
                    }
                    __typename
                }
            }"

            $gceInstanceIdsJson = $GceInstanceIds | ConvertTo-Json -Compress
            $variables = "{
                `"retentionSlaId`": `"${retentionSlaId}`",
                `"snappableIds`": ${gceInstanceIdsJson}
              }"
            $JSON_BODY = @{
                "variables" = $variables
                "query"     = $query
            } | ConvertTo-Json -Depth 5

            Write-Verbose "Sending GraphQL Request Body: $JSON_BODY"
            $result = Invoke-WebRequest -Uri $RSC_URL -Method POST -Headers $headers -Body $JSON_BODY -ContentType "application/json"
            
            $response = $result.Content | ConvertFrom-Json
            
            if ($response.data.takeOnDemandSnapshotSync) {
                Write-Output $response.data.takeOnDemandSnapshotSync
            }
            elseif ($response.errors) {
                Write-Error ("GraphQL API returned an error: " + ($response.errors | ConvertTo-Json -Depth 5))
            }
            else {
                Write-Error ("Unknown error. Full response: " + $result.Content)
            }
        }
        catch {
            Write-Error("Error in Invoke-GCEBulkSnapshot: $_")
            
            if ($_.Exception -is [System.Net.WebException]) {
                try {
                    $stream = $_.Exception.Response.GetResponseStream()
                    $reader = New-Object System.IO.StreamReader($stream)
                    $responseBody = $reader.ReadToEnd()
                    $reader.Close()
                    $stream.Close()
                    Write-Error "API Response Body on Error: $responseBody"
                } catch {
                    Write-Error "Could not read error response body."
                }
            }
        }
    }
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json

Write-Host "Connecting to Rubrik Security Cloud (RSC)..."
$rscSession = connect-rsc
$rubtok = $rscSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$RSC_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

Write-Host "Successfully connected to RSC."
Write-Host "Taking on-demand snapshot for $($GceInstanceIds.Count) GCE instances against SLA ID $RetentionSlaId..."

$snapshotResult = Invoke-GCEBulkSnapshot -retentionSlaId $RetentionSlaId -GceInstanceIds $GceInstanceIds

if ($snapshotResult) {
    Write-Host "Snapshot request complete. Results:"
    Write-Output $snapshotResult.workloadDetails | Format-Table
} else {
    Write-Error "Snapshot operation failed or returned no result."
}

Write-Host "Disconnecting from RSC..."
disconnect-rsc
