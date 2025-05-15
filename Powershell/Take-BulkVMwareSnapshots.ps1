<#
.SYNOPSIS
    Script to perform bulk on-demand snapshots of VMware virtual machines via the Rubrik Polaris API.
.DESCRIPTION
    This script authenticates to Rubrik Polaris/RSC using a service account JSON file, performs bulk snapshots of specified VMs based on the provided SLA ID, and safely closes the session after operation periods.
.PARAMETER ServiceAccountJson
    Path to the Rubrik Polaris Service Account JSON file downloaded from the console.
.PARAMETER SlaId
    SLA Domain ID used for snapshot assignment.
.PARAMETER VMIds
    One or more VMware VM IDs to snapshot (Provide as comma-separated IDs).
.EXAMPLE
    ./snapshot-vms.ps1 -ServiceAccountJson "C:\auth.json" -SlaId "09425d87-40fa-4903-8a4a-3fc9677e044f" -VMIds "id1,id2,id3"
.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : May 15, 2025
    Company : Rubrik Inc
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [Parameter(Mandatory=$true)]
    [string]$SlaId,
    [Parameter(Mandatory=$true)]
    [string]$VMIds
)

# Convert VMIds string into an array
[string[]]$VMIdsArray = $VMIds.Split(",") | ForEach-Object { $_.Trim() }

# Import service account JSON file.
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json

# Polaris functions
function Connect-Polaris {
    [CmdletBinding()]
    param ()

    try {
        $connectionBody = @{
            client_id     = $serviceAccountObj.client_id
            client_secret = $serviceAccountObj.client_secret
        } | ConvertTo-Json

        $polaris = Invoke-RestMethod -Method POST -Uri $serviceAccountObj.access_token_uri `
            -ContentType "application/json" -Body $connectionBody

        if ($null -ne $polaris.access_token) {
            Write-Output $polaris
        } else {
            Throw "Failed to retrieve access token from Polaris."
        }
    }
    catch {
        Write-Error "Polaris connection error: $_"
        exit 1
    }
}

function Disconnect-Polaris {
    [CmdletBinding()]
    param ()

    try {
        $response = Invoke-WebRequest -Method DELETE -Headers $headers `
            -Uri $logoutUrl -ContentType "application/json; charset=utf-8"
        if ($response.StatusCode -eq 204) {
            Write-Output "Session closed successfully."
        } else {
            Write-Warning "Unexpected response status code: $($response.StatusCode)"
        }
    }
    catch {
        Write-Error "Error during Polaris logout: $_"
    }
}

function Take-BulkOnDemandSnapshot {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [string[]]$VMIds,
        
        [Parameter(Mandatory=$true)]
        [string]$slaId,
        
        [string]$userNote = ""
    )

    try {
        $query = @"
mutation TakeBulkOnDemandSnapshotMutation(`$input: VsphereBulkOnDemandSnapshotInput!) {
    vsphereBulkOnDemandSnapshot(input: `$input) {
        responses {
            links {
                href
            }
        }
    }
}
"@

        $variables = @{
            input = @{
                config = @{
                    slaId = $slaId
                    vms = $VMIds
                }
                userNote = $userNote
            }
        }

        $JSON_BODY = @{
            query = $query
            variables = $variables
        } | ConvertTo-Json -Depth 10

        $result = Invoke-RestMethod -Uri $Polaris_URL -Method POST `
            -Headers $headers -Body $JSON_BODY

        Write-Output $result
    }
    catch {
        Write-Error "Error performing Bulk Snapshot: $_"
        exit 1
    }
}

# Main execution flow
# Set URLs based on provided service account JSON.
$Polaris_URL = $serviceAccountObj.access_token_uri.Replace("client_token", "graphql")
$logoutUrl = $serviceAccountObj.access_token_uri.Replace("client_token", "session")

# Authenticate to Polaris
$polSession = Connect-Polaris
$rubTok = $polSession.access_token

# Prepare headers for requests
$headers = @{
    'Content-Type'  = 'application/json'
    'Accept'        = 'application/json'
    'Authorization' = "Bearer $rubTok"
}

# Execute snapshot function
Take-BulkOnDemandSnapshot -VMIds $VMIdsArray -slaId $SlaId -userNote "Bulk snapshot operation via API call"

# Terminate connection
Disconnect-Polaris
