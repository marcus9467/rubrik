<#

.SYNOPSIS
This script will extract encryption status and information for each Rubrik cluster attached to Rubrik Security Cloud. It assumes that the service account json and output directory path are specified within the script correctly.

.EXAMPLE
./Get-RubrikClusterEncryptionStatus.ps1

This will generate a CSV that describes the encryption status of each cluster attached to Rubrik Security Cloud.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : September 25, 2023
    Company : Rubrik Inc
#>

[cmdletbinding()]
param (
)

################################################################################################################################################################################################
#USER INPUTS

#These must be input specific to your environment. 
$ServiceAccountJson = "/Users/User/Documents/serviceAccountJson.json"
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
function Get-ClusterInfo{
    try{
        $query = "query ClusterListTableQuery(`$first: Int, `$after: String, `$filter: ClusterFilterInput, `$sortBy: ClusterSortByEnum, `$sortOrder: SortOrder, `$showOrgColumn: Boolean = false) {
            clusterConnection(filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, first: `$first, after: `$after) {
              edges {
                cursor
                node {
                  id
                  ...ClusterListTableFragment
                  ...OrganizationClusterFragment @include(if: `$showOrgColumn)
                }
              }
              pageInfo {
                startCursor
                endCursor
                hasNextPage
                hasPreviousPage
              }
              count
            }
          }
          
          fragment OrganizationClusterFragment on Cluster {
            allOrgs {
              name
            }
          }
          
          fragment ClusterListTableFragment on Cluster {
            id
            name
            pauseStatus
            defaultAddress
            ccprovisionInfo {
              progress
              jobStatus
              jobType
              __typename
            }
            estimatedRunway
            geoLocation {
              address
              __typename
            }
            ...ClusterCardSummaryFragment
            ...ClusterNodeConnectionFragment
            ...ClusterStateFragment
            ...ClusterGlobalManagerFragment
            ...ClusterAuthorizedOperationsFragment
            ...ClusterVersionColumnFragment
            ...ClusterTypeColumnFragment
            ...ClusterCapacityColumnFragment
          }
          
          fragment ClusterCardSummaryFragment on Cluster {
            status
            systemStatus
            systemStatusAffectedNodes {
              id
            }
            clusterNodeConnection {
              count
            }
            lastConnectionTime
          }
          
          fragment ClusterNodeConnectionFragment on Cluster {
            clusterNodeConnection {
              nodes {
                id
                status
                ipAddress
              }
            }
          }
          
          fragment ClusterStateFragment on Cluster {
            state {
              connectedState
              clusterRemovalState
            }
          }
          
          fragment ClusterGlobalManagerFragment on Cluster {
            passesConnectivityCheck
            globalManagerConnectivityStatus {
              urls {
                url
                isReachable
              }
            }
            connectivityLastUpdated
          }
          
          fragment ClusterAuthorizedOperationsFragment on Cluster {
            authorizedOperations {
              id
              operations
            }
          }
          
          fragment ClusterVersionColumnFragment on Cluster {
            version
          }
          
          fragment ClusterTypeColumnFragment on Cluster {
            name
            productType
            type
            clusterNodeConnection {
              nodes {
                id
              }
            }
          }
          
          fragment ClusterCapacityColumnFragment on Cluster {
            metric {
              usedCapacity
              availableCapacity
              totalCapacity
            }
          }"
        
        $variables = "{
            `"showOrgColumn`": true,
            `"sortBy`": `"ClusterName`",
            `"sortOrder`": `"ASC`",
            `"filter`": {
              `"id`": [],
              `"name`": [
                `"`"
              ],
              `"type`": [],
              `"orgId`": []
            }
          }"
        
        
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $clusterInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $clusterInfo = (((($clusterInfo.content | ConvertFrom-Json).data).clusterConnection).edges).node | where-object{$_.productType -ne "DATOS"}
        #$clusterList = $clusterInfo.id | ConvertTo-Json
    }
    catch{
        Write-Error("Error $($_)")
    }
        Write-Output $clusterInfo
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

$clusterInfo = Get-ClusterInfo
$clusterIds = $clusterInfo.id | ConvertTo-Json
$query = "query ClusterEncryptionDetails(`$clusters: [UUID!]!) {
    clusterEncryptionInfo(clusters: `$clusters) {
      nodes {
        name
        uuid
        cipher
        isEncrypted
        encryptionType
      }
    }
  }"

$variables = "{
    `"clusters`": $clusterIds
  }"
  $JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json

$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$clusterEncryptionStatus = ((($info.Content | ConvertFrom-Json).data).clusterEncryptionInfo).nodes
Write-Host ("Writing Encryption report to " + $Output_directory + "/clusterEncryptionReport-" +$mdate + ".csv")
$clusterEncryptionStatus | Export-Csv -NoTypeInformation ($Output_directory + "/clusterEncryptionReport-" +$mdate + ".csv")

disconnect-polaris




