<#

.SYNOPSIS
Example script for how to rotate vCenter credentials via a service account in RSC.

.EXAMPLE
./Update-vCenterCredentials.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -vCenterId "4cd4beec-cd9c-572b-b115-075fa3ebbd8e" -vCenterHostname "vCenter.rubrik.com" -user "admin" -pass "secret_password"

This will update the vCenter password for the selected vCenter using the service account credentials provided via JSON. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : October 02, 2023
    Company : Rubrik Inc
#>




[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$vCenterId,
    [parameter(Mandatory=$true)]
    [string]$vCenterHostname,
    [parameter(Mandatory=$true)]
    [string]$user,
    [parameter(Mandatory=$true)]
    [string]$pass
)

##################################

# Adding certificate exception to prevent API errors

##################################
if ($IsWindows -eq $true){

<#
  add-type @"

    using System.Net;

    using System.Security.Cryptography.X509Certificates;

    public class TrustAllCertsPolicy : ICertificatePolicy {

        public bool CheckValidationResult(

            ServicePoint srvPoint, X509Certificate certificate,

            WebRequest request, int certificateProblem) {

            return true;

        }

    }

"@

  [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
#>


}

if($IsMacOS -eq $true){
  #Do Nothing for now
}
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

$query = "mutation UpdateVcenterMutation(`$input: UpdateVcenterInput!) {
    updateVcenter(input: `$input) {
      output {
        computeVisibilityFilter {
          name
          clusterVisibilityConfig {
            id
            hostGroupFilter
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
  }"
$variables = "{
    `"input`": {
      `"id`": `"$vCenterId`",
      `"updateProperties`": {
        `"shouldEnableHotAddProxyForOnPrem`": false,
        `"username`": `"$user`",
        `"password`": `"$pass`",
        `"hostname`": `"$vCenterHostname`",
        `"computeVisibilityFilter`": []
      }
    }
  }"
$JSON_BODY = @{
    "variables" = $variables
    "query" = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
$info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$vCenterUpdate = (((($info.content | ConvertFrom-Json).data).updateVcenter)).output
Write-Output $vCenterUpdate
disconnect-polaris
