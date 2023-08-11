<#

.SYNOPSIS
This script will onboard new MSSQL hosts based on a provided CSV file and then later assign protection to MSSQL Databases. 

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV ./onboardhoststest.csv -clusterId "f60da42b-f191-4ed4-8278-164f148b839c" -OnboardHosts

This will onboard new Windows hosts that were noted in the supplied CSV file to cluster f60da42b-f191-4ed4-8278-164f148b839c

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : August 11, 2023
    Company : Rubrik Inc

#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$false)]
    [string]$SlaIds,
    [parameter(Mandatory=$false)]
    [string]$CSV,
    [parameter(Mandatory=$false)]
    [string]$clusterId,
    [parameter(Mandatory=$false)]
    [switch]$OnboardHosts
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
function Get-MssqlDbs{
    [CmdletBinding()]
  
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterId
    )
    try{
      $variables = "{
        `"isMultitenancyEnabled`": true,
        `"first`": 200,
        `"filter`": [
          {
            `"field`": `"CLUSTER_ID`",
            `"texts`": [
              `"$clusterId`"
            ]
          },
          {
            `"field`": `"IS_RELIC`",
            `"texts`": [
              `"false`"
            ]
          },
          {
            `"field`": `"IS_REPLICATED`",
            `"texts`": [
              `"false`"
            ]
          },
          {
            `"field`": `"IS_ARCHIVED`",
            `"texts`": [
              `"false`"
            ]
          }
        ],
        `"sortBy`": `"NAME`",
        `"sortOrder`": `"ASC`",
        `"instanceDescendantFilter`": [
          {
            `"field`": `"IS_ARCHIVED`",
            `"texts`": [
              `"false`"
            ]
          }
        ],
        `"databaseDescendantFilter`": [
          {
            `"field`": `"IS_LOG_SHIPPING_SECONDARY`",
            `"texts`": [
              `"false`"
            ]
          },
          {
            `"field`": `"IS_MOUNT`",
            `"texts`": [
              `"false`"
            ]
          },
          {
            `"field`": `"IS_ARCHIVED`",
            `"texts`": [
              `"false`"
            ]
          }
        ]
      }"
      $query = "query MssqlHostHierarchyHostListQuery(`$first: Int!, `$after: String, `$filter: [Filter!], `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isMultitenancyEnabled: Boolean = false, `$instanceDescendantFilter: [Filter!], `$databaseDescendantFilter: [Filter!]) {
        mssqlTopLevelDescendants(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, typeFilter: [PhysicalHost]) {
          edges {
            cursor
            node {
              id
              authorizedOperations
              ...HostChildInstancesEffectiveSlaColumnFragment
              ... on PhysicalHost {
                instanceDescendantConnection: descendantConnection(filter: `$instanceDescendantFilter, typeFilter: [MssqlInstance]) {
                  count
                  __typename
                }
                databaseDescendantConnection: descendantConnection(filter: `$databaseDescendantFilter, typeFilter: [Mssql]) {
                  count
                  __typename
                }
                ...MssqlNameColumnFragment
                ...CbtStatusColumnFragment
                ...CdmClusterColumnFragment
                ...CdmClusterLabelFragment
                ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
                ...EffectiveSlaColumnFragment
                ...PhysicalHostConnectionStatusColumnFragment
                __typename
              }
              __typename
            }
            __typename
          }
          pageInfo {
            startCursor
            endCursor
            hasNextPage
            hasPreviousPage
            __typename
          }
          __typename
        }
      }
      
      fragment OrganizationsColumnFragment on HierarchyObject {
        allOrgs {
          name
          __typename
        }
        __typename
      }
      
      fragment CbtStatusColumnFragment on PhysicalHost {
        cbtStatus
        defaultCbt
        __typename
      }
      
      fragment MssqlNameColumnFragment on HierarchyObject {
        id
        name
        objectType
        __typename
      }
      
      fragment CdmClusterColumnFragment on CdmHierarchyObject {
        replicatedObjectCount
        cluster {
          id
          name
          version
          status
          __typename
        }
        __typename
      }
      
      fragment CdmClusterLabelFragment on CdmHierarchyObject {
        cluster {
          id
          name
          version
          __typename
        }
        primaryClusterLocation {
          id
          __typename
        }
        __typename
      }
      
      fragment HostChildInstancesEffectiveSlaColumnFragment on PhysicalHost {
        id
        instanceDescendantConnection: descendantConnection(filter: `$instanceDescendantFilter, typeFilter: [MssqlInstance]) {
          edges {
            node {
              id
              ...EffectiveSlaColumnFragment
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      
      fragment EffectiveSlaColumnFragment on HierarchyObject {
        id
        effectiveSlaDomain {
          ...EffectiveSlaDomainFragment
          ... on GlobalSlaReply {
            description
            __typename
          }
          __typename
        }
        ... on CdmHierarchyObject {
          pendingSla {
            ...SLADomainFragment
            __typename
          }
          __typename
        }
        __typename
      }
      
      fragment EffectiveSlaDomainFragment on SlaDomain {
        id
        name
        ... on GlobalSlaReply {
          isRetentionLockedSla
          __typename
        }
        ... on ClusterSlaDomain {
          fid
          cluster {
            id
            name
            __typename
          }
          isRetentionLockedSla
          __typename
        }
        __typename
      }
      
      fragment SLADomainFragment on SlaDomain {
        id
        name
        ... on ClusterSlaDomain {
          fid
          cluster {
            id
            name
            __typename
          }
          __typename
        }
        __typename
      }
      
      fragment PhysicalHostConnectionStatusColumnFragment on PhysicalHost {
        id
        authorizedOperations
        connectionStatus {
          connectivity
          __typename
        }
        __typename
      }"
      $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
      }
    $snappableInfo = @()
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $snappableInfo += (((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).edges).node
  
    while ((((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).pageInfo).hasNextPage -eq $true){
        $endCursor = (((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).pageInfo).endCursor
        Write-Host ("Looking at End Cursor " + $endCursor)
        $variables = "{
          `"isMultitenancyEnabled`": true,
          `"first`": 200,
          `"filter`": [
            {
              `"field`": `"CLUSTER_ID`",
              `"texts`": [
                `"$clusterId`"
              ]
            },
            {
              `"field`": `"IS_RELIC`",
              `"texts`": [
                `"false`"
              ]
            },
            {
              `"field`": `"IS_REPLICATED`",
              `"texts`": [
                `"false`"
              ]
            },
            {
              `"field`": `"IS_ARCHIVED`",
              `"texts`": [
                `"false`"
              ]
            }
          ],
          `"sortBy`": `"NAME`",
          `"sortOrder`": `"ASC`",
          `"instanceDescendantFilter`": [
            {
              `"field`": `"IS_ARCHIVED`",
              `"texts`": [
                `"false`"
              ]
            }
          ],
          `"databaseDescendantFilter`": [
            {
              `"field`": `"IS_LOG_SHIPPING_SECONDARY`",
              `"texts`": [
                `"false`"
              ]
            },
            {
              `"field`": `"IS_MOUNT`",
              `"texts`": [
                `"false`"
              ]
            },
            {
              `"field`": `"IS_ARCHIVED`",
              `"texts`": [
                `"false`"
              ]
            }
          ],
          `"after`": `"${endCursor}`"
        }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $snappableInfo += (((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).edges).node
    }
    }
    catch{
      Write-Error("Error $($_)")
    }
    finally{
      Write-Output $snappableInfo
    }
  }
  function Register-Host{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterId,
        [parameter(Mandatory=$true)]
        [string]$clientName
    )
        try{
            $objectCount = ($clientName.split(",") | measure-object).count
            if($objectCount -gt 1){
                $clientName = $clientName.split(",")
                $clientArray = @()
                ForEach($Object in $clientName){
                  $formattedClient = @{"hostname" = $Object}
                  $clientArray += $formattedClient
                }
                $clientArray = $clientArray | ConvertTo-Json
                $variables = "{
                  `"clusterUuid`": `"$clusterId`",
                  `"hosts`": $clientArray
                }"
            }
            if($objectCount -eq 1){
              $variables = "{
                `"clusterUuid`": `"$clusterId`",
                `"hosts`": [
                  {
                    `"hostname`": `"$clientName`"
                  }
                ]
              }"
            }
            $query = "mutation AddPhysicalHostMutation(`$clusterUuid: String!, `$hosts: [HostRegisterInput!]!) {
              bulkRegisterHost(input: {clusterUuid: `$clusterUuid, hosts: `$hosts}) {
                data {
                  hostSummary {
                    id
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
            $jobStatus = (((($result.content | convertFrom-Json).data).bulkRegisterHost).data).hostSummary
            $jobErrors = (($result.content | convertFrom-Json).errors).message
          }
          catch{
            Write-Error("Error $($_)")
          }
          finally{
            Write-Output $jobStatus
            Write-Output $jobErrors
          } 
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

if($OnboardHosts){
    $hostlist = Import-Csv $CSV
    $hostlist = $hostlist.Name
    $hostlistCount = ($hostlist | Measure-Object).Count
    $Output_directory = (Get-Location).path
    $mdate = (Get-Date).tostring("yyyyMMddHHmm")

    $IndexCount = 1
    $MissingHostList = @()

    ForEach($client in $hostlist){
        try{
            Write-Host ("Registering Host  " + $client)
            Register-Host -clusterId $clusterId -clientName $client -ErrorVariable vmerror
            Write-Host ("Finished Processing " + $IndexCount + " of " + $hostlistCount + " Hosts")
        }
        catch{
            Write-Host ("Unable to register host " + $client)
            Write-Host "Appending to a CSV for later review"
            $errorMessage = ($vmerror.message | Select-Object -last 1)
            $clientErrorInfo = New-Object psobject
            $clientErrorInfo | Add-Member -NotePropertyName "Name" -NotePropertyValue $VM
            $clientErrorInfo | Add-Member -NotePropertyName "Id" -NotePropertyValue $VmInfo.id
            $clientErrorInfo | Add-Member -NotePropertyName "errorMessage" -NotePropertyValue $errorMessage
    
            $MissinghostList += $clientErrorInfo  
        }
        $IndexCount++
    }
    if((-not ([string]::IsNullOrEmpty($MissinghostList)))){
        Write-Host ("Writing CSV file to "  + $Output_directory + "/MissingHostsReport_" + $clusterName + "_" +$mdate + ".csv")
        $MissinghostList | Export-Csv -NoTypeInformation ($Output_directory + "/MissingHostsReport_" + $clusterName + "_" +$mdate + ".csv")
    }
    Write-Host "Disconnecting From Rubrik Security Cloud."
    disconnect-rsc
}
