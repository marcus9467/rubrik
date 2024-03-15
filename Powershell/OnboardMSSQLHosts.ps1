<#

.SYNOPSIS
This script will onboard new MSSQL hosts based on a provided CSV file and then later assign protection to MSSQL Databases. 

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV ./onboardhoststest.csv -clusterId "f60da42b-f191-4ed4-8278-164f148b839c" -OnboardHosts

This will onboard new Windows hosts that were noted in the supplied CSV file to cluster f60da42b-f191-4ed4-8278-164f148b839c

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV -GatherMSSQLHosts

Generates a CSV of unprotected MSSQL Hosts for use with the MSSQL onboarding process.

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV ./onboardhoststest.csv -OnboardMSSQL

This will onboard new MSSQL databases by applying protection at either the AG or Host level. Need to update the SLA logic based on tier. For the input CSV the expectation is to have the following headers:

serverName,SlaId

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : March 15, 2024
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
    [switch]$OnboardHosts,
    [parameter(Mandatory=$false)]
    [switch]$OnboardMSSQL,
    [parameter(Mandatory=$false)]
    [switch]$GatherMSSQLHosts
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
function Get-MssqlHosts{
    [CmdletBinding()]
  
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterId,
        [parameter(Mandatory=$false)]
        [switch]$UnProtectedObjects

    )
    try{
      $variables = "{
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
      if($UnProtectedHosts){
        $variables = "{
            `"first`": 200,
            `"filter`": [
                {
                    `"field`": `"PHYSICAL_HOST_BY_MSSQL_EFFECTIVE_SLA`",
                    `"texts`": [
                      `"Unprotected`"
                    ]
                  },
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
      }
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
        if($UnProtectedHosts){
            $variables = "{
                `"first`": 200,
                `"filter`": [
                    {
                        `"field`": `"PHYSICAL_HOST_BY_MSSQL_EFFECTIVE_SLA`",
                        `"texts`": [
                          `"Unprotected`"
                        ]
                      },
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
        }
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
function Set-mssqlSlas{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$slaId,
        [parameter(Mandatory=$true)]
        [string]$ObjectIds
    )

    $variables = "{
        `"input`": {
          `"updateInfo`": {
            `"ids`": ${objectIds},
            `"existingSnapshotRetention`": `"EXISTING_SNAPSHOT_RETENTION_RETAIN_SNAPSHOTS`",
            `"mssqlSlaPatchProperties`": {
              `"configuredSlaDomainId`": `"$slaId`",
              `"mssqlSlaRelatedProperties`": {
                `"copyOnly`": false
              }
            }
          },
          `"userNote`": `"`"
        }
      }"
    $query = "mutation AssignMssqlSLAMutation(`$input: AssignMssqlSlaDomainPropertiesAsyncInput!) {
        assignMssqlSlaDomainPropertiesAsync(input: `$input) {
          items {
            objectId
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
    (($result.Content | convertfrom-json).data).assignMssqlSlaDomainPropertiesAsync


}  
function Get-mssqlAGs{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterId,
        [parameter(Mandatory=$false)]
        [switch]$UnProtectedObjects
    )
    try{
        $variables = "{
            `"first`": 200,
            `"filter`": [
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
        if($UnProtectedObjects){
            $variables = "{
                `"first`": 200,
                `"filter`": [
                    {
                        `"field`": `"EFFECTIVE_SLA_WITH_RETENTION_SLA`",
                        `"texts`": [
                          `"Unprotected`"
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
        }
        $query = "query MssqlAvailabilityGroupListQuery(`$first: Int!, `$after: String, `$filter: [Filter!], `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isMultitenancyEnabled: Boolean = false, `$databaseDescendantFilter: [Filter!]) {
            mssqlTopLevelDescendants(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, typeFilter: [MssqlAvailabilityGroup]) {
              edges {
                cursor
                node {
                  id
                  authorizedOperations
                  ...MssqlNameColumnFragment
                  ...AvailabilityGroupDatabaseCopyOnlyColumnFragment
                  ...AvailabilityGroupMssqlDatabaseCountColumnFragment
                  ...CdmClusterColumnFragment
                  ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
                  ...CdmClusterLabelFragment
                  ...EffectiveSlaColumnFragment
                  ...SlaAssignmentColumnFragment
                  ...AvailabilityGroupInstanceColumnFragment
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
          
          fragment MssqlNameColumnFragment on HierarchyObject {
            id
            name
            objectType
            __typename
          }
          
          fragment AvailabilityGroupDatabaseCopyOnlyColumnFragment on MssqlAvailabilityGroup {
            copyOnly
            __typename
          }
          
          fragment AvailabilityGroupMssqlDatabaseCountColumnFragment on MssqlAvailabilityGroup {
            descendantConnection(filter: `$databaseDescendantFilter, typeFilter: [Mssql]) {
              count
              __typename
            }
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
          
          fragment SlaAssignmentColumnFragment on HierarchyObject {
            slaAssignment
            __typename
          }
          
          fragment AvailabilityGroupInstanceColumnFragment on MssqlAvailabilityGroup {
            instances {
              logicalPath {
                fid
                name
                __typename
              }
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
        $snappableInfo += (((($result.content | ConvertFrom-Json).data).mssqlTopLevelDescendants).edges).node
    
        while ((((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).pageInfo).hasNextPage -eq $true){
            $endCursor = (((($result.content | convertFrom-Json).data).mssqlTopLevelDescendants).pageInfo).endCursor
            Write-Host ("Looking at End Cursor " + $endCursor)
            $variables = "{
                `"first`": 200,
                `"filter`": [
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
            if($UnProtectedObjects){
                $variables = "{
                    `"first`": 200,
                    `"filter`": [
                        {
                            `"field`": `"EFFECTIVE_SLA_WITH_RETENTION_SLA`",
                            `"texts`": [
                              `"Unprotected`"
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
            }
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $snappableInfo += (((($result.content | ConvertFrom-Json).data).mssqlTopLevelDescendants).edges).node
    
        }
    }
    catch{
        Write-Error("Error $($_)")
      }
      finally{
        Write-Output $snappableInfo
      }
    
}
function Get-mssqlFCs{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterId,
        [parameter(Mandatory=$false)]
        [switch]$UnProtectedObjects
    )
    try{
        $variables = "{
            `"isMultitenancyEnabled`": true,
            `"first`": 200,
            `"filter`": [
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
        if($UnProtectedObjects){
            $variables = "{
                `"first`": 200,
                `"filter`": [
                    {
                        `"field`": `"EFFECTIVE_SLA_WITH_RETENTION_SLA`",
                        `"texts`": [
                          `"Unprotected`"
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
        }
        $query = "query MssqlFailoverClusterHierarchyClusterListQuery(`$first: Int!, `$after: String, `$filter: [Filter!], `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isMultitenancyEnabled: Boolean = false, `$instanceDescendantFilter: [Filter!], `$databaseDescendantFilter: [Filter!]) {
            mssqlTopLevelDescendants(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, typeFilter: [WindowsCluster]) {
              edges {
                cursor
                node {
                  id
                  authorizedOperations
                  ...ClusterChildInstancesEffectiveSlaColumnFragment
                  ... on WindowsCluster {
                    hosts {
                      id
                      ...CbtStatusColumnFragment
                      __typename
                    }
                    instanceDescendantConnection: descendantConnection(filter: `$instanceDescendantFilter, typeFilter: [MssqlInstance]) {
                      count
                      __typename
                    }
                    databaseDescendantConnection: descendantConnection(filter: `$databaseDescendantFilter, typeFilter: [Mssql]) {
                      count
                      __typename
                    }
                    __typename
                  }
                  ...MssqlNameColumnFragment
                  ...CdmClusterColumnFragment
                  ...CdmClusterLabelFragment
                  ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
                  ...EffectiveSlaColumnFragment
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
          
          fragment CbtStatusColumnFragment on PhysicalHost {
            cbtStatus
            defaultCbt
            __typename
          }
          
          fragment OrganizationsColumnFragment on HierarchyObject {
            allOrgs {
              name
              __typename
            }
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
              retentionLockMode
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
              retentionLockMode
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
          
          fragment ClusterChildInstancesEffectiveSlaColumnFragment on WindowsCluster {
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
                `"first`": 200,
                `"filter`": [
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
            if($UnProtectedObjects){
                $variables = "{
                    `"first`": 200,
                    `"filter`": [
                        {
                            `"field`": `"EFFECTIVE_SLA_WITH_RETENTION_SLA`",
                            `"texts`": [
                              `"Unprotected`"
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
            }
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $snappableInfo += (((($result.content | ConvertFrom-Json).data).mssqlTopLevelDescendants).edges).node
    
        }
    }
    catch{
        Write-Error("Error $($_)")
      }
      finally{
        Write-Output $snappableInfo
      }
    
}
function Get-SLADomains{
  <#
  .SYNOPSIS
  Gathers all the info for SLA domains in a given RSC instance. 
  #>
  try{
      $query = "query SLAListQuery(`$after: String, `$first: Int, `$filter: [GlobalSlaFilterInput!], `$sortBy: SlaQuerySortByField, `$sortOrder: SortOrder, `$shouldShowProtectedObjectCount: Boolean, `$shouldShowPausedClusters: Boolean = false) {
          slaDomains(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, shouldShowProtectedObjectCount: `$shouldShowProtectedObjectCount, shouldShowPausedClusters: `$shouldShowPausedClusters) {
            edges {
              cursor
              node {
                name
                ...AllObjectSpecificConfigsForSLAFragment
                ...SlaAssignedToOrganizationsFragment
                ... on ClusterSlaDomain {
                  id: fid
                  protectedObjectCount
                  baseFrequency {
                    duration
                    unit
                    __typename
                  }
                  archivalSpecs {
                    archivalLocationName
                    __typename
                  }
                  archivalSpec {
                    archivalLocationName
                    __typename
                  }
                  replicationSpecsV2 {
                    ...DetailedReplicationSpecsV2ForSlaDomainFragment
                    __typename
                  }
                  localRetentionLimit {
                    duration
                    unit
                    __typename
                  }
                  snapshotSchedule {
                    ...SnapshotSchedulesForSlaDomainFragment
                    __typename
                  }
                  isRetentionLockedSla
                  __typename
                }
                ... on GlobalSlaReply {
                  id
                  objectTypes
                  description
                  protectedObjectCount
                  baseFrequency {
                    duration
                    unit
                    __typename
                  }
                  archivalSpecs {
                    storageSetting {
                      id
                      name
                      groupType
                      targetType
                      __typename
                    }
                    archivalLocationToClusterMapping {
                      cluster {
                        id
                        name
                        __typename
                      }
                      location {
                        id
                        name
                        targetType
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  replicationSpecsV2 {
                    ...DetailedReplicationSpecsV2ForSlaDomainFragment
                    __typename
                  }
                  localRetentionLimit {
                    duration
                    unit
                    __typename
                  }
                  snapshotSchedule {
                    ...SnapshotSchedulesForSlaDomainFragment
                    __typename
                  }
                  pausedClustersInfo @include(if: `$shouldShowPausedClusters) {
                    pausedClustersCount
                    pausedClusters {
                      id
                      name
                      __typename
                    }
                    __typename
                  }
                  objectTypes
                  isRetentionLockedSla
                  __typename
                }
                __typename
              }
              __typename
            }
            pageInfo {
              endCursor
              hasNextPage
              hasPreviousPage
              __typename
            }
            __typename
          }
        }
        
        fragment AllObjectSpecificConfigsForSLAFragment on SlaDomain {
          objectSpecificConfigs {
            awsRdsConfig {
              logRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            sapHanaConfig {
              incrementalFrequency {
                duration
                unit
                __typename
              }
              differentialFrequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            db2Config {
              incrementalFrequency {
                duration
                unit
                __typename
              }
              differentialFrequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            oracleConfig {
              frequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              hostLogRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            mongoConfig {
              logFrequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            mssqlConfig {
              frequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            oracleConfig {
              frequency {
                duration
                unit
                __typename
              }
              logRetention {
                duration
                unit
                __typename
              }
              hostLogRetention {
                duration
                unit
                __typename
              }
              __typename
            }
            vmwareVmConfig {
              logRetentionSeconds
              __typename
            }
            azureSqlDatabaseDbConfig {
              logRetentionInDays
              __typename
            }
            azureSqlManagedInstanceDbConfig {
              logRetentionInDays
              __typename
            }
            awsNativeS3SlaConfig {
              continuousBackupRetentionInDays
              __typename
            }
            __typename
          }
          __typename
        }
        
        fragment SnapshotSchedulesForSlaDomainFragment on SnapshotSchedule {
          minute {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            __typename
          }
          hourly {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            __typename
          }
          daily {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            __typename
          }
          weekly {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            dayOfWeek
            __typename
          }
          monthly {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            dayOfMonth
            __typename
          }
          quarterly {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            dayOfQuarter
            quarterStartMonth
            __typename
          }
          yearly {
            basicSchedule {
              frequency
              retention
              retentionUnit
              __typename
            }
            dayOfYear
            yearStartMonth
            __typename
          }
          __typename
        }
        
        fragment DetailedReplicationSpecsV2ForSlaDomainFragment on ReplicationSpecV2 {
          replicationLocalRetentionDuration {
            duration
            unit
            __typename
          }
          cascadingArchivalSpecs {
            archivalTieringSpec {
              coldStorageClass
              shouldTierExistingSnapshots
              minAccessibleDurationInSeconds
              isInstantTieringEnabled
              __typename
            }
            archivalLocation {
              id
              name
              targetType
              ... on RubrikManagedAwsTarget {
                immutabilitySettings {
                  lockDurationDays
                  __typename
                }
                __typename
              }
              ... on RubrikManagedAzureTarget {
                immutabilitySettings {
                  lockDurationDays
                  __typename
                }
                __typename
              }
              ... on CdmManagedAwsTarget {
                immutabilitySettings {
                  lockDurationDays
                  __typename
                }
                __typename
              }
              ... on CdmManagedAzureTarget {
                immutabilitySettings {
                  lockDurationDays
                  __typename
                }
                __typename
              }
              ... on RubrikManagedRcsTarget {
                immutabilityPeriodDays
                syncStatus
                tier
                __typename
              }
              ... on RubrikManagedS3CompatibleTarget {
                immutabilitySetting {
                  bucketLockDurationDays
                  __typename
                }
                __typename
              }
              __typename
            }
            frequency
            archivalThreshold {
              duration
              unit
              __typename
            }
            __typename
          }
          retentionDuration {
            duration
            unit
            __typename
          }
          cluster {
            id
            name
            version
            __typename
          }
          targetMapping {
            id
            name
            targets {
              id
              name
              cluster {
                id
                name
                __typename
              }
              __typename
            }
            __typename
          }
          awsTarget {
            accountId
            accountName
            region
            __typename
          }
          azureTarget {
            region
            __typename
          }
          __typename
        }
        
        fragment SlaAssignedToOrganizationsFragment on SlaDomain {
          ... on GlobalSlaReply {
            allOrgsWithAccess {
              id
              name
              __typename
            }
            __typename
          }
          __typename
        }"
      $variables = "{
          `"shouldShowPausedClusters`": true,
          `"filter`": [],
          `"sortBy`": `"NAME`",
          `"sortOrder`": `"ASC`",
          `"shouldShowProtectedObjectCount`": true,
          `"first`": 200
      }"
      $JSON_BODY = @{
          "variables" = $variables
          "query" = $query
      }

      $SlaInfo = @()
      $JSON_BODY = $JSON_BODY | ConvertTo-Json
      $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
      $SlaInfo += (((($result.content | convertFrom-Json).data).slaDomains).edges).node

      while ((((($result.content | convertFrom-Json).data).slaDomains).pageInfo).hasNextPage -eq $true){
      $endCursor = (((($result.content | convertFrom-Json).data).slaDomains).pageInfo).endCursor
      Write-Host ("Looking at End Cursor " + $endCursor)
      $variables = "{
        `"shouldShowPausedClusters`": true,
        `"filter`": [],
        `"sortBy`": `"NAME`",
        `"sortOrder`": `"ASC`",
        `"shouldShowProtectedObjectCount`": true,
        `"first`": 200,
        `"after`": `"${endCursor}`"
      }"

    $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
      }
      $JSON_BODY = $JSON_BODY | ConvertTo-Json
      $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
      $SlaInfo += (((($result.content | convertFrom-Json).data).slaDomains).edges).node
      }
  }
  catch{
      Write-Error("Error $($_)")
  }
  finally{
      Write-Output $SlaInfo
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
if($GatherMSSQLHosts){
    $Output_directory = (Get-Location).path
    $mdate = (Get-Date).tostring("yyyyMMddHHmm")
    $sqlHostInfo = Get-MssqlHosts -clusterId $clusterId -UnProtectedObjects
    $AGInfo = Get-mssqlAGs -clusterId $clusterId -UnProtectedObjects
    $FCinfo = Get-mssqlFCs -clusterId $clusterId -UnProtectedObjects
    $UnprotectedSql = @()
    forEach($hostitem in $sqlHostInfo){
	    $hostInfo = New-object psobject
	    $hostInfo | Add-Member -NotePropertyName "ServerName" -NotePropertyValue $hostitem.name
	    $hostInfo | Add-Member -NotePropertyName "hostId" -NotePropertyValue $hostitem.id
	    $hostInfo | Add-Member -NotePropertyName "slaId" -NotePropertyValue $hostitem.effectiveSlaDomain.id
        $hostInfo | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "StandAlone"
	    $UnprotectedSql += $hostinfo
    }
    forEach($hostitem in $AGInfo){
	    $hostInfo = New-object psobject
	    $hostInfo | Add-Member -NotePropertyName "ServerName" -NotePropertyValue $hostitem.name
	    $hostInfo | Add-Member -NotePropertyName "hostId" -NotePropertyValue $hostitem.id
	    $hostInfo | Add-Member -NotePropertyName "slaId" -NotePropertyValue $hostitem.effectiveSlaDomain.id
        $hostInfo | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "availabilityGroup"
	    $UnprotectedSql += $hostinfo
    }
    forEach($hostitem in $FCinfo){
	    $hostInfo = New-object psobject
	    $hostInfo | Add-Member -NotePropertyName "ServerName" -NotePropertyValue $hostitem.name
	    $hostInfo | Add-Member -NotePropertyName "hostId" -NotePropertyValue $hostitem.id
	    $hostInfo | Add-Member -NotePropertyName "slaId" -NotePropertyValue $hostitem.effectiveSlaDomain.id
        $hostInfo | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "failoverCluster"
	    $UnprotectedSql += $hostinfo
    }
    Write-Host ("Writing CSV file to "  + $Output_directory + "/UnprotectedMssqlHosts" + $mdate + ".csv")
    $UnprotectedSql | Export-Csv -NoTypeInformation ($Output_directory + "/UnprotectedMssqlHosts" +$mdate + ".csv")
    disconnect-rsc
}
if($OnboardMSSQL){
    $hostlist = Import-Csv $CSV
    $hostlistCount = ($hostlist | Measure-Object).Count
    $Output_directory = (Get-Location).path
    $mdate = (Get-Date).tostring("yyyyMMddHHmm")

    $IndexCount = 1
    $MissingHostList = @()
    # Get a List of Current MSSQL Hosts and DBs that are unprotected 
    $sqlHostInfo = Get-MssqlHosts -clusterId $clusterId -UnProtectedObjects
    $AGInfo = Get-mssqlAGs -clusterId $clusterId -UnProtectedObjects
    $FCinfo = Get-mssqlFCs -clusterId $clusterId -UnProtectedObjects
    $AssignmentObjects = @()
    ForEach($objectName in $hostlist){    
        ForEach($AG in $AGInfo){
            if((($AG.instances).logicalPath).name -match $objectName.Servername){
                $mssqlObject = New-Object PSobject
                $mssqlObject | Add-Member -NotePropertyName "Name" -NotePropertyValue $objectName.ServerName
                $mssqlObject | Add-Member -NotePropertyName "Id" -NotePropertyValue ($AG.id | ConvertTo-Json)
                $mssqlObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $ObjectName.slaID
                $mssqlObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "availabilityGroup"
                $AssignmentObjects += $mssqlObject
            }
        }
        ForEach($SQLHost in $sqlHostInfo){
            if(($SQLHost).name -match $objectName.servername){
                $mssqlObject = New-Object PSobject
                $mssqlObject | Add-Member -NotePropertyName "Name" -NotePropertyValue $objectName.ServerName
                $mssqlObject | Add-Member -NotePropertyName "Id" -NotePropertyValue ($SQLHost.id | ConvertTo-Json)
                $mssqlObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $ObjectName.slaID
                $mssqlObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "standAlone"
                $AssignmentObjects += $mssqlObject
                if($fcinfo.hosts.id -contains $SQLHost.id){
                    foreach($FC in $FCinfo){
                        if($FC.hosts.id -match $SQLHost.id){
                            $FCObject = New-Object PSobject
                            $FCObject | Add-Member -NotePropertyName "Name" -NotePropertyValue $objectName.ServerName
                            $FCObject | Add-Member -NotePropertyName "Id" -NotePropertyValue ($SQLHost.id | ConvertTo-Json)
                            $FCObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $ObjectName.slaID
                            $FCObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "failoverCluster"
                            $AssignmentObjects += $FCObject
                        }
                    }
                    
                }
                
            }
        }
    }   
    $AssignmentObjectsCount = ($AssignmentObjects | Measure-Object).count
    $AssignmentObjectsIndex = 1
    foreach($Object in $AssignmentObjects){
        Write-Output ("Assigning SLA "+ $object.slaid + " to Object " + $object.Name)
        Set-mssqlSlas -ObjectIds $object.id -slaId $object.slaId
        Write-Host ("Assigned SLA to object " + $AssignmentObjectsIndex + " of " + $AssignmentObjectsCount)
        $AssignmentObjectsIndex++
    }
    Write-Host "Disconnecting From Rubrik Security Cloud."
    disconnect-rsc
}
