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
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV ./onboardhoststest.csv -GenerateOnboardMSSQLCSV -clusterId $clusterId

Generates a CSV that shows the proposed new SLA assignment as well as the current assignment. This allows for human review before sending the generated CSV to the actual assignment phase. 
serverName,SlaId,failoverClusterName

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceaccountJson -CSV ./onboardhoststest.csv -AssignSLA -batched  -clusterId $clusterId

Assigns the SLAs proposed in the prior step's CSV. Can be batched to group up to 50 MSSQL instances in each API call. 

.EXAMPLE
./OnboardMSSQLHosts.ps1 -ServiceAccountJson $serviceAccountJson -clusterId $clusterId -cdmValidate -CSV ./mssqlAssignmentList-202403281151.csv -clusterIP "10.8.49.104"

Validates the SLA assignment with the local CDM using the CSV produced by either the AssignSLA or GenerateOnboardMSSQLCSV flags, and produces a CSV for human review.

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
    [switch]$GatherMSSQLHosts,
    [parameter(Mandatory=$false)]
    [switch]$batched,
    [parameter(Mandatory=$false)]
    [switch]$GenerateOnboardMSSQLCSV,
    [parameter(Mandatory=$false)]
    [switch]$AssignSLA,
    [parameter(Mandatory=$false)]
    [string]$clusterIP,
    [parameter(Mandatory=$false)]
    [switch]$cdmValidate
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
function Set-mssqlSlasBatch{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$slaId,
        [parameter(Mandatory = $true)]
        [string]$ObjectIds
    )

    try{    
        $variables = "{
        `"input`": {
          `"updateInfo`": {
            `"ids`": ${objectIds},
            `"shouldApplyToExistingSnapshots`": false,
            `"shouldApplyToNonPolicySnapshots`": false,
            `"mssqlSlaPatchProperties`": {
              `"configuredSlaDomainId`": `"$slaId`",
              `"mssqlSlaRelatedProperties`": {
                `"copyOnly`": false,
                `"hasLogConfigFromSla`": true,
                `"hostLogRetention`": -1
              },
              `"useConfiguredDefaultLogRetention`": false
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
    ((($result.Content | convertfrom-json).data).assignMssqlSlaDomainPropertiesAsync).items
    }
    catch{
        Write-Error("Error $($_)")
      }
      finally{
        Write-Output ((($result.Content | convertfrom-json).data).assignMssqlSlaDomainPropertiesAsync).items
    }
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
function Get-PhysicalHost{
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$true)]
      [string]$clusterId,
      [parameter(Mandatory=$false)]
      [switch]$UnProtectedObjects,
      [parameter(Mandatory=$false)]
      [string]$ObjectName
  )
  try{
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"WINDOWS_HOST_ROOT`",
      `"first`": 50,
      `"filter`": [
        {
          `"field`": `"NAME`",
          `"texts`": [
            `"$objectName`"
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
          `"field`": `"IS_KUPR_HOST`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"childFilter`": [
        {
          `"field`": `"IS_GHOST`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_RELIC`",
          `"texts`": [
            `"false`"
          ]
        }
      ]
    }"
    $query = "query PhysicalHostListQuery(`$hostRoot: HostRoot!, `$first: Int!, `$after: String, `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$filter: [Filter!]!, `$childFilter: [Filter!], `$isMultitenancyEnabled: Boolean = false) {
      physicalHosts(hostRoot: `$hostRoot, filter: `$filter, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
        edges {
          cursor
          node {
            id
            name
            isArchived
            descendantConnection(typeFilter: [LinuxFileset, WindowsFileset]) {
              edges {
                node {
                  id
                  name
                  objectType
                  __typename
                }
                __typename
              }
              __typename
            }
            authorizedOperations
            cluster {
              id
              name
              version
              status
              ...ClusterNodeConnectionFragment
              __typename
            }
            ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
            primaryClusterLocation {
              id
              __typename
            }
            effectiveSlaDomain {
              ...EffectiveSlaDomainFragment
              __typename
            }
            osType
            osName
            connectionStatus {
              connectivity
              timestampMillis
              __typename
            }
            isOracleHost
            oracleUserDetails {
              sysDbaUser
              queryUser
              __typename
            }
            ...PhysicalHostConnectionStatusColumnFragment
            physicalChildConnection(typeFilter: [LinuxFileset, WindowsFileset], filter: `$childFilter) {
              count
              edges {
                node {
                  id
                  name
                  objectType
                  slaPauseStatus
                  effectiveSlaDomain {
                    ...EffectiveSlaDomainFragment
                    __typename
                  }
                  pendingSla {
                    ...SLADomainFragment
                    __typename
                  }
                  ...LinuxFilesetListFragment
                  ...WindowsFilesetListFragment
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        pageInfo {
          endCursor
          startCursor
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
    
    fragment ClusterNodeConnectionFragment on Cluster {
      clusterNodeConnection {
        nodes {
          id
          status
          ipAddress
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
    }
    
    fragment LinuxFilesetListFragment on LinuxFileset {
      isRelic
      excludes: pathExcluded
      includes: pathIncluded
      exceptions: pathExceptions
      isPassThrough
      replicatedObjects {
        cluster {
          id
          name
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment WindowsFilesetListFragment on WindowsFileset {
      isRelic
      excludes: pathExcluded
      includes: pathIncluded
      exceptions: pathExceptions
      isPassThrough
      replicatedObjects {
        cluster {
          id
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

  $windowsHostInfo = @()
  $JSON_BODY = $JSON_BODY | ConvertTo-Json
  $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
  $windowsHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node

  while ((((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).hasNextPage -eq $true){
  $endCursor = (((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).endCursor
  Write-Host ("Looking at End Cursor " + $endCursor)
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"WINDOWS_HOST_ROOT`",
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
          `"field`": `"IS_KUPR_HOST`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"childFilter`": [
        {
          `"field`": `"IS_GHOST`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_RELIC`",
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
    $windowsHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node
  }
  }
  catch{
    Write-Error("Error $($_)")
  }
  finally{
    Write-Output $windowsHostInfo
  }
}
function Connect-RubrikCdm{
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$true)]
      [string]$clusterId,
      [parameter(Mandatory=$true)]
      [string]$serviceAccountJson
  )
  try{
      $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
        $connectionData = [ordered]@{
            'client_id' = $serviceAccountObj.client_id
            'client_secret' = $serviceAccountObj.client_secret
            'cluster_uuid' = $clusterId
        } | ConvertTo-Json
        $cdmTokenUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "cdm_client_token")

    $rubrikCdm = Invoke-RestMethod -Method Post -uri $cdmTokenUrl -ContentType application/json -body $connectionData -skipcertificateCheck
  }
  catch{
    Write-Error("Error $($_)")
  }
  finally{
    Write-Output $rubrikCdm
  }
}
function Connect-RubrikSpecialCdm{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$clusterIp,
        [parameter(Mandatory=$true)]
        [string]$serviceAccountJson
    )
    try{
        $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
          $connectionData = [ordered]@{
              'serviceAccountId' = $serviceAccountObj.client_id
              'secret' = $serviceAccountObj.client_secret
          } | ConvertTo-Json
          $uriString = "https://$($clusterIp)/api/v1/service_account/session"
  
      $rubrikCdm = Invoke-RestMethod -Method Post -uri $uriString -ContentType application/json -body $connectionData -skipcertificateCheck
    }
    catch{
      Write-Error("Error $($_)")
    }
    finally{
      Write-Output $rubrikCdm
    }
  }

if($cdmValidate){
    $Output_directory = (Get-Location).path
    $mdate = (Get-Date).tostring("yyyyMMddHHmm")
    #Establish Session to Local CDM
    $rubrikConnection = Connect-RubrikSpecialCdm -clusterIp $clusterIp -serviceAccountJson $ServiceAccountJson
    $rubtok = $rubrikconnection.token
    $RubrikToken =  @{'Authorization' = ("Bearer $rubtok")}

    #Import Assignment CSV to check SLA Status
    $sqlList = Import-Csv $CSV
    $AssignmentConfirmList = @()
    ForEach($instance in $sqlList){
      if($instance.assignmentType -eq "standAlone"){
        Write-Output ("Investigating SLA for SQL Host " + $instance.hostName)
        $singleInstance = Invoke-WebRequest -Uri ("https://" + $clusterIp + "/api/v1/mssql/hierarchy/root/children?has_instances=true&is_clustered=false&is_live_mount=false&limit=51&name="+ $instance.hostName +"&object_type=Host,MssqlInstance&offset=0&primary_cluster_id=local&snappable_status=Protectable&sort_by=name&sort_order=asc") -Method GET -Headers $RubrikToken -SkipCertificateCheck
      }
      if($instance.assignmentType -eq "availabilityGroup"){
        Write-Output ("Investigating SLA for SQL Availability Group " + $instance.sqlClusterName)
        $singleInstance = Invoke-WebRequest -Uri ("https://" + $clusterIp + "/api/v1/mssql/hierarchy/root/children?has_instances=false&is_clustered=false&is_live_mount=false&limit=51&name="+ $instance.sqlClusterName + "&object_type=MssqlAvailabilityGroup,MssqlDatabase&offset=0&primary_cluster_id=local&snappable_status=Protectable&sort_by=name&sort_order=asc") -Method GET -Headers $RubrikToken -SkipCertificateCheck
      }
      if($instance.assignmentType -eq "failoverCluster"){
        Write-Output ("Investigating SLA for SQL Failover Cluster " + $instance.sqlClusterName)
        $singleInstance = Invoke-WebRequest -Uri ("https://" + $clusterIp + "/api/v1/mssql/hierarchy/root/children?has_instances=true&is_clustered=false&is_live_mount=false&limit=51&name="+ $instance.sqlClusterName +"&object_type=WindowsCluster,MssqlInstance&offset=0&primary_cluster_id=local&snappable_status=Protectable&sort_by=name&sort_order=asc") -Method GET -Headers $RubrikToken -SkipCertificateCheck
      }
      
      $singleInstance = ($singleInstance.Content | ConvertFrom-Json).data
      $AssignmentConfirmList += $singleInstance
    }
    #Wait-Debugger
    Write-Host ("Writing CSV file to "  + $Output_directory + "/cdmValidateList-" + $mdate + ".csv")
    $AssignmentConfirmList | Export-Csv -NoTypeInformation ($Output_directory + "/cdmValidateList-" +$mdate + ".csv")
exit  
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
    $hostlist = $hostlist.serverName
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
if($GenerateOnboardMSSQLCSV){
    $hostlist = Import-Csv $CSV
    $hostlistCount = ($hostlist | Measure-Object).Count
    $Output_directory = (Get-Location).path
    $mdate = (Get-Date).tostring("yyyyMMddHHmm")

    $IndexCount = 1
    $MissingHostList = @()
    # Get a List of Current MSSQL Hosts and DBs that are unprotected 
    $sqlHostInfo = Get-MssqlHosts -clusterId $clusterId #-UnProtectedObjects
    $AGInfo = Get-mssqlAGs -clusterId $clusterId #-UnProtectedObjects
    $FCinfo = Get-mssqlFCs -clusterId $clusterId #-UnProtectedObjects
    $AssignmentObjects = @()
    Write-Host "Resolving failover cluster relationships for any hosts where failoverClusterName is not NULL"
    ForEach($WindowsMachine in $hostlist){
      if($WindowsMachine.failoverClusterName -ne "NULL"){
        Write-Host ("Looking up windows host information to compare FC membership for object " + $WindowsMachine.ServerName)
        $FC = $FCinfo | Where-Object {$_.name -match $WindowsMachine.failoverClusterName}
        $instanceList = $FC.instanceDescendantConnection.edges.node
        foreach($instance in $instanceList){
          Write-Host ("Gathering Information for FC " + $FC.Name)
          $FCObject = New-Object PSobject
          $FCObject | Add-Member -NotePropertyName "hostName" -NotePropertyValue $WindowsMachine.ServerName
          $FCObject | Add-Member -NotePropertyName "sqlClusterName" -NotePropertyValue $FC.Name
          $FCObject | Add-Member -NotePropertyName "hostId" -NotePropertyValue "notApplicable"
          $FCObject | Add-Member -NotePropertyName "instanceId" -NotePropertyValue $instance.id 
          $FCObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $WindowsMachine.slaID
          $FCObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "failoverCluster"
          $FCObject | Add-Member -NotePropertyName "currentSlaId" -NotePropertyValue ($instance.effectiveSlaDomain).id
          $FCObject | Add-Member -NotePropertyName "currentSlaName" -NotePropertyValue ($instance.effectiveSlaDomain).name
          $AssignmentObjects += $FCObject
        }
      }
    }
    ForEach($objectName in $hostlist){ 
        #Availability Groups   
        ForEach($AG in $AGInfo){
            if((($AG.instances).logicalPath).name -match $objectName.Servername){
                Write-Host ("Gathering Information for AG " + $AG.Name)
                $mssqlObject = New-Object PSobject
                $mssqlObject | Add-Member -NotePropertyName "hostName" -NotePropertyValue $objectName.ServerName
                $mssqlObject | Add-Member -NotePropertyName "sqlClusterName" -NotePropertyValue $AG.Name
                $mssqlObject | Add-Member -NotePropertyName "hostId" -NotePropertyValue "notApplicable"
                $mssqlObject | Add-Member -NotePropertyName "instanceId" -NotePropertyValue $AG.id
                $mssqlObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $ObjectName.slaID
                $mssqlObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "availabilityGroup"
                $mssqlObject | Add-Member -NotePropertyName "currentSlaId" -NotePropertyValue ($instance.effectiveSlaDomain).id
                $mssqlObject | Add-Member -NotePropertyName "currentSlaName" -NotePropertyValue ($instance.effectiveSlaDomain).name
                $AssignmentObjects += $mssqlObject
            }
        }
        ForEach($SQLHost in $sqlHostInfo){
            if($SQLHost.name -match $objectName.ServerName){
            #StandAlone Hosts
              if(($SQLHost).name -match $objectName.servername){
                $instanceList = $sqlHost.instanceDescendantConnection.edges.node
                foreach($instance in $instanceList){
                        Write-Host ("Gathering Information for SQLHost " + $objectName.ServerName)
                        $mssqlObject = New-Object PSobject
                        $mssqlObject | Add-Member -NotePropertyName "hostName" -NotePropertyValue $objectName.ServerName
                        $mssqlObject | Add-Member -NotePropertyName "sqlClusterName" -NotePropertyValue "notApplicable"
                        $mssqlObject | Add-Member -NotePropertyName "hostId" -NotePropertyValue $SQLHost.id
                        $mssqlObject | Add-Member -NotePropertyName "instanceId" -NotePropertyValue $instance.id
                        $mssqlObject | Add-Member -NotePropertyName "slaId" -NotePropertyValue $ObjectName.slaID
                        $mssqlObject | Add-Member -NotePropertyName "assignmentType" -NotePropertyValue "standAlone"
                        $mssqlObject | Add-Member -NotePropertyName "currentSlaId" -NotePropertyValue ($instance.effectiveSlaDomain).id
                        $mssqlObject | Add-Member -NotePropertyName "currentSlaName" -NotePropertyValue ($instance.effectiveSlaDomain).name
                        $AssignmentObjects += $mssqlObject
                }
              }
            }
        }
    }
    $AssignmentObjects = $AssignmentObjects | Sort-Object -Unique {$_.instanceId}
    Write-Host ("Writing CSV file to "  + $Output_directory + "/mssqlAssignmentList-" + $mdate + ".csv")
    $AssignmentObjects | Export-Csv -NoTypeInformation ($Output_directory + "/mssqlAssignmentList-" +$mdate + ".csv")
Disconnect-Rsc
}
if($AssignSLA){
  $AssignmentObjects = Import-Csv $CSV
  $Output_directory = (Get-Location).path
  $mdate = (Get-Date).tostring("yyyyMMddHHmm")
  if($batched){
    # Assuming $AssignmentObjects is already populated
    # Group by SLAId
    $groupedObjects = $AssignmentObjects | Group-Object -Property slaId
    Write-host "Grouping MSSQL objects into batches of 50 based on the supplied SLA domains"
    foreach ($group in $groupedObjects) {
        $slaId = $group.Name
        $allIds = $group.Group.instanceId

        # Split into batches of 50
        $batches = [System.Collections.Generic.List[object]]::new()
        foreach ($id in $allIds) {
         $batches.Add($id)
            if ($batches.Count -eq 50) {
                Write-Host ("Applying SLA to the following Objects " + $batches.ToArray())
                Write-Host "======================================================================================================================================================================================================"
                Write-Host "API Output:"
                Set-mssqlSlasBatch -slaId $slaId -ObjectIds ($batches | ConvertTo-Json)
                $batches.Clear()
            }
        }
        # Process remaining items if any
        if ($batches.Count -gt 0) {
            Write-Host ("Applying SLA to the following Objects " + $batches.ToArray())
            Write-Host "======================================================================================================================================================================================================"
            Write-Host "API Output:"
            Set-mssqlSlasBatch -slaId $slaId -ObjectIds ($batches | ConvertTo-Json)
        }
    }
    Write-Host ("Writing CSV file to "  + $Output_directory + "/AssignedSLAMSSQL-" + $mdate + ".csv")
    $AssignmentObjects | Export-Csv -NoTypeInformation ($Output_directory + "/AssignedSLAMSSQL-" +$mdate + ".csv")
    Write-Host "Disconnecting From Rubrik Security Cloud."
    disconnect-rsc
  }
  else{
      $AssignmentObjectsCount = ($AssignmentObjects | Measure-Object).count
      $AssignmentObjectsIndex = 1
      foreach($Object in $AssignmentObjects){
          $objectId = $object.instanceId | ConvertTo-Json
          Write-Host "======================================================================================================================================================================================================"
          Write-Output ("Assigning SLA "+ $object.slaid + " to Object " + $object.Name + " with object Id " + $objectId)
          Write-Host "======================================================================================================================================================================================================"
          Write-Host "API Output:"
          Set-mssqlSlasBatch -ObjectIds $objectId -slaId $object.slaId
          Write-Host ("Assigned SLA to object " + $AssignmentObjectsIndex + " of " + $AssignmentObjectsCount)
          $AssignmentObjectsIndex++
      }
      Write-Host "======================================================================================================================================================================================================"
      Write-Host ("Writing CSV file to "  + $Output_directory + "/AssignedSLAMSSQL-" + $mdate + ".csv")
      $AssignmentObjects | Export-Csv -NoTypeInformation ($Output_directory + "/AssignedSLAMSSQL-" +$mdate + ".csv")
      Write-Host "Disconnecting From Rubrik Security Cloud."
      disconnect-rsc
  }
}
