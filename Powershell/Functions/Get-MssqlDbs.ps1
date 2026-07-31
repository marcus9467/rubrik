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
