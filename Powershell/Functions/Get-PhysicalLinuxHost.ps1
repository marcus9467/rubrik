function Get-PhysicalLinuxHost{
  [CmdletBinding()]
  param ()
  try{
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"LINUX_HOST_ROOT`",
      `"first`": 50,
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

  $LinuxHostInfo = @()
  $JSON_BODY = $JSON_BODY | ConvertTo-Json
  $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
  $LinuxHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node

  while ((((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).hasNextPage -eq $true){
  $endCursor = (((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).endCursor
  Write-Host ("Looking at End Cursor " + $endCursor)
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"LINUX_HOST_ROOT`",
      `"first`": 50,
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
    $LinuxHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node
  }
  }
  catch{
    Write-Error("Error $($_)")
  }
  finally{
    Write-Output $LinuxHostInfo
  }
}
