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
