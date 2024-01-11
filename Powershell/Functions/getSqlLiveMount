function getSqlLiveMount{
    process{
        Try{
            $query = "query MssqlDatabaseLiveMountListQuery(`$first: Int!, `$after: String, `$filters: [MssqlDatabaseLiveMountFilterInput!], `$sortBy: MssqlDatabaseLiveMountSortByInput) {
                mssqlDatabaseLiveMounts(after: `$after, first: `$first, filters: `$filters, sortBy: `$sortBy) {
                  edges {
                    cursor
                    node {
                      id: fid
                      creationDate
                      mountedDatabaseName
                      isReady
                      recoveryPoint
                      targetInstance {
                        name
                        logicalPath {
                          name
                          objectType
                        }
                      }
                      sourceDatabase {
                        id
                        name
                      }
                      cluster {
                        id
                        name
                        version
                        status
                        timezone
                      }
                    }
                  }
                  pageInfo {
                    startCursor
                    endCursor
                    hasNextPage
                    hasPreviousPage
                  }
                }
              }"
            $variables = "{
                `"first`": 50,
                `"filters`": [
                  {
                    `"field`": `"MOUNTED_DATABASE_NAME`",
                    `"texts`": null
                  },
                  {
                    `"field`": `"CLUSTER_UUID`",
                    `"texts`": null
                  },
                  {
                    `"field`": `"SOURCE_DATABASE_ID`",
                    `"texts`": null
                  },
                  {
                    `"field`": `"ORG_ID`",
                    `"texts`": []
                  }
                ],
                `"sortBy`": {
                  `"field`": `"MOUNTED_DATABASE_NAME`",
                  `"sortOrder`": `"ASC`"
                }
              }"
        
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $mountInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $mountInfo = (((($mountInfo.Content| ConvertFrom-Json).data).mssqlDatabaseLiveMounts).edges).node
        }
        Catch{
            Write-Error("Error $($_)")
        }
        End{
            Write-Output $mountInfo
        }
    }
}
