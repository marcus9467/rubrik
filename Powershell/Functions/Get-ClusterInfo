function Get-ClusterInfo{
    <#
    .SYNOPSIS
    Generates information specific to the CDM clusters found in the RSC environment. If read into a variable this function output can later be filtered to derive things like Cluster ID to Cluster name mapping. Does not require any inputs.
    
    CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    #>
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
          Write-Output $clusterInfo
      }
      catch{
          Write-Error("Error $($_)")
      }
      #End{        
      #  Write-Output $clusterList
      #}
  }
