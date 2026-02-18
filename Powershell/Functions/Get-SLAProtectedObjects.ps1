function Get-SLAProtectedObjects {
    param(
        [Parameter(Mandatory=$true)]
        [string]$SlaId
    )
    <#
    .SYNOPSIS
    Gathers all protected objects assigned to a specific SLA Domain ID.
    #>
    try {
        # Define the Query
        $query = "query ProtectedObjectListQuery(`$slaIds: [UUID!]!, `$first: Int, `$after: String, `$sortBy: ObjectQuerySortByParamInput, `$filter: GetProtectedObjectsFilterInput) {
          slaProtectedObjects(slaIds: `$slaIds, first: `$first, after: `$after, sortBy: `$sortBy, filter: `$filter) {
            edges {
              cursor
              node {
                id
                name
                objectType
                slaPauseStatus
                protectionStatus
                isPrimary
                cluster {
                  id
                  name
                  __typename
                }
                __typename
              }
              __typename
            }
            pageInfo {
              endCursor
              hasNextPage
              __typename
            }
            count
            __typename
          }
        }"

        # Define Initial Variables
        # Note: We interpolate the $SlaId into the array string
        $variables = "{
          `"first`": 50,
          `"slaIds`": [`"$SlaId`"],
          `"filter`": {
            `"showOnlyDirectlyAssignedObjects`": true,
            `"objectName`": `"`"
          },
          `"sortBy`": {
            `"sortOrder`": `"ASC`",
            `"field`": `"NAME`"
          }
        }"

        # Build Body
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }

        # Initialize Container
        $ObjectInfo = @()
        
        # First Call
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        
        # Parse Result
        $ObjectInfo += (((($result.content | convertFrom-Json).data).slaProtectedObjects).edges).node

        # Pagination Loop
        while ((((($result.content | convertFrom-Json).data).slaProtectedObjects).pageInfo).hasNextPage -eq $true) {
            $endCursor = (((($result.content | convertFrom-Json).data).slaProtectedObjects).pageInfo).endCursor
            
            Write-Host ("Looking at End Cursor " + $endCursor)
            
            # Rebuild Variables with 'after' cursor
            $variables = "{
              `"first`": 50,
              `"slaIds`": [`"$SlaId`"],
              `"after`": `"${endCursor}`",
              `"filter`": {
                `"showOnlyDirectlyAssignedObjects`": true,
                `"objectName`": `"`"
              },
              `"sortBy`": {
                `"sortOrder`": `"ASC`",
                `"field`": `"NAME`"
              }
            }"

            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }

            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            
            # Append Results
            $ObjectInfo += (((($result.content | convertFrom-Json).data).slaProtectedObjects).edges).node
        }
    }
    catch {
        Write-Error("Error $($_)")
    }
    finally {
        Write-Output $ObjectInfo
    }
}
