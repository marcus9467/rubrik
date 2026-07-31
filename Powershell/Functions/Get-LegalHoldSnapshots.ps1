function Get-LegalHoldSnapshots {
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$true)]
      [string]$clusterId
  )
  process {
    try {
      # Initialize variables for pagination
      $hasNextPage = $true
      $afterCursor = $null
      $allResults = @()

      while ($hasNextPage) {
        $query = @"
        query SnapshotManagementLegalHoldObjectsQuery(`$input: SnappablesWithLegalHoldSnapshotsInput!, `$first: Int, `$after: String, `$last: Int, `$before: String) {
          snappablesWithLegalHoldSnapshotsSummary(input: `$input, first: `$first, after: `$after, last: `$last, before: `$before) {
            edges {
              cursor
              node {
                name
                id
                snapshotDetails{
                  snapshotTime
                }
                snappableType
                snapshotCount
                physicalLocation {
                  name
                  managedId
                  __typename
                }
                __typename
              }
              __typename
            }
            pageInfo {
              endCursor
              hasPreviousPage
              hasNextPage
              __typename
            }
            __typename
          }
        }
        "@
        
        $variables = @{
          "input" = @{
            "sortParam" = @{
              "sortOrder" = "ASC"
              "type" = "SNAPPABLE_NAME"
            }
            "filterParams" = @()
            "clusterUuid" = $clusterId
          }
          "first" = 50
          "after" = $afterCursor
        }
        $JSON_BODY = @{
          "variables" = $variables | ConvertTo-Json -Compress
          "query" = $query
        } | ConvertTo-Json -Compress
        
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $data = ($result.Content | ConvertFrom-Json).data.snappablesWithLegalHoldSnapshotsSummary
        
        # Append current page of results to allResults
        $allResults += $data.edges.node

        # Update pagination variables
        $hasNextPage = $data.pageInfo.hasNextPage
        $afterCursor = $data.pageInfo.endCursor
      }
      
      return $allResults
    }
    catch {
      Write-Error("Error $($_)")
    }
  }
}
