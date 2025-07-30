function Get-RSCLatestSnapshot {
    <#
    .SYNOPSIS
    Retrieves the most recent snapshots for a specific Rubrik workload.

    .DESCRIPTION
    This function authenticates using a service account JSON file and then queries
    the GraphQL API to find the most recent snapshots for a given workload ID.
    It sorts snapshots by creation time in descending order and requests the first 3 results.

    .PARAMETER ServiceAccountJsonPath
    The full path to your service account JSON file. This file should contain
    'client_id', 'client_secret', and 'access_token_uri'. This is a mandatory parameter.

    .PARAMETER WorkloadId
    The unique identifier (FID) of the workload for which to retrieve the latest snapshots.
    This is a mandatory parameter.

    .EXAMPLE
    # Example 1: Get the most recent 3 snapshots for a specific workload
    Get-RSCLatestSnapshot `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -WorkloadId "99a9618f-8fca-5192-b8e2-de188e4a9352"

    .NOTES
    This function will output the details of the most recent snapshots found as PowerShell objects.
    You can pipe this output to other cmdlets or functions.
    #>
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Low')]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServiceAccountJsonPath,

        [Parameter(Mandatory=$true)]
        [string]$WorkloadId
    )

    if ($PSCmdlet.ShouldProcess("Retrieving the latest snapshots for workload '$WorkloadId'", "Are you sure you want to proceed?")) {
        try {
            Write-Verbose "Reading service account JSON from: $ServiceAccountJsonPath"
            $serviceAccountObj = Get-Content $ServiceAccountJsonPath | ConvertFrom-Json

            if (-not $serviceAccountObj.client_id -or -not $serviceAccountObj.client_secret -or -not $serviceAccountObj.access_token_uri) {
                Write-Error "Service account JSON is missing 'client_id', 'client_secret', or 'access_token_uri'."
                return
            }

            $connectionData = @{
                'client_id' = $serviceAccountObj.client_id
                'client_secret' = $serviceAccountObj.client_secret
            } | ConvertTo-Json

            $accessTokenUri = $serviceAccountObj.access_token_uri
            Write-Verbose "Attempting to obtain access token from: $accessTokenUri"

            $polarisSession = Invoke-RestMethod -Method Post `
                                                -Uri $accessTokenUri `
                                                -ContentType "application/json" `
                                                -Body $connectionData `
                                                -ErrorAction Stop

            if (-not $polarisSession.access_token) {
                Write-Error "Failed to retrieve access token. Response: $($polarisSession | ConvertTo-Json -Depth 5)"
                return
            }

            $rubrikToken = $polarisSession.access_token
            Write-Verbose "Successfully obtained access token."

            $graphqlUrl = ($serviceAccountObj.access_token_uri).Replace("client_token", "graphql")
            Write-Verbose "Derived GraphQL URL: $graphqlUrl"

            $headers = @{
                'Content-Type'  = 'application/json'
                'Accept'        = 'application/json'
                'Authorization' = "Bearer $rubrikToken"
            }

            # Query to get the most recent snapshots by sorting descending and taking the first 3.
            $query = @"
query GetLatestSnapshotForWorkload(`$workloadId: String!) {
  snapshotOfASnappableConnection(
    workloadId: `$workloadId,
    first: 3, # Changed from 1 to 3
    sortBy: CREATION_TIME,
    sortOrder: DESC
  ) {
    nodes {
      id
      date
      isIndexed
      isOnDemandSnapshot
      isQuarantined
      isAnomaly
      isExpired
      expirationDate
      ...on CdmSnapshot {
        isRetentionLocked
        legalHoldInfo {
          shouldHoldInPlace
        }
        snapshotRetentionInfo {
          localInfo {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
          archivalInfos {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
          replicationInfos {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
        }
        fileCount
        consistencyLevel
      }
      ...on PolarisSnapshot {
        snapshotRetentionInfo {
          localInfo {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
          archivalInfos {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
          replicationInfos {
            isSnapshotPresent
            isExpirationDateCalculated
            expirationTime
          }
        }
        polarisConsistencyLevel: consistencyLevel
      }
      __typename
    }
    __typename
  }
}
"@

            $variables = @{
                workloadId = $WorkloadId
            }

            $body = @{
                query = $query
                variables = $variables
            } | ConvertTo-Json -Depth 10

            Write-Verbose "Sending GraphQL request to: $graphqlUrl"

            $response = Invoke-RestMethod -Uri $graphqlUrl `
                                        -Method Post `
                                        -Headers $headers `
                                        -Body $body `
                                        -ContentType "application/json" `
                                        -ErrorAction Stop

            if ($response.errors) {
                Write-Warning "GraphQL Errors encountered:"
                $response.errors | ForEach-Object {
                    Write-Warning "  Message: $($_.message)"
                    if ($_.locations) { Write-Warning "  Locations: $($_.locations | ConvertTo-Json -Compress)" }
                    if ($_.path) { Write-Warning "  Path: $($_.path)" }
                    if ($_.extensions) { Write-Warning "  Extensions: $($_.extensions | ConvertTo-Json -Compress)" }
                }
                return $null
            }

            $snapshots = $response.data.snapshotOfASnappableConnection.nodes

            if ($snapshots -and $snapshots.Count -gt 0) {
                Write-Host "Found $($snapshots.Count) latest snapshots for workload '$WorkloadId':"
                return $snapshots # Return the entire collection of snapshots
            } else {
                Write-Host "No snapshots found for workload '$WorkloadId'."
                return $null
            }
        }
        catch {
            Write-Error "An error occurred during API call or processing: $($_.Exception.Message)"
            Write-Error "Full Error Details: $_"
            return $null
        }
    }
    return $null
}
