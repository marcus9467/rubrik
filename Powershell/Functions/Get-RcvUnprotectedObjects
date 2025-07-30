function Get-RcvUnprotectedObjects {
    <#
    .SYNOPSIS
    Retrieves a list of "REMOTE_UNPROTECTED" objects for a specified cluster
    and exports their names and workload IDs to a CSV file.

    .DESCRIPTION
    This function leverages the GraphQL API to query for unmanaged objects
    with a status of "REMOTE_UNPROTECTED". It handles pagination to ensure
    all matching objects are retrieved and then extracts the object's name
    and its `recoveryInfo.oldWorkloadId`. The collected data is then
    exported to a CSV file for easy review and subsequent use with the
    `Refresh-RcvReaderTarget` function.

    .PARAMETER ServiceAccountJsonPath
    The full path to your service account JSON file. This file should contain
    'client_id', 'client_secret', and 'access_token_uri'. This is a mandatory parameter.

    .PARAMETER ClusterUuid
    The UUID of the cluster for which to retrieve unmanaged objects. This is a mandatory parameter.

    .PARAMETER OutputCsvPath
    The full path, including filename, for the CSV file to which the results will be exported.
    If not specified, the CSV will be saved in the current directory with a generated filename.

    .EXAMPLE
    # Example 1: Get unprotected objects for a cluster and save to a specific CSV
    Get-RcvUnprotectedObjects `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -ClusterUuid "deec0cb8-183b-4e4e-9460-753aa23e2cc8" `
        -OutputCsvPath "C:\Reports\UnprotectedObjects_MyCluster.csv"

    .EXAMPLE
    # Example 2: Get unprotected objects and save to default CSV name in current directory
    Get-RcvUnprotectedObjects `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -ClusterUuid "deec0cb8-183b-4e4e-9460-753aa23e2cc8"

    .NOTES
    The output CSV will contain 'ObjectName' and 'WorkloadId' columns.
    #>
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Low')]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServiceAccountJsonPath,

        [Parameter(Mandatory=$true)]
        [string]$ClusterUuid,

        [string]$OutputCsvPath = ""
    )

    $defaultCsvFileName = "RcvUnprotectedObjects_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
    if ([string]::IsNullOrEmpty($OutputCsvPath)) {
        $OutputCsvPath = Join-Path (Get-Location) $defaultCsvFileName
    }

    if ($PSCmdlet.ShouldProcess("Retrieving REMOTE_UNPROTECTED objects for cluster '$ClusterUuid' and saving to '$OutputCsvPath'", "Are you sure you want to proceed?")) {
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


            $query = @"
query SnapshotManagementGlobalObjectsQuery(`$input: UnmanagedObjectsInput!, `$after: String, `$first: Int) {
  unmanagedObjects(input: `$input, first: `$first, after: `$after) {
    edges {
      cursor
      node {
        name
        recoveryInfo {
          oldWorkloadId
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

            $allUnprotectedObjects = New-Object System.Collections.Generic.List[PSObject]
            $hasNextPage = $true
            $afterCursor = $null
            $pageSize = 50 

            Write-Verbose "Starting pagination for unmanaged objects..."

            do {
                $variables = @{
                    input = @{
                        clusterUuid = $ClusterUuid
                        sortParam = @{
                            sortOrder = "ASC"
                            type = "NAME"
                        }
                        retentionSlaDomainIds = @()
                        objectTypes = @()
                        unmanagedStatuses = @("REMOTE_UNPROTECTED")
                    }
                    first = $pageSize
                    after = $afterCursor
                }

                $body = @{
                    query = $query
                    variables = $variables
                } | ConvertTo-Json -Depth 10

                Write-Verbose "Fetching page with cursor: $($afterCursor -replace '[\r\n\t]', ' ')"

                $response = Invoke-RestMethod -Uri $graphqlUrl `
                                            -Method Post `
                                            -Headers $headers `
                                            -Body $body `
                                            -ContentType "application/json" `
                                            -ErrorAction Stop

                if ($response.errors) {
                    Write-Warning "GraphQL Errors encountered during pagination:"
                    $response.errors | ForEach-Object {
                        Write-Warning "  Message: $($_.message)"
                    }
                    $hasNextPage = $false 
                    break
                }

                $unmanagedObjectsConnection = $response.data.unmanagedObjects

                foreach ($edge in $unmanagedObjectsConnection.edges) {
                    $node = $edge.node
                    if ($node.name -and $node.recoveryInfo.oldWorkloadId) {
                        $allUnprotectedObjects.Add([PSCustomObject]@{
                            ObjectName = $node.name
                            WorkloadId = $node.recoveryInfo.oldWorkloadId 
                        })
                    }
                }

                $afterCursor = $unmanagedObjectsConnection.pageInfo.endCursor
                $hasNextPage = $unmanagedObjectsConnection.pageInfo.hasNextPage

            } while ($hasNextPage)

            Write-Verbose "Finished retrieving all pages. Total objects found: $($allUnprotectedObjects.Count)"

            if ($allUnprotectedObjects.Count -gt 0) {
                $allUnprotectedObjects | Export-Csv -Path $OutputCsvPath -NoTypeInformation -Encoding UTF8
                Write-Host "Successfully exported $($allUnprotectedObjects.Count) objects to '$OutputCsvPath'"
            } else {
                Write-Host "No REMOTE_UNPROTECTED objects found for cluster '$ClusterUuid'. No CSV file created."
            }

            Write-Output $allUnprotectedObjects
        }
        catch {
            Write-Error "An error occurred during API call or processing: $($_.Exception.Message)"
            Write-Error "Full Error Details: $_"
        }
    }
}
