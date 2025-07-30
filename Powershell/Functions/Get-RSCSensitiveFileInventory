function Get-RSCSensitiveFileInventory {
    <#
    .SYNOPSIS
    Retrieves sensitive file inventory for a specific snapshot of a Rubrik workload.

    .DESCRIPTION
    This function authenticates using a service account JSON and queries the GraphQL API
    to get a detailed sensitive file inventory for a given snapshot and workload. It handles
    pagination to retrieve all results and allows for filtering and sorting.
    The collected file information is then output as PowerShell objects and can
    optionally be exported to a CSV file.

    .PARAMETER ServiceAccountJsonPath
    The full path to your service account JSON file. This is mandatory for authentication.

    .PARAMETER WorkloadId
    The unique identifier (snappableFid) of the workload. This is mandatory.

    .PARAMETER SnapshotId
    The unique identifier (snapshotFid) of the specific snapshot to get inventory from. This is mandatory.

    .PARAMETER OutputCsvPath
    Optional. The full path, including filename, for the CSV file to which the results will be exported.
    If not specified, the CSV will be saved in the current directory with a generated filename.

    .PARAMETER FileType
    Optional. Filters the file results by type.
    Accepted values: "HITS", "ALL", "NO_HITS", "OPEN_ACCESS", "STALE_FILES".
    Defaults to "HITS" as per the provided example.

    .PARAMETER SearchText
    Optional. Text to search for within the file inventory. Defaults to an empty string.

    .PARAMETER SortBy
    Optional. Sorts the file results by a specific field.
    Accepted values: "HITS", "NAME", "SIZE", "LAST_MODIFIED_TIME", "CREATION_TIME".
    Defaults to "HITS" as per the provided example.

    .PARAMETER SortOrder
    Optional. The order in which to sort the results.
    Accepted values: "ASC", "DESC". Defaults to "DESC" as per the provided example.

    .PARAMETER Timezone
    Optional. The timezone for time-related fields in the query.
    Defaults to "America/New_York" as per the provided example.

    .PARAMETER IncludeDocumentTypes
    Optional. A boolean indicating whether to include document types in the query results.
    Defaults to $false.

    .EXAMPLE
    # Example 1: Get sensitive file inventory for a snapshot with default filters (HITS, DESC)
    $serviceAccountJson = "C:\Path\To\your-service-account.json"
    $workloadId = "0bcb0d4e-fe6b-59d0-9a93-4c6c7491d997"
    $snapshotId = "09efc62b-8137-5ea1-8bab-6c74871f0da5"

    Get-RSCSensitiveFileInventory `
        -ServiceAccountJsonPath $serviceAccountJson `
        -WorkloadId $workloadId `
        -SnapshotId $snapshotId `
        -OutputCsvPath "C:\Reports\SensitiveFileInventory_Snapshot_$snapshotId.csv"

    .EXAMPLE
    # Example 2: Get all files (not just HITS) sorted by filename ascending
    $serviceAccountJson = "C:\Path\To\your-service-account.json"
    $workloadId = "0bcb0d4e-fe6b-59d0-9a93-4c6c7491d997"
    $snapshotId = "09efc62b-8137-5ea1-8bab-6c74871f0da5"

    Get-RSCSensitiveFileInventory `
        -ServiceAccountJsonPath $serviceAccountJson `
        -WorkloadId $workloadId `
        -SnapshotId $snapshotId `
        -FileType "ALL" `
        -SortBy "NAME" `
        -SortOrder "ASC"

    .NOTES
    The output CSV will include columns like 'NativePath', 'Filename', 'Size',
    'LastModifiedTime', 'TotalHits', and 'Violations'.
    #>
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Low')]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServiceAccountJsonPath,

        [Parameter(Mandatory=$true)]
        [string]$WorkloadId,

        [Parameter(Mandatory=$true)]
        [string]$SnapshotId,

        [string]$OutputCsvPath = "",

        [ValidateSet("HITS", "ALL", "NO_HITS", "OPEN_ACCESS", "STALE_FILES")]
        [string]$FileType = "HITS",

        [string]$SearchText = "",

        [ValidateSet("HITS", "NAME", "SIZE", "LAST_MODIFIED_TIME", "CREATION_TIME")]
        [string]$SortBy = "HITS",

        [ValidateSet("ASC", "DESC")]
        [string]$SortOrder = "DESC",

        [string]$Timezone = "America/New_York",

        [boolean]$IncludeDocumentTypes = $false
    )

    $defaultCsvFileName = "RSCSensitiveFileInventory_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
    if ([string]::IsNullOrEmpty($OutputCsvPath)) {
        $OutputCsvPath = Join-Path (Get-Location) $defaultCsvFileName
    }

    if ($PSCmdlet.ShouldProcess("Retrieving sensitive file inventory for snapshot '$SnapshotId' of workload '$WorkloadId' and saving to '$OutputCsvPath'", "Are you sure you want to proceed?")) {
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
query ObjectInventoryFilesQuery(`$first: Int!, `$after: String, `$snappableFid: String!, `$snapshotFid: String!, `$filters: ListFileResultFiltersInput, `$sort: FileResultSortInput, `$timezone: String!, `$includeDocumentTypes: Boolean = false) {
  policyObj(snappableFid: `$snappableFid, snapshotFid: `$snapshotFid) {
    id: snapshotFid
    fileResultConnection(first: `$first, after: `$after, filter: `$filters, sort: `$sort, timezone: `$timezone) {
      edges {
        cursor
        node {
          ...DiscoveryFileFragment
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
      hasLatestData
      __typename
    }
    __typename
  }
}

fragment DiscoveryFileFragment on FileResult {
  nativePath
  stdPath
  filename
  mode
  size
  lastAccessTime
  lastModifiedTime
  creationTime
  lastScanTime
  directory
  createdBy
  modifiedBy
  numDescendantFiles
  numDescendantErrorFiles
  numDescendantSkippedExtFiles
  numDescendantSkippedSizeFiles
  errorCode
  hits {
    totalHits
    violations
    violationsDelta
    totalHitsDelta
    __typename
  }
  filesWithHits {
    totalHits
    violations
    __typename
  }
  openAccessFilesWithHits {
    totalHits
    violations
    __typename
  }
  staleFilesWithHits {
    totalHits
    violations
    __typename
  }
  analyzerGroupResults {
    ...AnalyzerGroupResultFragment
    __typename
  }
  sensitiveFiles {
    ...SensitiveFilesTableCellFragment
    __typename
  }
  sensitiveHits {
    highRiskHits {
      totalHits
      violatedHits
      __typename
    }
    mediumRiskHits {
      totalHits
      violatedHits
      __typename
    }
    lowRiskHits {
      totalHits
      violatedHits
      __typename
    }
    noRiskHits {
      totalHits
      violatedHits
      __typename
    }
    __typename
  }
  analyzerRiskHits {
    highRiskHits {
      totalHits
      violatedHits
      __typename
    }
    mediumRiskHits {
      totalHits
      violatedHits
      __typename
    }
    lowRiskHits {
      totalHits
      violatedHits
      __typename
    }
    noRiskHits {
      totalHits
      violatedHits
      __typename
    }
    __typename
  }
  analyzerResults {
    hits {
      totalHits
      violations
      __typename
    }
    analyzer {
      id
      name
      analyzerType
      __typename
    }
    __typename
  }
  openAccessType
  stalenessType
  numActivities
  numActivitiesDelta
  exposureSummary {
    exposureType
    fileCount {
      totalCount
      violatedCount
      __typename
    }
    __typename
  }
  dbEntityType
  mipLabelsSummary {
    mipLabel {
      hasProtection
      labelId
      labelName
      siteId
      __typename
    }
    filesCount {
      totalCount
      violatedCount
      __typename
    }
    __typename
  }
  documentTypesSummary @include(if: `$includeDocumentTypes) {
    id
    name
    filesCount {
      totalCount
      violatedCount
      __typename
    }
    __typename
  }
  __typename
}

fragment SensitiveFilesTableCellFragment on SensitiveFiles {
  highRiskFileCount {
    ...SummaryCountFragment
    __typename
  }
  mediumRiskFileCount {
    ...SummaryCountFragment
    __typename
  }
  lowRiskFileCount {
    ...SummaryCountFragment
    __typename
  }
  noRiskFileCount {
    ...SummaryCountFragment
    __typename
  }
  totalFileCount {
    ...SummaryCountFragment
    __typename
  }
  __typename
}

fragment SummaryCountFragment on SummaryCount {
  totalCount
  violatedCount
  __typename

}

fragment AnalyzerGroupResultFragment on AnalyzerGroupResult {
  analyzerGroup {
    groupType
    id
    name
    __typename
  }
  analyzerResults {
    hits {
      totalHits
      violations
      __typename
    }
    analyzer {
      id
      name
      analyzerType
      __typename
    }
    __typename
  }
  hits {
    totalHits
    violations
    violationsDelta
    totalHitsDelta
    __typename
  }
  __typename
}
"@
            $allFiles = New-Object System.Collections.Generic.List[PSObject]
            $hasNextPage = $true
            $afterCursor = $null
            $pageSize = 25

            Write-Verbose "Starting pagination for file inventory..."

            do {
                $filtersInput = @{
                    fileType = $FileType
                    searchText = $SearchText
                    snappablePaths = @(
                        @{
                            snappableFid = $WorkloadId
                            stdPath = ""
                        }
                    )
                    whitelistEnabled = $true
                    riskLevelTypesFilter = @()
                    documentTypesFilter = @()
                    exposureFilter = @()
                    mipLabelsFilter = @()
                }

                $sortInput = @{
                    dataTypeId = ""
                    sortBy = $SortBy
                    sortOrder = $SortOrder
                }

                $variables = @{
                    snappableFid = $WorkloadId
                    snapshotFid = $SnapshotId
                    first = $pageSize
                    after = $afterCursor
                    filters = $filtersInput
                    sort = $sortInput
                    timezone = $Timezone
                    includeDocumentTypes = $IncludeDocumentTypes
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
                        if ($_.locations) { Write-Warning "  Locations: $($_.locations | ConvertTo-Json -Compress)" }
                        if ($_.path) { Write-Warning "  Path: $($_.path)" }
                        if ($_.extensions) { Write-Warning "  Extensions: $($_.extensions | ConvertTo-Json -Compress)" }
                    }
                    $hasNextPage = $false
                    break
                }

                $fileConnection = $response.data.policyObj.fileResultConnection

                foreach ($edge in $fileConnection.edges) {
                    $node = $edge.node
                    $totalHits = $null
                    $violations = $null # Initialize violations variable

                    if ($node.hits -and $node.hits.totalHits -ne $null) {
                        $totalHits = $node.hits.totalHits
                    }
                    if ($node.hits -and $node.hits.violations -ne $null) {
                        $violations = $node.hits.violations
                    }

                    $allFiles.Add([PSCustomObject]@{
                        NativePath = $node.nativePath
                        Filename = $node.filename
                        Size = $node.size
                        LastModifiedTime = $node.lastModifiedTime
                        TotalHits = $totalHits
                        Violations = $violations # Added Violations property
                    })
                }

                $afterCursor = $fileConnection.pageInfo.endCursor
                $hasNextPage = $fileConnection.pageInfo.hasNextPage

            } while ($hasNextPage)

            Write-Verbose "Finished retrieving all pages. Total files found: $($allFiles.Count)"

            if ($allFiles.Count -gt 0) {
                $allFiles | Export-Csv -Path $OutputCsvPath -NoTypeInformation -Encoding UTF8
                Write-Host "Successfully exported $($allFiles.Count) files to '$OutputCsvPath'"
            } else {
                Write-Host "No files found for snapshot '$SnapshotId' of workload '$WorkloadId' with the specified filters. No CSV file created."
            }

            return $allFiles
        }
        catch {
            Write-Error "An error occurred during API call or processing: $($_.Exception.Message)"
            Write-Error "Full Error Details: $_"
            return $null
        }
    }
    return $null
}
