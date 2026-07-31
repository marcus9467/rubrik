function Refresh-RcvReaderTarget {
    <#
    .SYNOPSIS
    Refreshes a Rubrik Cloud Vault (RCV) Reader Target, optionally for specific workloads.

    .DESCRIPTION
    This function sends a GraphQL mutation to refresh an RCV Reader Target.
    It supports refreshing either by `ArchiveReaderId` (which requires `clusterId`)
    or by `locationId`. It also allows specifying `ArchivalDataSourceIds` and/or
    `localDataSourceIds` for a partial refresh, ensuring that a full refresh
    is not performed if specific workload IDs are provided.

    .PARAMETER ServiceAccountJsonPath
    The full path to your service account JSON file. This file should contain
    'client_id', 'client_secret', and 'access_token_uri'. This is a mandatory parameter.

    .PARAMETER ArchiveReaderId
    Rubrik CDM ID of the reader archival location to be refreshed.
    This parameter is part of the 'ByExternalLocation' parameter set and requires 'ClusterId'.

    .PARAMETER ClusterId
    ID of the Rubrik cluster on which the reader archival location to be refreshed is created.
    This parameter is part of the 'ByExternalLocation' parameter set and requires 'ArchiveReaderId'.

    .PARAMETER LocationId
    ID of the reader archival location to be refreshed.
    This parameter is part of the 'ByLocationId' parameter set and cannot be used with 'ArchiveReaderId'.

    .PARAMETER ArchivalDataSourceIds
    A comma-separated string of workload IDs on the original Rubrik cluster to refresh.
    If provided, this implies a partial refresh. Default is an empty string (full refresh if no other IDs).

    .PARAMETER LocalDataSourceIds
    List of workload IDs on the reader Rubrik cluster to refresh.
    If provided, this implies a partial refresh. Default is an empty array (full refresh if no other IDs).

    .EXAMPLE
    # Example 1: Refresh a reader target using ArchiveReaderId and ClusterId for specific archival data sources
    Refresh-RcvReaderTarget `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -ArchiveReaderId "your-archive-reader-id-123" `
        -ClusterId "your-cluster-id-abc" `
        -ArchivalDataSourceIds "workload-id-1,workload-id-2"

    .EXAMPLE
    # Example 2: Refresh a reader target using LocationId for specific local data sources
    Refresh-RcvReaderTarget `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -LocationId "your-location-id-456" `
        -LocalDataSourceIds @("local-workload-id-a", "local-workload-id-b")

    .EXAMPLE
    # Example 3: Perform a full refresh using LocationId (no specific workload IDs)
    Refresh-RcvReaderTarget `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -LocationId "your-location-id-789"

    .NOTES
    Ensures that either ArchiveReaderId/ClusterId OR LocationId is provided, but not both.
    If ArchivalDataSourceIds or LocalDataSourceIds are provided, it performs a partial refresh.
    #>
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Medium', DefaultParameterSetName='ByExternalLocation')]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServiceAccountJsonPath,

        [Parameter(ParameterSetName='ByExternalLocation', Mandatory=$true)]
        [string]$ArchiveReaderId, # Renamed from ExternalLocationId

        [Parameter(ParameterSetName='ByExternalLocation', Mandatory=$true)]
        [string]$ClusterId,

        [Parameter(ParameterSetName='ByLocationId', Mandatory=$true)]
        [string]$LocationId,

        [string]$ArchivalDataSourceIds = "", 

        [string[]]$LocalDataSourceIds = @()
    )

    $refreshTargetName = $ArchiveReaderId + $LocationId 

    if ($PSCmdlet.ShouldProcess("Refreshing RCV Reader Target '$refreshTargetName'", "Are you sure you want to refresh this RCV Reader Target?")) {
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
mutation RefreshFromRemoteMutation(`$input: RefreshReaderTargetInput!) {
  refreshReaderTarget(input: `$input)
}
"@

            $processedArchivalDataSourceIds = @()
            if (-not [string]::IsNullOrEmpty($ArchivalDataSourceIds)) {
                $processedArchivalDataSourceIds = ($ArchivalDataSourceIds.Split(',') | ForEach-Object { $_.Trim() })
            }


            $refreshInputVariables = @{
                archivalDataSourceIds = $processedArchivalDataSourceIds 
                localDataSourceIds = $LocalDataSourceIds
            }

            if ($PSCmdlet.ParameterSetName -eq 'ByExternalLocation') {
                $refreshInputVariables.externalLocationId = $ArchiveReaderId 
                $refreshInputVariables.clusterId = $ClusterId
            } elseif ($PSCmdlet.ParameterSetName -eq 'ByLocationId') {
                $refreshInputVariables.locationId = $LocationId
            }

            $body = @{
                query = $query
                variables = @{
                    input = $refreshInputVariables
                }
            } | ConvertTo-Json -Depth 10

            Write-Verbose "Sending GraphQL refresh request to: $graphqlUrl"

            # Send the POST request
            $response = Invoke-RestMethod -Uri $graphqlUrl `
                                        -Method Post `
                                        -Headers $headers `
                                        -Body $body `
                                        -ContentType "application/json" `
                                        -ErrorAction Stop


            if ($response.data) {
                Write-Output "Refresh operation initiated. Response for 'refreshReaderTarget': $($response.data.refreshReaderTarget)"
            } else {
                Write-Output $response 
            }

            if ($response.errors) {
                Write-Warning "GraphQL Errors encountered:"
                $response.errors | ForEach-Object {
                    Write-Warning "  Message: $($_.message)"
                    if ($_.locations) { Write-Warning "  Locations: $($_.locations | ConvertTo-Json -Compress)" }
                    if ($_.path) { Write-Warning "  Path: $($_.path)" }
                    if ($_.extensions) { Write-Warning "  Extensions: $($_.extensions | ConvertTo-Json -Compress)" }
                }
            }
        }
        catch {
            Write-Error "An error occurred during API call or authentication: $($_.Exception.Message)"
            Write-Error "Full Error Details: $_"
        }
    }
}
