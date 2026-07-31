function New-RcvReaderTarget {
    <#
    .SYNOPSIS
    Creates a new RCV Reader Target using a GraphQL mutation with service account authentication.

    .DESCRIPTION
    This function handles authentication via a service account JSON file, obtains an access token,
    and then sends a POST request to the GraphQL endpoint to create an RCS Reader Target.
    It constructs the GraphQL mutation query and the necessary variables based on the
    provided parameters.

    .PARAMETER ServiceAccountJsonPath
    The full path to your service account JSON file. This file should contain
    'client_id', 'client_secret', and 'access_token_uri'. This is a mandatory parameter.

    .PARAMETER ClusterUuid
    The UUID of the cluster to associate with the reader target. This is a mandatory parameter.

    .PARAMETER ReaderLocationName
    The desired name for the new reader location. This is a mandatory parameter.

    .PARAMETER RcsArchivalLocationName
    The name of the RCS archival location. This is a mandatory parameter.

    .PARAMETER ReaderRetrievalMethod
    The method for reader retrieval. Accepted values are "OBJECT_LIST_AND_DETAILS" or "OBJECT_LIST_ONLY".
    This is a mandatory parameter.

    .EXAMPLE
    # Example 1: Create a new RCV Reader Target using a service account JSON
    New-RcsReaderTarget `
        -ServiceAccountJsonPath "C:\Path\To\your-service-account.json" `
        -ClusterUuid "deec0cb8-183b-4e4e-9460-753aa23e2cc8" `
        -ReaderLocationName "MyNewReaderAuth" `
        -RcvArchivalLocationName "RR-RCV-Archive-vault-r-madison" `
        -ReaderRetrievalMethod "OBJECT_LIST_ONLY"

    .NOTES
    This function now internally handles fetching the access token using the provided
    service account JSON.
    #>
    [CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact='Medium')]
    param (
        [Parameter(Mandatory=$true)]
        [string]$ServiceAccountJsonPath,

        [Parameter(Mandatory=$true)]
        [string]$ClusterUuid,

        [Parameter(Mandatory=$true)]
        [string]$ReaderLocationName,

        [Parameter(Mandatory=$true)]
        [string]$RcvArchivalLocationName,

        [Parameter(Mandatory=$true)]
        [ValidateSet("OBJECT_LIST_AND_DETAILS", "OBJECT_LIST_ONLY")]
        [string]$ReaderRetrievalMethod
    )

    if ($PSCmdlet.ShouldProcess("Creating RCV Reader Target '$ReaderLocationName'", "Are you sure you want to create this RCV Reader Target?")) {
        try {
            # --- Authentication Logic ---
            Write-Verbose "Reading service account JSON from: $ServiceAccountJsonPath"
            $serviceAccountObj = Get-Content $ServiceAccountJsonPath | ConvertFrom-Json

            # Validate essential fields from service account JSON
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

            # Derive GraphQL URL from access token URI
            # Assuming the GraphQL endpoint is at the same base URL, but with '/graphql' path
            $graphqlUrl = ($serviceAccountObj.access_token_uri).Replace("client_token", "graphql")
            Write-Verbose "Derived GraphQL URL: $graphqlUrl"

            # Prepare headers for the GraphQL request
            $headers = @{
                'Content-Type'  = 'application/json'
                'Accept'        = 'application/json'
                'Authorization' = "Bearer $rubrikToken"
            }

            # --- GraphQL Mutation Logic ---
            # Define the GraphQL mutation query with expanded fields
            $query = @"
mutation CreateRCVReaderMutation(`$createRCVReaderRequest: CreateRcsReaderTargetInput!) {
  createRcsReaderTarget(input: `$createRCVReaderRequest) {
    id
    name
    clusterUuid
    readerLocationName
    rcsArchivalLocationName
    readerRetrievalMethod
    __typename
  }
}
"@

            # Define the variables as a PowerShell hashtable, which will be converted to JSON
            $variables = @{
                createRCVReaderRequest = @{
                    clusterUuid = $ClusterUuid
                    readerLocationName = $ReaderLocationName
                    rcsArchivalLocationName = $RcsArchivalLocationName
                    readerRetrievalMethod = $ReaderRetrievalMethod
                }
            }

            # Combine query and variables into the request body
            $body = @{
                query = $query
                variables = $variables
            } | ConvertTo-Json -Depth 10 # Use a sufficient depth for nested objects

            Write-Verbose "Sending GraphQL request to: $graphqlUrl"
            # Write-Verbose "Request Body: $($body | ConvertTo-Json -Depth 10)" # Uncomment for detailed debugging
            # Write-Verbose "Request Headers: $($headers | ConvertTo-Json -Depth 10)" # Uncomment for detailed debugging

            # Send the POST request
            $response = Invoke-RestMethod -Uri $graphqlUrl `
                                        -Method Post `
                                        -Headers $headers `
                                        -Body $body `
                                        -ContentType "application/json" `
                                        -ErrorAction Stop

            # Output the response
            # Using Select-Object -ExpandProperty data to directly access the data node
            # and then Format-List * to show all properties of the nested object.
            if ($response.data) {
                Write-Output ($response.data.createRcsReaderTarget | Format-List *)
            } else {
                Write-Output $response # Output raw response if no data node
            }


            # Check for GraphQL errors in the response
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
