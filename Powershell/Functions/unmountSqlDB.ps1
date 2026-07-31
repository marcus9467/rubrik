function unmountSqlDB{
    [CmdletBinding()]

    param (
        [parameter(Mandatory=$true)]
        [string]$mountId
    )
    process{
        try{
            $query = "mutation MssqlLiveMountUnmountMutation(`$input: DeleteMssqlLiveMountInput!) {
                deleteMssqlLiveMount(input: `$input) {
                  id
                  links {
                    href
                    rel
                    __typename
                  }
                  __typename
                }
              }"
            $variables = "{
                `"input`": {
                  `"id`": `"${mountId}`",
                  `"force`": false
                }
              }"
        
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $mountInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $mountInfo = ((($unmountStuff.Content | ConvertFrom-Json).data).deleteMssqlLiveMount).id
            Write-Output $mountInfo
        }
        catch{
            Write-Error("Error $($_)")
        }
        #End{
        #    Write-Output $mountInfo
        #}
    }
}
