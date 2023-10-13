function Register-VM{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [string]$vmId
    )
    try{
        $query = "mutation RegisterRubrikBackupServiceMutation(`$input: VsphereVmRegisterAgentInput!) {
            vsphereVmRegisterAgent(input: `$input) {
              success
            }
          }"
        $variables = "{
            `"input`": {
              `"id`": `"${vmId}`"
            }
          }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $result = ($results.Content | ConvertFrom-Json).data
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $result
    }
}
