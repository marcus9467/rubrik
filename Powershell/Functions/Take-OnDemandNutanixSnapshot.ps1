function Take-OnDemandNutanixSnapshot{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$nutanixVMId,
        [parameter(Mandatory=$true)]
        [string]$slaId
    )
    try{
        $query = "mutation NutanixAHVSnapshotMutation(`$input: CreateOnDemandNutanixBackupInput!) {
            createOnDemandNutanixBackup(input: `$input) {
              status
              __typename
            }
          }"
          
          $variables = "{
            `"input`": {
              `"config`": {
                `"slaId`": $slaId,
              },
              `"id`": $nutanixVMId,
              `"userNote`": `"`"
            }
          }"
          $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
      
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $result
    }
}
