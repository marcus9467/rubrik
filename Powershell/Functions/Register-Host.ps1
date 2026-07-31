function Register-Host{
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$true)]
      [string]$clusterId,
      [parameter(Mandatory=$true)]
      [string]$clientName
  )
  try{
    $objectCount = ($clientName.split(",") | measure-object).count
    if($objectCount -gt 1){
        $clientName = $clientName.split(",")
        $clientArray = @()
        ForEach($Object in $clientName){
          $formattedClient = @{"hostname" = $Object}
          $clientArray += $formattedClient
        }
        $clientArray = $clientArray | ConvertTo-Json
        $variables = "{
          `"clusterUuid`": `"$clusterId`",
          `"hosts`": $clientArray
        }"
    }
    if($objectCount -eq 1){
      $variables = "{
        `"clusterUuid`": `"$clusterId`",
        `"hosts`": [
          {
            `"hostname`": `"$clientName`"
          }
        ]
      }"
    }
    $query = "mutation AddPhysicalHostMutation(`$clusterUuid: String!, `$hosts: [HostRegisterInput!]!) {
      bulkRegisterHost(input: {clusterUuid: `$clusterUuid, hosts: `$hosts}) {
        data {
          hostSummary {
            id
            __typename
          }
          __typename
        }
        __typename
      }
    }"

    $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $jobStatus = (((($result.content | convertFrom-Json).data).bulkRegisterHost).data).hostSummary
    $jobErrors = (($result.content | convertFrom-Json).errors).message
  }
  catch{
    Write-Error("Error $($_)")
  }
  finally{
    Write-Output $jobStatus
    Write-Output $jobErrors
  }
}
