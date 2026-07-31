function liveMountSqlDB{

    [CmdletBinding()]

    param (
        [parameter(Mandatory=$true)]
        [string]$databaseId,
        [parameter(Mandatory=$true)]
        [string]$targetInstanceId,
        [parameter(Mandatory=$true)]
        [string]$recoveryPoint,
        [parameter(Mandatory=$true)]
        [string]$targetDatabaseName
    )
    
    $variables = "{
        `"input`": {
          `"id`": `"${databaseId}`",
          `"config`": {
            `"recoveryPoint`": {
              `"date`": `"${recoveryPoint}`"
            },
            `"targetInstanceId`": `"${recoveryPoint}`",
            `"mountedDatabaseName`": `"${targetDatabaseName}`"
          }
        }
      }"
    $query = "mutation MssqlDatabaseMountMutation(`$input: CreateMssqlLiveMountInput!) {
        createMssqlLiveMount(input: `$input) {
          id
          links {
            href
            rel
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
    $mountInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $mountInfo = (((($mountInfo.Content| ConvertFrom-Json).data).mssqlDatabaseLiveMounts).edges).node
}
