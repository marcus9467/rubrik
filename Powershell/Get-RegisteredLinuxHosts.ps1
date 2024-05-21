<#

.SYNOPSIS
This script will onboard new MSSQL hosts based on a provided CSV file and then later assign protection to MSSQL Databases. 

.EXAMPLE
./Get-RegisteredLinuxHosts.ps1 -ServiceAccountJson $serviceaccountJson 

This will generate a list of Linux/Unix hosts currently registered to RSC. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : May 21, 2024
    Company : Rubrik Inc

#>
[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson
)
function connect-rsc {

  # Function that uses the Polaris/RSC Service Account JSON and opens a new session, and returns the session temp token

  [CmdletBinding()]

  param (

      # Service account JSON file

  )

 

  begin {

      # Parse the JSON and build the connection string

      #$serviceAccountObj 

      $connectionData = [ordered]@{

          'client_id' = $serviceAccountObj.client_id

          'client_secret' = $serviceAccountObj.client_secret

      } | ConvertTo-Json

  }

 

  process {

      try{

          $polaris = Invoke-RestMethod -Method Post -uri $serviceAccountObj.access_token_uri -ContentType application/json -body $connectionData

      }

      catch [System.Management.Automation.ParameterBindingException]{

          Write-Error("The provided JSON has null or empty fields, try the command again with the correct file or redownload the service account JSON from Polaris")

      }

  }

 

  end {

          if($polaris.access_token){

              Write-Output $polaris

          } else {

              Write-Error("Unable to connect")

          }

         

      }

}
function disconnect-rsc {

  # Closes the session with the session token passed here

  [CmdletBinding()]

  param (
  )

 

  begin {



  }

 

  process {

      try{

          $closeStatus = $(Invoke-WebRequest -Method Delete -Headers $headers -ContentType "application/json; charset=utf-8" -Uri $logoutUrl).StatusCode

      }

      catch [System.Management.Automation.ParameterBindingException]{

          Write-Error("Failed to logout. Error $($_)")

      }

  }

 

  end {

          if({$closeStatus -eq 204}){

              Write-Output("Successfully logged out")

          } else {

              Write-Error("Error $($_)")

          }

      }

}
function Get-PhysicalLinuxHost{
  [CmdletBinding()]
  param ()
  try{
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"LINUX_HOST_ROOT`",
      `"first`": 50,
      `"filter`": [
        {
          `"field`": `"IS_RELIC`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_REPLICATED`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_KUPR_HOST`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"childFilter`": [
        {
          `"field`": `"IS_GHOST`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_RELIC`",
          `"texts`": [
            `"false`"
          ]
        }
      ]
    }"
    $query = "query PhysicalHostListQuery(`$hostRoot: HostRoot!, `$first: Int!, `$after: String, `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$filter: [Filter!]!, `$childFilter: [Filter!], `$isMultitenancyEnabled: Boolean = false) {
      physicalHosts(hostRoot: `$hostRoot, filter: `$filter, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
        edges {
          cursor
          node {
            id
            name
            isArchived
            descendantConnection(typeFilter: [LinuxFileset, WindowsFileset]) {
              edges {
                node {
                  id
                  name
                  objectType
                  __typename
                }
                __typename
              }
              __typename
            }
            authorizedOperations
            cluster {
              id
              name
              version
              status
              ...ClusterNodeConnectionFragment
              __typename
            }
            ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
            primaryClusterLocation {
              id
              __typename
            }
            effectiveSlaDomain {
              ...EffectiveSlaDomainFragment
              __typename
            }
            osType
            osName
            connectionStatus {
              connectivity
              timestampMillis
              __typename
            }
            isOracleHost
            oracleUserDetails {
              sysDbaUser
              queryUser
              __typename
            }
            ...PhysicalHostConnectionStatusColumnFragment
            physicalChildConnection(typeFilter: [LinuxFileset, WindowsFileset], filter: `$childFilter) {
              count
              edges {
                node {
                  id
                  name
                  objectType
                  slaPauseStatus
                  effectiveSlaDomain {
                    ...EffectiveSlaDomainFragment
                    __typename
                  }
                  pendingSla {
                    ...SLADomainFragment
                    __typename
                  }
                  ...LinuxFilesetListFragment
                  ...WindowsFilesetListFragment
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        pageInfo {
          endCursor
          startCursor
          hasNextPage
          hasPreviousPage
          __typename
        }
        __typename
      }
    }
    
    fragment OrganizationsColumnFragment on HierarchyObject {
      allOrgs {
        name
        __typename
      }
      __typename
    }
    
    fragment EffectiveSlaDomainFragment on SlaDomain {
      id
      name
      ... on GlobalSlaReply {
        isRetentionLockedSla
        retentionLockMode
        __typename
      }
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
          __typename
        }
        isRetentionLockedSla
        retentionLockMode
        __typename
      }
      __typename
    }
    
    fragment SLADomainFragment on SlaDomain {
      id
      name
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment ClusterNodeConnectionFragment on Cluster {
      clusterNodeConnection {
        nodes {
          id
          status
          ipAddress
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment PhysicalHostConnectionStatusColumnFragment on PhysicalHost {
      id
      authorizedOperations
      connectionStatus {
        connectivity
        __typename
      }
      __typename
    }
    
    fragment LinuxFilesetListFragment on LinuxFileset {
      isRelic
      excludes: pathExcluded
      includes: pathIncluded
      exceptions: pathExceptions
      isPassThrough
      replicatedObjects {
        cluster {
          id
          name
          __typename
        }
        __typename
      }
      __typename
    }
    
    fragment WindowsFilesetListFragment on WindowsFileset {
      isRelic
      excludes: pathExcluded
      includes: pathIncluded
      exceptions: pathExceptions
      isPassThrough
      replicatedObjects {
        cluster {
          id
          name
          __typename
        }
        __typename
      }
      __typename
    }"
    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
  }

  $LinuxHostInfo = @()
  $JSON_BODY = $JSON_BODY | ConvertTo-Json
  $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
  $LinuxHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node

  while ((((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).hasNextPage -eq $true){
  $endCursor = (((($result.content | convertFrom-Json).data).physicalHosts).pageInfo).endCursor
  Write-Host ("Looking at End Cursor " + $endCursor)
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"LINUX_HOST_ROOT`",
      `"first`": 50,
      `"filter`": [
        {
          `"field`": `"IS_RELIC`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_REPLICATED`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_KUPR_HOST`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"childFilter`": [
        {
          `"field`": `"IS_GHOST`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_RELIC`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"after`": `"${endCursor}`"
    }"

    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $LinuxHostInfo += (((($result.content | convertFrom-Json).data).physicalHosts).edges).node
  }
  }
  catch{
    Write-Error("Error $($_)")
  }
  finally{
    Write-Output $LinuxHostInfo
  }
}


$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = connect-rsc
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$Output_directory = (Get-Location).path
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

$LinuxHostList = Get-PhysicalLinuxHost

$LinuxHostDetails = @()
ForEach($LinHost in $LinuxHostList){
  Write-Host ("Gathering Information about host " + $LinHost.name)
  $clientInfo = New-Object psobject
  $clientInfo | Add-Member -NotePropertyName "Name" -NotePropertyValue $LinHost.name
  $clientInfo | Add-Member -NotePropertyName "Id" -NotePropertyValue $LinHost.id
  $clientInfo | Add-Member -NotePropertyName "osType" -NotePropertyValue $LinHost.osType
  $clientInfo | Add-Member -NotePropertyName "osName" -NotePropertyValue $LinHost.osName
  $clientInfo | Add-Member -NotePropertyName "clusterName" -NotePropertyValue ($LinHost.cluster).name
  $clientInfo | Add-Member -NotePropertyName "connectionStatus" -NotePropertyValue ($LinHost.connectionStatus).connectivity
  $LinuxHostDetails += $clientInfo
}
Write-Host ("Generating CSV at " + $Output_directory + "/LinuxHostDetails_" + $mdate + ".csv")
$LinuxHostDetails | Export-Csv -NoTypeInformation ($Output_directory + "/LinuxHostDetails_" + $mdate + ".csv")
Disconnect-Rsc
