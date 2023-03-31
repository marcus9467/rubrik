<#
.SYNOPSIS
This script will poll the RSC instance for either Linux or Windows hosts. 

.EXAMPLE
./Get-HostInfo.ps1 -ServiceAccountJson ServiceAccountJson.json -Windows -Linux

This will pull host information for both Windows and Linux hosts. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : March 30, 2023
    Company : Rubrik Inc


#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$false)]
    [switch]$Windows,
    [parameter(Mandatory=$false)]
    [switch]$Linux
)



##################################

# Adding certificate exception to prevent API errors

##################################
if ($IsWindows -eq $true){
  add-type @"

    using System.Net;

    using System.Security.Cryptography.X509Certificates;

    public class TrustAllCertsPolicy : ICertificatePolicy {

        public bool CheckValidationResult(

            ServicePoint srvPoint, X509Certificate certificate,

            WebRequest request, int certificateProblem) {

            return true;

        }

    }

"@

  [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

}

if($IsMacOS -eq $true){
  #Do Nothing for now
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$Output_directory = (Get-Location).path
$mdate = (Get-Date).tostring("yyyyMMddHHmm")
function connect-polaris {

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
function disconnect-polaris {

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

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")
function get-LinuxHost{
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
          descendantConnection {
            edges {
              node {
                id
                name
                objectType
              }
            }
          }
          authorizedOperations
          cluster {
            id
            name
            version
            status
            ...ClusterNodeConnectionFragment
          }
          ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
          primaryClusterLocation {
            id
          }
          effectiveSlaDomain {
            ...EffectiveSlaDomainFragment
          }
          osType
          osName
          connectionStatus {
            connectivity
            timestampMillis
          }
          isOracleHost
          oracleUserDetails {
            sysDbaUser
            queryUser
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
                }
                pendingSla {
                  ...SLADomainFragment
                }
                ...LinuxFilesetListFragment
                ...WindowsFilesetListFragment
              }
            }
          }
        }
      }
      pageInfo {
        endCursor
        startCursor
        hasNextPage
        hasPreviousPage
      }
    }
  }
  
  fragment OrganizationsColumnFragment on HierarchyObject {
    allOrgs {
      name
    }
  }
  
  fragment EffectiveSlaDomainFragment on SlaDomain {
    id
    name
    ... on GlobalSlaReply {
      isRetentionLockedSla
    }
    ... on ClusterSlaDomain {
      fid
      cluster {
        id
        name
      }
      isRetentionLockedSla
    }
  }
  
  fragment SLADomainFragment on SlaDomain {
    id
    name
    ... on ClusterSlaDomain {
      fid
      cluster {
        id
        name
      }
    }
  }
  
  fragment ClusterNodeConnectionFragment on Cluster {
    clusterNodeConnection {
      nodes {
        id
        status
        ipAddress
      }
    }
  }
  
  fragment PhysicalHostConnectionStatusColumnFragment on PhysicalHost {
    id
    authorizedOperations
    connectionStatus {
      connectivity
    }
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
      }
    }
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
      }
    }
  }"
  $JSON_BODY = @{
    "variables" = $variables
    "query" = $query
  }
  $JSON_BODY = $JSON_BODY | ConvertTo-Json
  $HostInfo = @()
  $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
  $HostInfo += (((($info.content | ConvertFrom-Json).data).physicalHosts).edges).node
  while ((((($info.content |ConvertFrom-Json).data).physicalHosts).pageInfo).hasNextPage -eq $true){
    $endCursor = (((($info.content | ConvertFrom-Json).data).physicalHosts).pageInfo).endCursor
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
      `"after`": `"${endCursor}`",
      `"childFilter`": [
        {
          `"field`": `"IS_GHOST`",
          `"texts`": [
            `"false`"
          ]
        }
      ]
    }"
    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $HostInfo += (((($info.content | ConvertFrom-Json).data).physicalHosts).edges).node
  }
  $HostInfo
}
function get-WindowsHost{
    $variables = "{
      `"isMultitenancyEnabled`": true,
      `"hostRoot`": `"WINDOWS_HOST_ROOT`",
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
            descendantConnection {
              edges {
                node {
                  id
                  name
                  objectType
                }
              }
            }
            authorizedOperations
            cluster {
              id
              name
              version
              status
              ...ClusterNodeConnectionFragment
            }
            ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
            primaryClusterLocation {
              id
            }
            effectiveSlaDomain {
              ...EffectiveSlaDomainFragment
            }
            osType
            osName
            connectionStatus {
              connectivity
              timestampMillis
            }
            isOracleHost
            oracleUserDetails {
              sysDbaUser
              queryUser
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
                  }
                  pendingSla {
                    ...SLADomainFragment
                  }
                  ...LinuxFilesetListFragment
                  ...WindowsFilesetListFragment
                }
              }
            }
          }
        }
        pageInfo {
          endCursor
          startCursor
          hasNextPage
          hasPreviousPage
        }
      }
    }
    
    fragment OrganizationsColumnFragment on HierarchyObject {
      allOrgs {
        name
      }
    }
    
    fragment EffectiveSlaDomainFragment on SlaDomain {
      id
      name
      ... on GlobalSlaReply {
        isRetentionLockedSla
      }
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
        }
        isRetentionLockedSla
      }
    }
    
    fragment SLADomainFragment on SlaDomain {
      id
      name
      ... on ClusterSlaDomain {
        fid
        cluster {
          id
          name
        }
      }
    }
    
    fragment ClusterNodeConnectionFragment on Cluster {
      clusterNodeConnection {
        nodes {
          id
          status
          ipAddress
        }
      }
    }
    
    fragment PhysicalHostConnectionStatusColumnFragment on PhysicalHost {
      id
      authorizedOperations
      connectionStatus {
        connectivity
      }
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
        }
      }
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
        }
      }
    }"
    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $HostInfo = @()
    $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $HostInfo += (((($info.content | ConvertFrom-Json).data).physicalHosts).edges).node
    while ((((($info.content |ConvertFrom-Json).data).physicalHosts).pageInfo).hasNextPage -eq $true){
      $endCursor = (((($info.content | ConvertFrom-Json).data).physicalHosts).pageInfo).endCursor
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
        `"after`": `"${endCursor}`",
        `"childFilter`": [
          {
            `"field`": `"IS_GHOST`",
            `"texts`": [
              `"false`"
            ]
          }
        ]
      }"
      $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
      }
      $JSON_BODY = $JSON_BODY | ConvertTo-Json
      $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
      $HostInfo += (((($info.content | ConvertFrom-Json).data).physicalHosts).edges).node
    }
    $HostInfo
  }

if($Windows){
    $windowsInfo = get-WindowsHost
}  
if($Linux){
    $linuxInfo = get-LinuxHost
}


disconnect-polaris