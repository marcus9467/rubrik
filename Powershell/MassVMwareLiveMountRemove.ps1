[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson
)
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

function Get-VMwareLiveMount{
    try{
        $query = "query vSphereMountQuery(`$first: Int, `$after: String, `$filter: [VsphereLiveMountFilterInput!], `$sortBy: VsphereLiveMountSortBy) {
  vSphereLiveMounts(first: `$first, after: `$after, filter: `$filter, sortBy: `$sortBy) {
    edges {
      cursor
      __typename
      node {
        __typename
        id
        isReady
        attachingDiskCount
        hasAttachingDisk
        migrateDatastoreRequestId
        sourceSnapshot {
          __typename
          date
          snappableNew {
            ... on VsphereVm {
              physicalPath {
                fid
                name
                objectType
                __typename
              }
              __typename
            }
            primaryClusterLocation {
              clusterUuid
              __typename
            }
            cluster {
              id
              __typename
            }
            __typename
          }
          cdmWorkloadSnapshot {
            subObjs {
              subObj {
                vmwareVmSubObj {
                  currentDatastoreId
                  deviceKey
                  fileSizeInBytes
                  filename
                  virtualDiskId
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        cluster {
          __typename
          id
          name
          status
          version
        }
        vCenter {
          isStandaloneHost
          id
          __typename
        }
        ...VsphereLiveMountTimeFragment
        ...VsphereLiveMountNameFragment
        ...VsphereLiveMountHostFragment
      }
    }
    pageInfo {
      startCursor
      endCursor
      hasPreviousPage
      hasNextPage
      __typename
    }
    __typename
  }
}

fragment VsphereLiveMountHostFragment on VsphereLiveMount {
  host {
    id
    name
    isStandaloneHost
    __typename
  }
  __typename
}

fragment VsphereLiveMountNameFragment on VsphereLiveMount {
  vmStatus
  newVmName
  sourceVm {
    __typename
    id
    name
  }
  hasAttachingDisk
  attachingDiskCount
  mountedVm {
    __typename
    name
  }
  __typename
}

fragment VsphereLiveMountTimeFragment on VsphereLiveMount {
  mountTimestamp
  cluster {
    id
    timezone
    __typename
  }
  __typename
}"
$variables = "{
  `"filter`": [
    {
      `"field`": `"ORG_ID`",
      `"texts`": []
    }
  ],
  `"sortBy`": {
    `"field`": `"MOUNT_NAME`",
    `"sortOrder`": `"ASC`"
  }
}"
$JSON_BODY = @{
    "variables" = $variables
    "query"     = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
$result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$VMLiveMounts = (((($result.content | ConvertFrom-Json).data).vSphereLiveMounts).edges).node
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $VMLiveMounts
    }
}

function Remove-VMwareLiveMount{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$MountId
    )
    try{
        $query = "mutation UnmountLiveMountMutation(`$livemountId: UUID!, `$force: Boolean) {
  vsphereVMDeleteLiveMount(livemountId: `$livemountId, force: `$force) {
    id
    status
    __typename
  }
}"

$variables = "{
  `"livemountId`": `"${MountId}`",
  `"force`": false
}"
$JSON_BODY = @{
    "variables" = $variables
    "query"     = $query
}
$JSON_BODY = $JSON_BODY | ConvertTo-Json
$result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
$VMUnmount = (($result.content | ConvertFrom-Json).data).vsphereVMDeleteLiveMount | ConvertTo-Json
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $VMUnmount
    }
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = connect-polaris
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

$MountsList = Get-VMwareLiveMount
ForEach($Mount in $MountsList){
    Remove-VMwareLiveMount -MountId $Mount.id
}

disconnect-polaris
