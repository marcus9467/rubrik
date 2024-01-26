<#

.SYNOPSIS
This script will browse a supplied fileset and provide a list of backed up files. It will also produce a CSV to capture the output for later review.

.EXAMPLE
./FilesetBackupList.ps1 -ServiceAccountJson $serviceAccountJson -snappableId "b8f52f04-74ab-5806-805e-38f0be250999"

This will generate a list of files contained in the latest backup for the supplied snappable.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : January 25, 2024
    Company : Rubrik Inc
#>


[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$snappableId
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
function Get-FilesetInfo{
    [CmdletBinding()]

    param (
        [parameter(Mandatory=$true)]
        [string]$snappableId
        #snappableId = FID, not ID
    )
    process {
        try{
            $query = "query FilesetObjectDetailQuery(`$id: UUID!) {
                hierarchyObject(fid: `$id) {
                  __typename
                  id
                  name
                  objectType
                  ...LinuxFilesetObjectDetailFragment
                  ...WindowsFilesetObjectDetailFragment
                  ... on ShareFileset {
                    isRelic
                    host {
                      name
                      id
                      __typename
                    }
                    cluster {
                      id
                      name
                      __typename
                    }
                    __typename
                  }
                }
              }
              
              fragment LinuxFilesetObjectDetailFragment on LinuxFileset {
                filesetTemplate {
                  name
                  id
                  includes
                  excludes
                  exceptions
                  postBackupScript
                  preBackupScript
                  isArrayEnabled
                  backupScriptErrorHandling
                  allowBackupNetworkMounts
                  allowBackupHiddenFoldersInNetworkMounts
                  __typename
                }
                reportWorkload {
                  id
                  localEffectiveStorage
                  physicalBytes
                  archiveStorage
                  dataReduction
                  __typename
                }
                id
                name
                isRelic
                authorizedOperations
                host {
                  name
                  id
                  __typename
                }
                onDemandSnapshotCount
                isPassThrough
                primaryClusterLocation {
                  id
                  __typename
                }
                newestSnapshot {
                  id
                  isIndexed
                  date
                  archivalLocations {
                    id
                    name
                    __typename
                  }
                  replicationLocations {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                oldestSnapshot {
                  id
                  isIndexed
                  date
                  archivalLocations {
                    id
                    name
                    __typename
                  }
                  replicationLocations {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                missedSnapshotConnection {
                  nodes {
                    date
                    __typename
                  }
                  __typename
                }
                snapshotConnection {
                  count
                  __typename
                }
                effectiveSlaDomain {
                  ...EffectiveSlaDomainFragment
                  __typename
                }
                replicatedObjects {
                  cluster {
                    id
                    name
                    __typename
                  }
                  primaryClusterLocation {
                    id
                    name
                    __typename
                  }
                  id
                  ... on LinuxFileset {
                    host {
                      id
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                cluster {
                  id
                  name
                  status
                  timezone
                  version
                  __typename
                }
                failoverClusterApp {
                  hostFailoverCluster {
                    name
                    id
                    __typename
                  }
                  failoverClusterId
                  name
                  id
                  __typename
                }
                pendingSla {
                  ...SLADomainFragment
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
              
              fragment WindowsFilesetObjectDetailFragment on WindowsFileset {
                filesetTemplate {
                  name
                  id
                  includes
                  excludes
                  exceptions
                  postBackupScript
                  preBackupScript
                  isArrayEnabled
                  backupScriptErrorHandling
                  allowBackupNetworkMounts
                  allowBackupHiddenFoldersInNetworkMounts
                  __typename
                }
                reportWorkload {
                  id
                  localEffectiveStorage
                  physicalBytes
                  archiveStorage
                  dataReduction
                  __typename
                }
                id
                name
                isRelic
                authorizedOperations
                host {
                  name
                  id
                  __typename
                }
                onDemandSnapshotCount
                isPassThrough
                primaryClusterLocation {
                  id
                  __typename
                }
                newestSnapshot {
                  id
                  isIndexed
                  date
                  archivalLocations {
                    id
                    name
                    __typename
                  }
                  replicationLocations {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                oldestSnapshot {
                  id
                  isIndexed
                  date
                  archivalLocations {
                    id
                    name
                    __typename
                  }
                  replicationLocations {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                missedSnapshotConnection {
                  nodes {
                    date
                    __typename
                  }
                  __typename
                }
                snapshotConnection {
                  count
                  __typename
                }
                effectiveSlaDomain {
                  ...EffectiveSlaDomainFragment
                  __typename
                }
                replicatedObjects {
                  cluster {
                    id
                    name
                    __typename
                  }
                  primaryClusterLocation {
                    id
                    name
                    __typename
                  }
                  id
                  ... on WindowsFileset {
                    host {
                      id
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                cluster {
                  id
                  name
                  status
                  timezone
                  version
                  __typename
                }
                failoverClusterApp {
                  hostFailoverCluster {
                    name
                    id
                    __typename
                  }
                  failoverClusterId
                  name
                  id
                  __typename
                }
                pendingSla {
                  ...SLADomainFragment
                  __typename
                }
                __typename
              }"

            $variables = "{
                `"id`": `"${snappableId}`"
              }"
              $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $snappableInfo = (($result.Content | convertFrom-Json).data).hierarchyObject

        }

        Catch{
            Write-Error("Error $($_)")
        }
    }
    End{
        Write-Output $snappableInfo
    }

}
function Get-FilePath{
    [CmdletBinding()]

    param (
        [parameter(Mandatory=$true)]
        [string]$snapshotId,
        [parameter(Mandatory=$true)]
        [string]$browsePath
    )
    process {
        try{
            $query = "query BrowseSnapshotQuery(`$after: String, `$first: Int, `$path: String!, `$searchPrefix: String, `$snapshotFid: UUID!) {
                browseSnapshotFileConnection(path: `$path, searchPrefix: `$searchPrefix, snapshotFid: `$snapshotFid, first: `$first, after: `$after) {
                  edges {
                    cursor
                    node {
                      absolutePath
                      displayPath
                      path
                      filename
                      fileMode
                      size
                      lastModified
                      quarantineInfo {
                        isQuarantined
                        containsQuarantinedFiles
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  count
                  pageInfo {
                    endCursor
                    hasNextPage
                    hasPreviousPage
                    __typename
                  }
                  __typename
                }
              }"
            $variables = "{
                `"snapshotFid`": `"${snapshotId}`",
                `"path`": `"${browsePath}`",
                `"first`": 50 
              }"
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $items = (((($result.Content | convertFrom-Json).data).browseSnapshotFileConnection).edges).node

            # Initialize a List[object] instead of an array
            $fileList = New-Object 'System.Collections.Generic.List[object]'

            foreach ($item in $items) {
                $type = if ($item.fileMode -eq 'DIRECTORY') { 'DIRECTORY' } else { 'FILE' }
                $fileInfo = [PSCustomObject]@{
                    Path = $item.absolutePath
                    filename = $item.filename
                    LastModified = $item.lastModified
                    size = $item.size
                    Type = $type
                }
                # Add the fileInfo to the list
                $fileList.Add($fileInfo)

                # If the item is a directory, recurse into it
                if ($type -eq 'DIRECTORY') {
                    Write-Host ("Processing directory " + $item.absolutePath)
                    $subFiles = Get-FilePath -snapshotId $snapshotId -browsePath $item.absolutePath
                    
                    # Add the results of the recursion to the list
                    foreach ($subFile in $subFiles) {
                        $fileList.Add($subFile)
                    }
                }
            }

            # Return the list
            return $fileList
        }
        catch {
            Write-Error("Error $($_)")
        }
    }
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")


$objectInfo = Get-FilesetInfo $snappableId
$filesetHostName = $objectInfo.host.name
#$filesetName = $objectInfo.name

$latestSnapshot = ($objectInfo.newestSnapshot).id

$fileSetList = Get-FilePath -snapshotId $latestSnapshot -browsePath "/"
Write-Host ("Exporting file list to CSV FileList-" + $filesetHostName + "-" + $mdate +".csv")
$fileSetList | Export-Csv -NoTypeInformation ("FileList-" + $filesetHostName + "-" + $mdate +".csv")

disconnect-polaris
