<#

.SYNOPSIS
This script will pull the latest backup from a user supplied list of objects (VMware VM) via a CSV and output the data to a CSV. 

.EXAMPLE
./Get-LatestBackup.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -CSV "objectList.csv"

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : March 07, 2024
    Company : Rubrik Inc
#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$false)]
    [string]$ClusterId = "[]",
    [parameter(Mandatory=$false)]
    [switch]$CSV
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
function Get-SnapshotInfo{
  [CmdletBinding()]

  param (
      [parameter(Mandatory=$true)]
      [string]$snappableId
      #snappableId = FID, not ID
  )
  process {
      try{
          $snappableInfo = @()
          $variables = "{
              `"snappableId`": `"${snappableId}`",
              `"first`": 50,
              `"sortBy`": `"CREATION_TIME`",
              `"sortOrder`": `"DESC`",
              `"snapshotFilter`": [
                {
                  `"field`": `"SNAPSHOT_TYPE`",
                  `"typeFilters`": []
                },
                {
                  `"field`": `"IS_LEGALLY_HELD`",
                  `"text`": `"false`"
                }
              ],
              `"timeRange`": {
                  `"start`": `"${startDate}`",
                  `"end`": `"${currentDate}`"
                }
            }"

          $query = "query SnapshotsListSingleQuery(`$snappableId: String!, `$first: Int, `$after: String, `$snapshotFilter: [SnapshotQueryFilterInput!], `$sortBy: SnapshotQuerySortByField, `$sortOrder: SortOrder, `$timeRange: TimeRangeInput) {
              snapshotsListConnection: snapshotOfASnappableConnection(workloadId: `$snappableId, first: `$first, after: `$after, snapshotFilter: `$snapshotFilter, sortBy: `$sortBy, sortOrder: `$sortOrder, timeRange: `$timeRange) {
                edges {
                  cursor
                  node {
                    ...CdmSnapshotLatestUserNotesFragment
                    id
                    date
                    expirationDate
                    isOnDemandSnapshot
                    ... on CdmSnapshot {
                      cdmVersion
                      isRetentionLocked
                      isDownloadedSnapshot
                      cluster {
                        id
                        name
                        version
                        status
                        timezone
                      }
                      pendingSnapshotDeletion {
                        id: snapshotFid
                        status
                      }
                      slaDomain {
                        ...EffectiveSlaDomainFragment
                      }
                      pendingSla {
                        ...SLADomainFragment
                      }
                      snapshotRetentionInfo {
                        isCustomRetentionApplied
                        archivalInfos {
                          name
                          isExpirationDateCalculated
                          expirationTime
                          locationId
                        }
                        localInfo {
                          name
                          isExpirationDateCalculated
                          expirationTime
                        }
                        replicationInfos {
                          name
                          isExpirationDateCalculated
                          expirationTime
                          locationId
                          isExpirationInformationUnavailable
                        }
                      }
                      sapHanaAppMetadata {
                        backupId
                        backupPrefix
                        snapshotType
                        files {
                          backupFileSizeInBytes
                        }
                      }
                      legalHoldInfo {
                        shouldHoldInPlace
                      }
                    }
                    ... on PolarisSnapshot {
                      isDeletedFromSource
                      isDownloadedSnapshot
                      isReplica
                      isArchivalCopy
                      slaDomain {
                        name
                        ... on ClusterSlaDomain {
                          fid
                          cluster {
                            id
                            name
                          }
                        }
                        ... on GlobalSlaReply {
                          id
                        }
                      }
                    }
                  }
                }
                pageInfo {
                  endCursor
                  hasNextPage
                }
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

            fragment CdmSnapshotLatestUserNotesFragment on CdmSnapshot {
              latestUserNote {
                time
                userName
                userNote
              }
            }"
            $JSON_BODY = @{
              "variables" = $variables
              "query" = $query
          }
          $JSON_BODY = $JSON_BODY | ConvertTo-Json
          $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
          $snappableInfo += (((($result.content | ConvertFrom-Json).data).snapshotsListConnection).edges).node

          while ((((($result.content | ConvertFrom-Json).data).snapshotsListConnection).pageInfo).hasNextPage -eq $true){
              $endCursor = ((((($result.content) | ConvertFrom-Json).data).taskDetailConnection).pageinfo).endCursor
              Write-Host ("Paging through another 50 snapshots. Looking at End Cursor " + $endCursor)
              $variables = "{
                  `"snappableId`": `"${snappableId}`",
                  `"first`": 50,
                  `"sortBy`": `"CREATION_TIME`",
                  `"sortOrder`": `"DESC`",
                  `"snapshotFilter`": [
                    {
                      `"field`": `"SNAPSHOT_TYPE`",
                      `"typeFilters`": []
                    },
                    {
                      `"field`": `"IS_LEGALLY_HELD`",
                      `"text`": `"false`"
                    }
                  ],
                  `"timeRange`": {
                      `"start`": `"${startDate}`",
                      `"end`": `"${currentDate}`"
                    },
                  `"after`": `"${endCursor}`"
                }"
              $JSON_BODY = @{
                  "variables" = $variables
                  "query" = $query
              }
              $JSON_BODY = $JSON_BODY | ConvertTo-Json
              $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
              $snappableInfo += (((($result.content | ConvertFrom-Json).data).snapshotsListConnection).edges).node 
          }

      }

      Catch{
          Write-Error("Error $($_)")
      }
  }
  End{
      Write-Output $snappableInfo
  }

}
Function Get-DateRange{ 
  #Function taken from https://thesurlyadmin.com/2014/07/25/quick-script-date-ranges/  
  [CmdletBinding()]
  Param (
      [datetime]$Start = (Get-Date),
      [datetime]$End = (Get-Date)
  )
  
  ForEach ($Num in (0..((New-TimeSpan –Start $Start –End $End).Days)))
  {   $Start.AddDays($Num)
  }
}
Function Get-SnappableInfo{
  [CmdletBinding()]

  param (
      [parameter(Mandatory=$true)]
      [string]$snappableName

  )
  process{
    try{
      $query = "query VSphereVMsListQuery(`$first: Int!, `$after: String, `$filter: [Filter!]!, `$isMultitenancyEnabled: Boolean = false, `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isDuplicatedVmsIncluded: Boolean = true) {
        vSphereVmNewConnection(filter: `$filter, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
          edges {
            cursor
            node {
              id
              ...VSphereNameColumnFragment
              ...CdmClusterColumnFragment
              ...EffectiveSlaColumnFragment
              ...VSphereSlaAssignmentColumnFragment
              ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
              isRelic
              authorizedOperations
              primaryClusterLocation {
                id
                name
                __typename
              }
              logicalPath {
                fid
                name
                objectType
                __typename
              }
              slaPauseStatus
              snapshotDistribution {
                id
                totalCount
                __typename
              }
              reportWorkload {
                id
                archiveStorage
                physicalBytes
                __typename
              }
              vmwareToolsInstalled
              agentStatus {
                agentStatus
                __typename
              }
              duplicatedVms @include(if: `$isDuplicatedVmsIncluded) {
                fid
                cluster {
                  id
                  name
                  version
                  status
                  __typename
                }
                slaAssignment
                effectiveSlaDomain {
                  ... on GlobalSlaReply {
                    id
                    name
                    isRetentionLockedSla
                    retentionLockMode
                    description
                    __typename
                  }
                  ... on ClusterSlaDomain {
                    id
                    fid
                    name
                    isRetentionLockedSla
                    retentionLockMode
                    cluster {
                      id
                      name
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                snapshotDistribution {
                  id
                  totalCount
                  __typename
                }
                effectiveSlaSourceObject {
                  fid
                  objectType
                  name
                  __typename
                }
                reportWorkload {
                  archiveStorage
                  physicalBytes
                  id
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          pageInfo {
            startCursor
            endCursor
            hasNextPage
            hasPreviousPage
            __typename
          }
          __typename
        }
      }
      
      fragment VSphereNameColumnFragment on HierarchyObject {
        id
        name
        ...HierarchyObjectTypeFragment
        __typename
      }
      
      fragment HierarchyObjectTypeFragment on HierarchyObject {
        objectType
        __typename
      }
      
      fragment EffectiveSlaColumnFragment on HierarchyObject {
        id
        effectiveSlaDomain {
          ...EffectiveSlaDomainFragment
          ... on GlobalSlaReply {
            description
            __typename
          }
          __typename
        }
        ... on CdmHierarchyObject {
          pendingSla {
            ...SLADomainFragment
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
      
      fragment CdmClusterColumnFragment on CdmHierarchyObject {
        replicatedObjectCount
        cluster {
          id
          name
          version
          status
          __typename
        }
        __typename
      }
      
      fragment VSphereSlaAssignmentColumnFragment on HierarchyObject {
        effectiveSlaSourceObject {
          fid
          name
          objectType
          __typename
        }
        ...SlaAssignmentColumnFragment
        __typename
      }
      
      fragment SlaAssignmentColumnFragment on HierarchyObject {
        slaAssignment
        __typename
      }
      
      fragment OrganizationsColumnFragment on HierarchyObject {
        allOrgs {
          name
          __typename
        }
        __typename
      }"
      $variables = "{
        `"isMultitenancyEnabled`": true,
        `"isDuplicatedVmsIncluded`": true,
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
            `"field`": `"IS_ACTIVE`",
            `"texts`": [
              `"true`"
            ]
          },
          {
            `"field`": `"NAME`",
            `"texts`": [
              `"${snappableName}`"
            ]
          },
          {
            `"field`": `"IS_ACTIVE_AMONG_DUPLICATED_OBJECTS`",
            `"texts`": [
              `"true`"
            ]
          }
        ],
        `"sortBy`": `"NAME`",
        `"sortOrder`": `"ASC`"
      }"
      $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $snappableInfo += (((($result.content | convertFrom-Json).data).vSphereVmNewConnection).edges).node
    }
    catch{
      Write-Error("Error $($_)")
    }
  }

  End{
    Write-Output $snappableInfo
  }
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$Output_directory = (Get-Location).path
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

#Set Timeframe to scan based on $DaysToReport
$InFormat = "yyyy-MM-ddTHH:mm:ss.fffZ"
#Setting current date to n - 1 to account for backups that run in the evening, while the report runs earlier in the day. 
$currentDate = ((Get-Date).AddDays(-1)).ToString($InFormat)

$startDate = ($currentDate | Get-Date).AddDays("-3") | Get-Date -Format $InFormat

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$objectList = import-CSV $CSV
$backupList = @()
ForEach($object in $objectList){
    $snappableInfo = Get-SnappableInfo -snappableName $object.name
    $backupInfo = Get-SnapshotInfo -snappableId $snappableInfo.id | Select-Object -First 1
    $LatestBackup = New-Object PSobject
    $LatestBackup | Add-Member -NotePropertyName "objectName" -NotePropertyValue $object.name
    $LatestBackup | Add-Member -NotePropertyName "lastBackup" -NotePropertyValue $backupInfo.date
    $backupList += $LatestBackup
}
$backupList | Export-Csv -NoTypeInformation ($Output_directory + "/BackupList" +$mdate + ".csv")
disconnect-polaris
