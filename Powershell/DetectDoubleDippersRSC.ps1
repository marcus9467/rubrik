<#

.SYNOPSIS
This script will detect objects that are protected multiple ways and offer suggestions to streamline and only backup the data once across the different snappables.

.EXAMPLE
./DetectDoubleDippersRSC.ps1 -serviceAccountJson C:\Temp\ServiceAccountJson.json

This will generate a CSV that displays all instances of common host protection for review to ensure the same data isn't being backed up twice.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : October 10, 2023
    Company : Rubrik Inc
#>

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

function Get-VolumeGroups{
    try{
        $query = "query WindowsVolumeGroupHostListQuery(`$first: Int!, `$after: String, `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$filter: [Filter!]!, `$childFilter: [Filter!], `$isMultitenancyEnabled: Boolean = false) {
            physicalHosts(hostRoot: WINDOWS_HOST_ROOT, filter: `$filter, first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
              edges {
                cursor
                node {
                  id
                  name
                  isArchived
                  descendantConnection(typeFilter: [VolumeGroup]) {
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
                  effectiveSlaDomain {
                    ...EffectiveSlaDomainFragment
                    __typename
                  }
                  ...CdmClusterColumnFragment
                  cluster {
                    ...ClusterNodeConnectionFragment
                    __typename
                  }
                  ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
                  primaryClusterLocation {
                    id
                    __typename
                  }
                  osType
                  osName
                  vfdState
                  connectionStatus {
                    connectivity
                    timestampMillis
                    __typename
                  }
                  hostVolumes {
                    mountPoints
                    fileSystemType
                    size
                    volumeId
                    volumeGroupId
                    __typename
                  }
                  ...PhysicalHostConnectionStatusColumnFragment
                  physicalChildConnection(typeFilter: [VolumeGroup], filter: `$childFilter) {
                    count
                    edges {
                      node {
                        id
                        name
                        effectiveSlaDomain {
                          ...EffectiveSlaDomainFragment
                          __typename
                        }
                        pendingSla {
                          ...SLADomainFragment
                          __typename
                        }
                        primaryClusterLocation {
                          id
                          __typename
                        }
                        ... on VolumeGroup {
                          isRelic
                          volumes
                          replicatedObjects {
                            cluster {
                              id
                              name
                            }
                          }
                        }
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
              count
            }
          }
          
          fragment OrganizationsColumnFragment on HierarchyObject {
            allOrgs {
              name
            }
          }
          
          fragment CdmClusterColumnFragment on CdmHierarchyObject {
            replicatedObjectCount
            cluster {
              id
              name
              version
              status
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
            }
          }"
        $variables = "{
            `"isMultitenancyEnabled`": false,
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
          $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
          }
          $VGInfo = @()
          $JSON_BODY = $JSON_BODY | ConvertTo-Json
          $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
          $VGInfo += (((($result.Content | convertFrom-Json).data).physicalHosts).edges).node
    }
    catch{
        Write-Error("Error $($_)")
    }
    Write-Output $VGInfo

}
function Get-VMwareVM{
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
              onDemandCount
              retrievedCount
              scheduledCount
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
                onDemandCount
                retrievedCount
                scheduledCount
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
    $VMInfo = @()
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $VMInfo += (((($result.Content | convertFrom-Json).data).vSphereVmNewConnection).edges).node
  }
  catch{
    Write-Error("Error $($_)")
  }
  Write-Output $VMInfo
}
function Get-MSSQLDBs{
  try{
    $query = "query MssqlHostHierarchyHostListQuery(`$first: Int!, `$after: String, `$filter: [Filter!], `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isMultitenancyEnabled: Boolean = false, `$instanceDescendantFilter: [Filter!], `$databaseDescendantFilter: [Filter!]) {
      mssqlTopLevelDescendants(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, typeFilter: [PhysicalHost]) {
        edges {
          cursor
          node {
            id
            authorizedOperations
            ...HostChildInstancesEffectiveSlaColumnFragment
            ... on PhysicalHost {
              instanceDescendantConnection: descendantConnection(filter: `$instanceDescendantFilter, typeFilter: [MssqlInstance]) {
                count
                __typename
              }
              databaseDescendantConnection: descendantConnection(filter: `$databaseDescendantFilter, typeFilter: [Mssql]) {
                count
                __typename
              }
              ...MssqlNameColumnFragment
              ...CbtStatusColumnFragment
              ...CdmClusterColumnFragment
              ...CdmClusterLabelFragment
              ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
              ...EffectiveSlaColumnFragment
              ...PhysicalHostConnectionStatusColumnFragment
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
    
    fragment OrganizationsColumnFragment on HierarchyObject {
      allOrgs {
        name
        __typename
      }
      __typename
    }
    
    fragment CbtStatusColumnFragment on PhysicalHost {
      cbtStatus
      defaultCbt
      __typename
    }
    
    fragment MssqlNameColumnFragment on HierarchyObject {
      id
      name
      objectType
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
    
    fragment CdmClusterLabelFragment on CdmHierarchyObject {
      cluster {
        id
        name
        version
        __typename
      }
      primaryClusterLocation {
        id
        __typename
      }
      __typename
    }
    
    fragment HostChildInstancesEffectiveSlaColumnFragment on PhysicalHost {
      id
      instanceDescendantConnection: descendantConnection(filter: `$instanceDescendantFilter, typeFilter: [MssqlInstance]) {
        edges {
          node {
            id
            ...EffectiveSlaColumnFragment
            __typename
          }
          __typename
        }
        __typename
      }
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
    
    fragment PhysicalHostConnectionStatusColumnFragment on PhysicalHost {
      id
      authorizedOperations
      connectionStatus {
        connectivity
        __typename
      }
      __typename
    }"
    $variables = "{
      `"isMultitenancyEnabled`": true,
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
          `"field`": `"IS_ARCHIVED`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"instanceDescendantFilter`": [
        {
          `"field`": `"IS_ARCHIVED`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"databaseDescendantFilter`": [
        {
          `"field`": `"IS_LOG_SHIPPING_SECONDARY`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_MOUNT`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_ARCHIVED`",
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
    $sqlDbInfo = @()
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $sqlDbInfo += (((($result.Content | convertFrom-Json).data).mssqlTopLevelDescendants).edges).node

  }
  catch{
    Write-Error("Error $($_)")
  }
  Write-Output $sqlDbInfo
}
function Get-MSSQLAGs{
  try{
    $query = "query MssqlAvailabilityGroupListQuery(`$first: Int!, `$after: String, `$filter: [Filter!], `$sortBy: HierarchySortByField, `$sortOrder: SortOrder, `$isMultitenancyEnabled: Boolean = false, `$databaseDescendantFilter: [Filter!]) {
      mssqlTopLevelDescendants(after: `$after, first: `$first, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, typeFilter: [MssqlAvailabilityGroup]) {
        edges {
          cursor
          node {
            id
            authorizedOperations
            ...MssqlNameColumnFragment
            ...AvailabilityGroupDatabaseCopyOnlyColumnFragment
            ...AvailabilityGroupMssqlDatabaseCountColumnFragment
            ...CdmClusterColumnFragment
            ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
            ...CdmClusterLabelFragment
            ...EffectiveSlaColumnFragment
            ...SlaAssignmentColumnFragment
            ...AvailabilityGroupInstanceColumnFragment
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
    
    fragment OrganizationsColumnFragment on HierarchyObject {
      allOrgs {
        name
        __typename
      }
      __typename
    }
    
    fragment MssqlNameColumnFragment on HierarchyObject {
      id
      name
      objectType
      __typename
    }
    
    fragment AvailabilityGroupDatabaseCopyOnlyColumnFragment on MssqlAvailabilityGroup {
      copyOnly
      __typename
    }
    
    fragment AvailabilityGroupMssqlDatabaseCountColumnFragment on MssqlAvailabilityGroup {
      descendantConnection(filter: `$databaseDescendantFilter, typeFilter: [Mssql]) {
        count
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
    
    fragment CdmClusterLabelFragment on CdmHierarchyObject {
      cluster {
        id
        name
        version
        __typename
      }
      primaryClusterLocation {
        id
        __typename
      }
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
    
    fragment SlaAssignmentColumnFragment on HierarchyObject {
      slaAssignment
      __typename
    }
    
    fragment AvailabilityGroupInstanceColumnFragment on MssqlAvailabilityGroup {
      instances {
        logicalPath {
          fid
          name
          __typename
        }
        __typename
      }
      __typename
    }"
    $variables = "{
      `"isMultitenancyEnabled`": true,
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
          `"field`": `"IS_ARCHIVED`",
          `"texts`": [
            `"false`"
          ]
        }
      ],
      `"sortBy`": `"NAME`",
      `"sortOrder`": `"ASC`",
      `"databaseDescendantFilter`": [
        {
          `"field`": `"IS_LOG_SHIPPING_SECONDARY`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_MOUNT`",
          `"texts`": [
            `"false`"
          ]
        },
        {
          `"field`": `"IS_ARCHIVED`",
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
    $sqlAgInfo = @()
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $sqlAgInfo += (((($result.Content | convertFrom-Json).data).mssqlTopLevelDescendants).edges).node

  }
  catch{
    Write-Error("Error $($_)")
  }
  Write-Output $sqlAgInfo
}
function Get-Fileset{
  try{
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
    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
    }
    $filesetHostInfo = @()
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $filesetHostInfo += (((($result.Content | convertFrom-Json).data).physicalHosts).edges).node
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
    $JSON_BODY = @{
      "variables" = $variables
      "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $filesetHostInfo += (((($result.Content | convertFrom-Json).data).physicalHosts).edges).node


  }
  catch{
    Write-Error("Error $($_)")
  }
  Write-Output $filesetHostInfo
}
$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$mdate = (Get-Date).tostring("yyyyMMddHHmm")
$Output_directory = (Get-Location).path

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")


#Get List of Hosts
Write-Host "Gathering a list of common hosts across snappable types."
$VMList = Get-VMwareVM
$VMList = $VMList |Where-object {($_.effectiveSlaDomain).name -ne "UNPROTECTED"} |Where-object {($_.effectiveSlaDomain).name -ne "DO_NOT_PROTECT"}
$VGList = Get-VolumeGroups
$VGList = $VGList |where-object {(((($_.physicalChildConnection).edges).node).effectiveSlaDomain).name -ne "UNPROTECTED"} |Where-object {(((($_.physicalChildConnection).edges).node).effectiveSlaDomain).name -ne "DO_NOT_PROTECT"}
$MSSQLDBList = Get-MSSQLDBs
#Filter based on protection status
$MSSQLDBList = $MSSQLDBList | Where-Object {(((($_.instanceDescendantConnection).edges).node).effectiveSlaDomain).name -ne "UNPROTECTED"} |Where-object {(((($_.instanceDescendantConnection).edges).node).effectiveSlaDomain).name -ne "DO_NOT_PROTECT"}
$AGList = Get-MSSQLAGs
$AGList = $AGList | Where-Object {(((($_.instanceDescendantConnection).edges).node).effectiveSlaDomain).name -ne "UNPROTECTED"} |Where-object {(((($_.instanceDescendantConnection).edges).node).effectiveSlaDomain).name -ne "DO_NOT_PROTECT"}

$AGHostList = (($AGList.instances).logicalpath).name
$filesetList = Get-Fileset
$filesetList = $filesetList | Where-Object {($_.effectiveSlaDomain).name -ne "UNPROTECTED"} |Where-object {($_.effectiveSlaDomain).name -ne "DO_NOT_PROTECT"}

Write-Host "Hosts gathered. Comparing results across snappable types to produce a list of multiple protection contexts."
$doubleDippers = @()
if(($filesetList | Measure-Object).count -ge 1){
  Write-Host "Comparing Fileset Assignments....."
  #Compare Filesets and SQL Hosts
  $Fileset_SQL_DoubleDippers = (Compare-Object -ReferenceObject $filesetList -DifferenceObject $MSSQLDBList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $Fileset_SQL_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "Fileset&MSSQL"
    $doubleDippers += $DoubleHit  
  }
  $Fileset_SQLAG_DoubleDippers = (Compare-Object -ReferenceObject $filesetList -DifferenceObject $AGHostList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $Fileset_SQLAG_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "Fileset&MSSQLAG"
    $doubleDippers += $DoubleHit
  }
  #Compare VGs and Filesets 
  $VG_Fileset_DoubleDippers = (Compare-Object -ReferenceObject $VGList -DifferenceObject $filesetList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $VG_Fileset_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "Fileset&VG"
    $doubleDippers += $DoubleHit
  }
  #Compare VMs and Filesets
  $Fileset_VMs_DoubleDippers = (Compare-Object -ReferenceObject $filesetList -DifferenceObject $VMList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $Fileset_VMs_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "Fileset&VM"
    $doubleDippers += $DoubleHit
  }
}
if(($VMList | Measure-Object).count -ge 1){
  Write-Host "Comparing VM Assignments....."
  #Compare SQL and VMware VMs
  $SQL_VM_DoubleDippers = (Compare-Object -ReferenceObject $MSSQLDBList -DifferenceObject $VMList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $SQL_VM_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "VM&MSSQL"
    $doubleDippers += $DoubleHit
  }
  $SQLAG_VM_DoubleDippers = (Compare-Object -ReferenceObject $AGHostList -DifferenceObject $VMList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $SQLAG_VM_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "VM&MSSQLAG"
    $doubleDippers += $DoubleHit
  }
  #Compare VGs and VMware VMs
  $VG_VMs_DoubleDippers = (Compare-Object -ReferenceObject $VGList -DifferenceObject $VMList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $VG_VMs_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "VG&VM"
    $doubleDippers += $DoubleHit
  }
}
if(($VGList | Measure-Object).count -ge 1){
  Write-Host "Comparing VolumeGroup Assignments....."
  #Compare VGs and SQL 
  $VG_SQL_DoubleDippers = (Compare-Object -ReferenceObject $VGList -DifferenceObject $MSSQLDBList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $VG_SQL_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "VG&MSSQL"
    $doubleDippers += $DoubleHit
  }
  $VG_SQLAG_DoubleDippers = (Compare-Object -ReferenceObject $VGList -DifferenceObject $AGHostList -IncludeEqual | Where-Object {$_.SideIndicator -eq "=="}).InputObject
  forEach($doubler in $VG_SQLAG_DoubleDippers){
    $DoubleHit = New-Object psobject
    $DoubleHit | Add-Member -NotePropertyName "id" -NotePropertyValue $doubler.id
    $DoubleHit | Add-Member -NotePropertyName "name" -NotePropertyValue $doubler.name
    $DoubleHit | Add-Member -NotePropertyName "DoubleHitType" -NotePropertyValue "VG&MSSQLAG"
    $doubleDippers += $DoubleHit
  }
}





Write-Host ("Generating a CSV to file " + $Output_directory + "/doubleDippers" + $mdate + ".csv")
$doubleDippers | Export-Csv -NoTypeInformation ($Output_directory + "/doubleDippers" +$mdate + ".csv")

disconnect-polaris
