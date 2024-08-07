<#
.SYNOPSIS
This script will extract compliance and snapshot information for all CDM clusters in a given RSC environment. Filters are available for both clusters and slas. 

.EXAMPLE
./BackupComplianceRangeReport.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -ReportRange PAST_7_DAYS

This will generate a list of objects and their compliance status over the last 7 days. In the event there are missed snapshots, snapshot information relative to the date range specified will be pulled and complied into a single report.


.EXAMPLE
./BackupComplianceRangeReport.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -ReportRange PAST_7_DAYS -ClusterId "3bc43be7-00ca-4ed8-ba13-cef249d337fa,39b92c18-d897-4b55-a7f9-17ff178616d0"

This will generate a list of objects and their compliance status over the last 7 days. In the event there are missed snapshots, snapshot information relative to the date range specified will be pulled and complied into a single report. This will also filter to only the clusterUUIDs specified 


.EXAMPLE
./BackupComplianceRangeReport.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -ReportRange PAST_7_DAYS -SlaIds "71ede730-34a2-53e0-a0f2-829d9a0b4b30"

This will generate a list of objects and their compliance status over the last 7 days. In the event there are missed snapshots, snapshot information relative to the date range specified will be pulled and complied into a single report. This will also filter to only the SLAIDs specified. The SLAIDs can either be the local CDM IDs, or the global RSC FIDs


.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> in collaboration with Reggie Hobbs
    Created : March 30, 2023
    Last Edit : August 07, 2024
    Company : Rubrik Inc
#>

[CmdletBinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [ValidateSet("PAST_365_DAYS", "PAST_90_DAYS", "PAST_30_DAYS", "PAST_7_DAYS", "PAST_3_DAYS", "LAST_24_HOURS")]
    [string]$ReportRange,
    [parameter(Mandatory=$false)]
    [string]$ClusterId = "[]",
    [parameter(Mandatory=$false)]
    [string]$SlaIds,
    [parameter(Mandatory=$false)]
    [switch]$CSV,
    [parameter(Mandatory=$false)]
    [switch]$debugSingle,
    [parameter(Mandatory=$false)]
    [string]$snappableId
)

function connect-polaris {
    [CmdletBinding()]
    param ()
      
    begin {
        $connectionData = [ordered]@{
            'client_id'     = $serviceAccountObj.client_id
            'client_secret' = $serviceAccountObj.client_secret
        } | ConvertTo-Json
    }

    process {
        try {
            $polaris = Invoke-RestMethod -Method Post -uri $serviceAccountObj.access_token_uri -ContentType application/json -body $connectionData
        } catch [System.Management.Automation.ParameterBindingException] {
            Write-Error("The provided JSON has null or empty fields, try the command again with the correct file or redownload the service account JSON from Polaris")
        }
    }

    end {
        if ($polaris.access_token) {
            Write-Output $polaris
        } else {
            Write-Error("Unable to connect")
        }
    }
}

function disconnect-polaris {
    [CmdletBinding()]
    param ()

    begin {}

    process {
        try {
            $closeStatus = (Invoke-WebRequest -Method Delete -Headers $headers -ContentType "application/json; charset=utf-8" -Uri $logoutUrl).StatusCode
        } catch [System.Management.Automation.ParameterBindingException] {
            Write-Error("Failed to logout. Error $($_)")
        }
    }

    end {
        if ($closeStatus -eq 204) {
            Write-Output("Successfully logged out")
        } else {
            Write-Error("Error $($_)")
        }
    }
}

function Get-ClusterInfo {
    try {
        $query = "query ClusterListTableQuery(`$first: Int, `$after: String, `$filter: ClusterFilterInput, `$sortBy: ClusterSortByEnum, `$sortOrder: SortOrder, `$showOrgColumn: Boolean = false) {
            clusterConnection(filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder, first: `$first, after: `$after) {
                edges {
                    cursor
                    node {
                        id
                        ...ClusterListTableFragment
                        ...OrganizationClusterFragment @include(if: `$showOrgColumn)
                    }
                }
                pageInfo {
                    startCursor
                    endCursor
                    hasNextPage
                    hasPreviousPage
                }
                count
            }
        }

        fragment OrganizationClusterFragment on Cluster {
            allOrgs {
                name
            }
        }

        fragment ClusterListTableFragment on Cluster {
            id
            name
            pauseStatus
            defaultAddress
            ccprovisionInfo {
                progress
                jobStatus
                jobType
                __typename
            }
            estimatedRunway
            geoLocation {
                address
                __typename
            }
            ...ClusterCardSummaryFragment
            ...ClusterNodeConnectionFragment
            ...ClusterStateFragment
            ...ClusterGlobalManagerFragment
            ...ClusterAuthorizedOperationsFragment
            ...ClusterVersionColumnFragment
            ...ClusterTypeColumnFragment
            ...ClusterCapacityColumnFragment
        }

        fragment ClusterCardSummaryFragment on Cluster {
            status
            systemStatus
            systemStatusAffectedNodes {
                id
            }
            clusterNodeConnection {
                count
            }
            lastConnectionTime
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

        fragment ClusterStateFragment on Cluster {
            state {
                connectedState
                clusterRemovalState
            }
        }

        fragment ClusterGlobalManagerFragment on Cluster {
            passesConnectivityCheck
            globalManagerConnectivityStatus {
                urls {
                    url
                    isReachable
                }
            }
            connectivityLastUpdated
        }

        fragment ClusterAuthorizedOperationsFragment on Cluster {
            authorizedOperations {
                id
                operations
            }
        }

        fragment ClusterVersionColumnFragment on Cluster {
            version
        }

        fragment ClusterTypeColumnFragment on Cluster {
            name
            productType
            type
            clusterNodeConnection {
                nodes {
                    id
                }
            }
        }

        fragment ClusterCapacityColumnFragment on Cluster {
            metric {
                usedCapacity
                availableCapacity
                totalCapacity
            }
        }"

        $variables = "{
            `"showOrgColumn`": true,
            `"sortBy`": `"ClusterName`",
            `"sortOrder`": `"ASC`",
            `"filter`": {
                `"id`": [],
                `"name`": [
                    `"`"
                ],
                `"type`": [],
                `"orgId`": []
            }
        }"

        $JSON_BODY = @{
            "variables" = $variables
            "query"     = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $clusterInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $clusterInfo = (((($clusterInfo.content | ConvertFrom-Json).data).clusterConnection).edges).node | Where-Object { $_.productType -ne "DATOS" }
    } catch {
        Write-Error("Error $($_)")
    }
    Write-Output $clusterInfo
}

function Get-ProtectionTaskDetails {
    try {
        $InFormat = "yyyy-MM-ddTHH:mm:ss.fffZ"
        $currentDate = (Get-Date -Format $InFormat)
        $startDate = ($currentDate | Get-Date).AddDays("-" + $actualDaysToReport) | Get-Date -Format $InFormat
        $protectionTaskDetailsData = @()

        $variables = "{
            `"first`": 200,
            `"filter`": {
                `"time_gt`": `"${startDate}`",
                `"time_lt`": `"${currentDate}`",
                `"clusterUuid`": ${clusterList},
                `"taskCategory`": [
                    `"Protection`"
                ],
                `"taskType`": [
                `"Backup`"
              ],
                `"orgId`": []
            },
            `"sortBy`": `"EndTime`",
            `"sortOrder`": `"DESC`"
        }"
        $query = "query ProtectionTaskDetailTableQuery(`$first: Int!, `$after: String, `$filter: TaskDetailFilterInput, `$sortBy: TaskDetailSortByEnum, `$sortOrder: SortOrder) {
            taskDetailConnection(first: `$first, after: `$after, filter: `$filter, sortBy: `$sortBy, sortOrder: `$sortOrder) {
                edges {
                    cursor
                    node {
                        id
                        clusterUuid
                        clusterName
                        taskType
                        status
                        objectName
                        objectType
                        location
                        clusterLocation
                        slaDomainName
                        replicationSource
                        replicationTarget
                        archivalTarget
                        directArchive
                        failureReason
                        snapshotConsistency
                        protectedVolume
                        startTime
                        endTime
                        duration
                        dataTransferred
                        totalFilesTransferred
                        physicalBytes
                        logicalBytes
                        dedupRatio
                        logicalDedupRatio
                        dataReduction
                        logicalDataReduction
                        orgId
                        orgName
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }"
        $JSON_BODY = @{
            "variables" = $variables
            "query"     = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $protectionTaskDetailsData += ((((($result.content) | ConvertFrom-Json).data).taskDetailConnection).edges).node

        while (((((($result.content) | ConvertFrom-Json).data).taskDetailConnection).pageinfo).hasNextPage -eq $true) {
            $endCursor = ((((($result.content) | ConvertFrom-Json).data).taskDetailConnection).pageinfo).endCursor
            Write-Host ("Looking at End Cursor " + $endCursor)
            $variables = "{
                `"first`": 200,
                `"filter`": {
                    `"time_gt`": `"${startDate}`",
                    `"time_lt`": `"${currentDate}`",
                    `"clusterUuid`": ${clusterList},
                    `"taskCategory`": [
                        `"Protection`"
                    ],
                    `"taskType`": [
                        `"Backup`"
                    ],
                    `"orgId`": []
                },
                `"sortBy`": `"EndTime`",
                `"sortOrder`": `"DESC`",
                `"after`": `"${endCursor}`"
            }"
            $JSON_BODY = @{
                "variables" = $variables
                "query"     = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $protectionTaskDetailsData += ((((($result.content | ConvertFrom-Json).data).taskDetailConnection).edges).node)
        }
    } catch {
        Write-Error("Error $($_)")
    }
    End {
        Write-Output $protectionTaskDetailsData
    }
}

function get-info {
    process {
        try {
            if (!($SlaIDs)) {
                $variables = "{
                    `"first`": 200,
                    `"filter`": {
                        `"cluster`": {
                            `"id`": $clusterId
                        },
                        `"complianceStatus`": [
                            `"IN_COMPLIANCE`",
                            `"OUT_OF_COMPLIANCE`",
                            `"NOT_AVAILABLE`"
                        ],
                        `"protectionStatus`": [],
                        `"slaTimeRange`": `"${daysToReport}`",
                        `"orgId`": []
                    },
                    `"sortBy`": `"Name`",
                    `"sortOrder`": `"ASC`"
                }"
            }

            if ($SlaIDs) {
                $SlaIDs = $SlaIDs.Split(",")
                $SlaIDs = $SlaIDs | ConvertTo-Json
                $variables = "{
                    `"first`": 200,
                    `"filter`": {
                        `"cluster`": {
                            `"id`": $clusterId
                        },
                        `"complianceStatus`": [
                            `"IN_COMPLIANCE`",
                            `"OUT_OF_COMPLIANCE`",
                            `"NOT_AVAILABLE`"
                        ],
                        `"protectionStatus`": [],
                        `"slaDomain`": {
                            `"id`": $SlaIDs
                        },
                        `"slaTimeRange`": `"${daysToReport}`",
                        `"orgId`": []
                    },
                    `"sortBy`": `"Name`",
                    `"sortOrder`": `"ASC`"
                }"
            }

            if ($ClusterId -ne "[]") {
                Write-Host ("Gathering Compliance Info for clusters " + $clusterId)
            }

            $query = "query ComplianceTableQuery(`$first: Int!, `$filter: SnappableFilterInput, `$after: String, `$sortBy: SnappableSortByEnum, `$sortOrder: SortOrder) {
                snappableConnection(first: `$first, filter: `$filter, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
                    edges {
                        cursor
                        node {
                            id
                            name
                            cluster {
                                id
                                name
                            }
                            slaDomain {
                                id
                                name
                                ... on GlobalSlaReply {
                                    isRetentionLockedSla
                                }
                                ... on ClusterSlaDomain {
                                    isRetentionLockedSla
                                }
                            }
                            location
                            complianceStatus
                            localSnapshots
                            replicaSnapshots
                            archiveSnapshots
                            totalSnapshots
                            missedSnapshots
                            lastSnapshot
                            latestArchivalSnapshot
                            latestReplicationSnapshot
                            objectType
                            fid
                            localOnDemandSnapshots
                            localSlaSnapshots
                            archivalSnapshotLag
                            replicationSnapshotLag
                            archivalComplianceStatus
                            replicationComplianceStatus
                            awaitingFirstFull
                            pullTime
                            orgName
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }"

            $JSON_BODY = @{
                "variables" = $variables
                "query"     = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json

            $snappableInfo = @()
            $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $snappableInfo += (((($info.content | ConvertFrom-Json).data).snappableConnection).edges).node

            while ((((($info.content | ConvertFrom-Json).data).snappableConnection).pageInfo).hasNextPage -eq $true) {
                $endCursor = ((((($info.content | ConvertFrom-Json).data).snappableConnection).pageInfo).endCursor)
                Write-Host ("Paging through another 200 Objects. Looking at End Cursor " + $endCursor)
                if (!($SlaIDs)) {
                    $variables = "{
                        `"first`": 200,
                        `"filter`": {
                            `"cluster`": {
                                `"id`": $clusterId
                            },
                            `"complianceStatus`": [
                                `"IN_COMPLIANCE`",
                                `"OUT_OF_COMPLIANCE`",
                                `"NOT_AVAILABLE`"
                            ],
                            `"protectionStatus`": [],
                            `"slaTimeRange`": `"${daysToReport}`",
                            `"orgId`": []
                        },
                        `"sortBy`": `"Name`",
                        `"sortOrder`": `"ASC`",
                        `"after`": `"${endCursor}`"
                    }"
                }
                if ($SlaIDs) {
                    $variables = "{
                        `"first`": 200,
                        `"filter`": {
                            `"cluster`": {
                                `"id`": $clusterId
                            },
                            `"complianceStatus`": [
                                `"IN_COMPLIANCE`",
                                `"OUT_OF_COMPLIANCE`",
                                `"NOT_AVAILABLE`"
                            ],
                            `"protectionStatus`": [],
                            `"slaDomain`": {
                                `"id`": $SlaIDs
                                },
                            `"slaTimeRange`": `"${daysToReport}`",
                            `"orgId`": []
                        },
                        `"sortBy`": `"Name`",
                        `"sortOrder`": `"ASC`",
                        `"after`": `"${endCursor}`"
                    }"
                }
                $JSON_BODY = @{
                    "variables" = $variables
                    "query"     = $query
                }
                $JSON_BODY = $JSON_BODY | ConvertTo-Json
                $info = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
                $snappableInfo += (((($info.content | ConvertFrom-Json).data).snappableConnection).edges).node
            }

        } catch {
            Write-Error("Error $($_)")
        }
    }

    End {
        Write-Output $snappableInfo
    }
}

function get-SnapshotInfo {
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$snappableId
        # snappableId = FID, not ID
    )
    process {
        try {
            $variables = "{
                `"first`": 50,
                `"filter`": {
                    `"objectFid`": `"${snappableId}`"
                }
            }"
            $query = "query MarcusSnappableType(`$first: Int!, `$filter: SnappableFilterInput, `$after: String, `$sortBy: SnappableSortByEnum, `$sortOrder: SortOrder) {
                snappableConnection(first: `$first, filter: `$filter, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
                    edges {
                        node {
                            id
                            fid
                            name
                            objectType
                            complianceStatus
                            location
                            localSnapshots
                            replicaSnapshots
                            archiveSnapshots
                            totalSnapshots
                            missedSnapshots
                        }
                    }
                }
            }"
            $JSON_BODY = @{
                "variables" = $variables
                "query"     = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $snappableTypeInfo = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $snappableTypeInfo = (((($snappableTypeInfo.content | ConvertFrom-Json).data).snappableConnection).edges).node

            if ($snappableTypeInfo.objectType -ne "Mssql") {
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
                    "query"     = $query
                }
                $JSON_BODY = $JSON_BODY | ConvertTo-Json
                $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
                $snappableInfo += (((($result.content | ConvertFrom-Json).data).snapshotsListConnection).edges).node

                while ((((($result.content | ConvertFrom-Json).data).snapshotsListConnection).pageInfo).hasNextPage -eq $true) {
                    $endCursor = ((((($result.content | ConvertFrom-Json).data).snapshotsListConnection).pageInfo).endCursor)
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
                        "query"     = $query
                    }
                    $JSON_BODY = $JSON_BODY | ConvertTo-Json
                    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
                    $snappableInfo += (((($result.content | ConvertFrom-Json).data).snapshotsListConnection).edges).node
                }
            } else {
                $snappableInfo = @()
                $query = "query MssqlDatabaseSnapshotCalendarQuery(`$snappableFid: UUID!, `$snapshotGroupBy: CdmSnapshotGroupByEnum!, `$filters: [CdmSnapshotFilter!], `$missedSnapshotGroupBy: MissedSnapshotGroupByTime!, `$timezoneOffset: Float, `$timeRange: TimeRangeInput!) {
                    snappable: mssqlDatabase(fid: `$snappableFid) {
                        id
                        missedSnapshotGroupByConnection(groupBy: `$missedSnapshotGroupBy, filter: {timeRange: `$timeRange}, timezoneOffset: `$timezoneOffset) {
                            nodes {
                                groupByInfo {
                                    ... on TimeRangeWithUnit {
                                        unit
                                        start
                                        end
                                        __typename
                                    }
                                    __typename
                                }
                                missedSnapshotConnection {
                                    count
                                    nodes {
                                        date
                                        __typename
                                    }
                                    __typename
                                }
                                __typename
                            }
                            __typename
                        }
                        cdmGroupedSnapshots(groupBy: `$snapshotGroupBy, CdmSnapshotFilter: `$filters, timezoneOffset: `$timezoneOffset) {
                            nodes {
                                groupByInfo {
                                    group
                                    start
                                    end
                                    __typename
                                }
                                cdmSnapshots {
                                    count
                                    nodes {
                                        id
                                        date
                                        isIndexed
                                        isUnindexable
                                    }
                                }
                            }
                        }
                    }
                }"
                $timezone = [System.TimeZoneInfo]::Local
                $offset = $timezone.BaseUtcOffset
                $hoursOffset = $offset.Hours

                if ($timezone.SupportsDaylightSavingTime -and $timezone.IsDaylightSavingTime([System.DateTime]::Now)) {
                    $hoursOffset++
                }

                $offsetString = if ($hoursOffset -ge 0) { "+$hoursOffset" } else { "$hoursOffset" }

                $variables = "{
                    `"snappableFid`": `"${snappableId}`",
                    `"snapshotGroupBy`": `"Day`",
                    `"missedSnapshotGroupBy`": `"DAY`",
                    `"timeRange`": {
                        `"start`": `"${startDate}`",
                        `"end`": `"${currentDate}`"
                    },
                    `"timezoneOffset`": $offsetString,
                    `"filters`": [
                        {
                            `"field`": `"TIME_RANGE`",
                            `"texts`": [
                                `"${startDate}`",
                                `"${currentDate}`"
                            ]
                        },
                        {
                            `"field`": `"IS_EXPIRED`",
                            `"texts`": [
                                `"false`"
                            ]
                        }
                    ]
                }"
                $JSON_BODY = @{
                    "variables" = $variables
                    "query"     = $query
                }
                $JSON_BODY = $JSON_BODY | ConvertTo-Json
                $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
                $result = (((((($result.content | ConvertFrom-Json).data).snappable).cdmGroupedSnapshots).nodes).cdmSnapshots).nodes
                $snappableInfo += $result
            }
        } catch {
            Write-Error("Error $($_)")
        }
    }
    End {
        Write-Output $snappableInfo
    }
}

Function Get-DateRange {
    [CmdletBinding()]
    Param (
        [datetime]$Start = (Get-Date),
        [datetime]$End = (Get-Date)
    )
    
    ForEach ($Num in (0..((New-TimeSpan –Start $Start –End $End).Days))) {
        $Start.AddDays($Num)
    }
}

# Adding certificate exception to prevent API errors
if ($IsWindows -eq $true) {

    <#
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
    #>

}

if ($IsMacOS -eq $true) {
    # Do Nothing for now
}

# Inserting logic to filter the dates down to the last 3 days only.
if ($ReportRange -eq "PAST_3_DAYS") {
    $daysToReport = "PAST_7_DAYS"
} else {
    $daysToReport = $ReportRange
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$Output_directory = (Get-Location).path
$mdate = (Get-Date).ToString("yyyyMMddHHmm")

# Set Timeframe to scan based on $DaysToReport
$InFormat = "yyyy-MM-ddTHH:mm:ss.fffZ"
# Setting current date to n - 1 to account for backups that run in the evening, while the report runs earlier in the day. 
$currentDate = ((Get-Date).AddDays(-1)).ToString($InFormat)

if ($daysToReport) {
    if ($daysToReport -match "PAST_365_DAYS") {
        $actualDaysToReport = 365
    }
    if ($daysToReport -match "PAST_90_DAYS") {
        $actualDaysToReport = 90
    }
    if ($daysToReport -match "PAST_30_DAYS") {
        $actualDaysToReport = 30
    }
    if ($daysToReport -match "PAST_7_DAYS") {
        $actualDaysToReport = 7
    }
    if ($daysToReport -match "LAST_24_HOURS") {
        $actualDaysToReport = 1
    }
}
$startDate = ($currentDate | Get-Date).AddDays("-" + $actualDaysToReport) | Get-Date -Format $InFormat

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json'
    'Accept'        = 'application/json'
    'Authorization' = $('Bearer ' + $rubtok)
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$R2 = get-info
if ($debugSingle) {
    $R2 = $R2 | Where-Object { $_.fid -eq $snappableId }
    $DebugBackupList = get-SnapshotInfo -snappableId $snappableId
    Write-Host ("Writing report file to "  + $Output_directory + "/DebugBackupList" + $mdate + ".csv")
    $DebugBackupInfo = @()
    foreach ($backup in $DebugBackupList) {
        $DebugSummaryInfo = New-Object PSObject
        $DebugSummaryInfo | Add-Member -NotePropertyName "ObjectId" -NotePropertyValue $snappableId
        $DebugSummaryInfo | Add-Member -NotePropertyName "BackupId" -NotePropertyValue $backup.id
        $DebugSummaryInfo | Add-Member -NotePropertyName "date" -NotePropertyValue $backup.date
        $DebugSummaryInfo | Add-Member -NotePropertyName "isOnDemandSnapshot" -NotePropertyValue $backup.isOnDemandSnapshot
        $DebugSummaryInfo | Add-Member -NotePropertyName "clusterName" -NotePropertyValue ($backup.cluster).name
        $DebugSummaryInfo | Add-Member -NotePropertyName "clusterId" -NotePropertyValue ($backup.cluster).Id
        $DebugSummaryInfo | Add-Member -NotePropertyName "slaName" -NotePropertyValue ($backup.sladomain).name
        $DebugSummaryInfo | Add-Member -NotePropertyName "slaId" -NotePropertyValue ($backup.sladomain).Id
        $DebugBackupInfo += $DebugSummaryInfo
    }
    $DebugBackupInfo | Export-Csv -NoTypeInformation ($Output_directory + "/DebugBackupList" + $mdate + ".csv")
}

$R2Count = $R2 | Measure-Object | Select-Object -ExpandProperty Count
$ClusterInfo = Get-ClusterInfo
#$TotalRSClusterCount = ($ClusterInfo.id | Measure-Object).Count
$R2ClusterCount = ($R2.cluster | Select-Object id -Unique | Measure-Object).Count
$R2SLACount = ($R2.slaDomain) | Select-Object ID -Unique | Measure-Object | Select-Object -ExpandProperty Count

# Totals
$TotalBackups = $R2 | Select-Object -ExpandProperty totalSnapshots | Measure-Object -Sum | Select-Object -ExpandProperty Sum
$TotalStrikes = $R2 | Select-Object -ExpandProperty missedSnapshots | Measure-Object -Sum | Select-Object -ExpandProperty Sum

$SummaryInfo = New-Object PSObject
$SummaryInfo | Add-Member -NotePropertyName "ObjectCount" -NotePropertyValue $R2Count
$SummaryInfo | Add-Member -NotePropertyName "ClusterCount" -NotePropertyValue $R2ClusterCount
$SummaryInfo | Add-Member -NotePropertyName "SLACount" -NotePropertyValue $R2SLACount
$SummaryInfo | Add-Member -NotePropertyName "TotalBackupsCount" -NotePropertyValue $TotalBackups
$SummaryInfo | Add-Member -NotePropertyName "TotalMissedBackupCount" -NotePropertyValue $TotalStrikes

# Get Range of Dates Specified and setup table for html function later. 
$dateArray = Get-DateRange $startDate $currentDate
if ($ReportRange -eq "PAST_3_DAYS") {
    # Need to set the range to -2 as the report includes the current day as well as the prior amount specified here. 
    $threeDaysAgo = (Get-Date $currentdate).AddDays(-2)
    $dateArray = Get-DateRange $threeDaysAgo $currentDate
}
$dateReportTemplate = @()
foreach ($day in $dateArray) {
    $formatedDate = $day.ToString("yyyy-MM-dd")
    $dateReportTemplate += $formatedDate
}

$dateFormattedReportTemplate = New-Object PSObject
foreach ($date in $dateReportTemplate) {
    $dateFormattedReportTemplate | Add-Member -NotePropertyName $date -NotePropertyValue 1
}

# Add in the DateRangeTemplate to Objects
foreach ($object in $R2) {
    $object | Add-Member -NotePropertyName "AvailableBackupRange" -NotePropertyValue $dateFormattedReportTemplate
}

# Map the Missed Backups to the dateRangeTemplate
$ObjectStrikeSnapInfo = @()
$threeStrikeOffender = @()

foreach ($object in $R2) {
    $BackupList = New-Object PSObject
    $BackupList | Add-Member -NotePropertyName "ObjectName"  -NotePropertyValue $object.name
    $BackupList | Add-Member -NotePropertyName "ObjectId"  -NotePropertyValue $object.Id
    $BackupList | Add-Member -NotePropertyName "Location" -NotePropertyValue $object.location
    $BackupList | Add-Member -NotePropertyName "objectType"  -NotePropertyValue $object.objectType
    $BackupList | Add-Member -NotePropertyName "clusterName"  -NotePropertyValue ($object.cluster).name
    $BackupList | Add-Member -NotePropertyName "clusterId"  -NotePropertyValue ($object.cluster).id
    $BackupList | Add-Member -NotePropertyName "lastSnapshot"  -NotePropertyValue $object.lastSnapshot
    $BackupList | Add-Member -NotePropertyName "complianceStatus"  -NotePropertyValue $object.complianceStatus
    $BackupList | Add-Member -NotePropertyName "missedSnapshots"  -NotePropertyValue $object.missedSnapshots

    if ($object.missedSnapshots -gt 0) {
        Write-Host ($object.name + " is out of compliance. Gathering Snapshot Information.")
        $objectInfo = Get-SnapshotInfo -snappableId $object.fid
        $snapshotList = @()
        foreach ($snapshot in $objectInfo) {
            $formatdate = (Get-Date $snapshot.date).ToString("yyyy-MM-dd")
            $snapshotList += $formatdate
        }
        $formattedSnapshotList = $snapshotList | Sort-Object -Unique
        if ($null -eq $formattedSnapshotList) {
            $formattedSnapshotList = @()
        }
        $MissedBackups = Compare-Object -ReferenceObject $dateReportTemplate -DifferenceObject $formattedSnapshotList | ForEach-Object { $_.InputObject }
        $MissedBackupIndex = 0
        $threestrikeHit = 0
        foreach ($date in $dateReportTemplate) {
            if ($MissedBackups -contains $date) {
                $BackupList | Add-Member -NotePropertyName $date -NotePropertyValue "No Backup"
                $MissedBackupIndex++
                if ($MissedBackupIndex -gt 2) {
                    $threestrikeHit = 1
                }
            } else {
                $BackupList | Add-Member -NotePropertyName $date -NotePropertyValue "Backup Available"
                $MissedBackupIndex = 0
            }
        }
        if ($threestrikeHit -eq 1) {
            $threeStrikeOffender += $BackupList
        }
    } else {
        foreach ($date in $dateReportTemplate) {
            $BackupList | Add-Member -NotePropertyName $date -NotePropertyValue "Backup Available"
        }
    }
    $object.AvailableBackupRange = $BackupList
    $ObjectStrikeSnapInfo += $object
}

# Sort Based on Location
$BackupRangeData = $ObjectStrikeSnapInfo.AvailableBackupRange | Group-Object objectType
$SortedBackupRangeData = @()

foreach ($Snappable in $BackupRangeData) {
    if ($Snappable.Name -ne "VmwareVirtualMachine") {
        $sortedData = $Snappable.Group | Sort-Object -Property "Location"
    } else {
        $sortedData = $Snappable.Group | Sort-Object -Property "Name"
    }
    $SortedBackupRangeData += $sortedData
}

# Establish HTML Header information
$HtmlHead = '<style>
    body {
        background-color: white;
        font-family: "Calibri";
    }
    table {
        border-width: 1px;
        border-style: solid;
        border-color: black;
        border-collapse: collapse;
        width: 100%;
        margin: 50px;
    }
    th {
        border-width: 1px;
        padding: 5px;
        border-style: solid;
        border-color: black;
        background-color: #98C6F3;
    }
    td {
        border-width: 1px;
        padding: 5px;
        border-style: solid;
        border-color: black;
        background-color: White;
    }
    tr {
        text-align: left;
    }
</style>'

$threeStrikeOffenderCount = ($threeStrikeOffender | Measure-Object).Count
$SummaryInfo | Add-Member -NotePropertyName "TotalThreeStrikes" -NotePropertyValue $threeStrikeOffenderCount

# Get Color coordination for backup report
$HTMLData = $SortedBackupRangeData | ConvertTo-Html -Head $HtmlHead | ForEach-Object {
    $_ -replace "<td>No Backup</td>", "<td style='background-color:#FF8080'>No Backup</td>"
}
$FinishedData = $HTMLData | ForEach-Object {
    $_ -replace "<td>Backup Available</td>", "<td style='background-color:#008000'>Backup Available</td>"
}

$HTMLSummary = $SummaryInfo | ConvertTo-Html -Head $HtmlHead
$completedReport = $HTMLSummary + $FinishedData

# Get Color coordination for 3-strike report
$threeStrikeHTMLData = $threeStrikeOffender | ConvertTo-Html -Head $HtmlHead | ForEach-Object {
    $_ -replace "<td>No Backup</td>", "<td style='background-color:#FF8080'>No Backup</td>"
}
$threeStrikeFinishedData = $threeStrikeHTMLData | ForEach-Object {
    $_ -replace "<td>Backup Available</td>", "<td style='background-color:#008000'>Backup Available</td>"
}
$threeStrikeHTML = $HTMLSummary + $threeStrikeFinishedData

# Write the reports to disk
Write-Host ("Writing compliance report file to "  + $Output_directory + "/ComplianceRangeReport" + $mdate + ".html")
$completedReport | Out-File ($Output_directory + "/ComplianceRangeReport" + $mdate + ".html")

Write-Host ("Writing three-strike report file to "  + $Output_directory + "/threeStrikeReport" + $mdate + ".html")
$threeStrikeHTML | Out-File ($Output_directory + "/threeStrikeReport" + $mdate + ".html")

if ($CSV) {
    Write-Host ("Writing compliance report file to "  + $Output_directory + "/ComplianceRangeReport" + $mdate + ".csv")
    $SortedBackupRangeData | Export-Csv -NoTypeInformation ($Output_directory + "/ComplianceRangeReport" + $mdate + ".csv")
    
    Write-Host ("Writing three-strike report file to "  + $Output_directory + "/threeStrikeReport" + $mdate + ".csv")
    $threeStrikeOffender | Export-Csv -NoTypeInformation ($Output_directory + "/threeStrikeReport" + $mdate + ".csv")
}
disconnect-polaris
