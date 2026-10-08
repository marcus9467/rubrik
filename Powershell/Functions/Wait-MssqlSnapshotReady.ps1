function Wait-MssqlSnapshotReady {
    <#
    .SYNOPSIS
    Polls the RSC activity series for an MSSQL backup until the snapshot is ready, then returns.

    .DESCRIPTION
    The on-demand backup mutation returns as soon as the job is queued. Rubrik takes a VSS snapshot
    of the database files, releases the freeze, and then reads the data from the shadow copy. Once
    the 'Retrieving SQL Server backup ... using N parallel connections' event is logged the data
    pull has started, which means the snapshot exists, so anything that was
    waiting on a consistent point in time (for example an application unlock/thaw) can proceed
    without waiting for the data transfer. Use -WaitForCompletion to wait for the whole job instead.

    Returns an object with State of Ready, Completed, Failed or TimedOut.

    Uses $Polaris_URL and $headers from the calling scope unless -Uri and -Headers are passed.

    .EXAMPLE
    $start = (Get-Date).AddSeconds(-60)
    Start-MSSQLBackup -databaseId $id -SLAId $sla
    Wait-MssqlSnapshotReady -DatabaseId $id -StartedAfter $start

    .NOTES
    'Retrieving' is the in-progress event and may be replaced by 'Retrieved' on short jobs, so
    both match. Override -ReadyPattern if your CDM version words it differently.
    #>
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$DatabaseId,
        [parameter(Mandatory=$true)]
        [datetime]$StartedAfter,
        [string]$Uri = $Polaris_URL,
        [hashtable]$Headers = $headers,
        [int]$PollSeconds = 10,
        [int]$TimeoutSeconds = 900,
        [string]$ReadyPattern = '^(Retrieving|Retrieved) SQL Server backup',
        [string]$CompletePattern = '^Completed backup',
        [switch]$WaitForCompletion
    )
    process {
        if (-not $Uri -or -not $Headers) {
            throw "Uri and Headers are required (pass them, or set `$Polaris_URL and `$headers in the calling scope)."
        }
        $query = 'query WaitMssqlSnapshot($filters: ActivitySeriesFilter) {
            activitySeriesConnection(first: 5, sortBy: START_TIME, sortOrder: DESC, filters: $filters) {
                nodes {
                    activitySeriesId
                    lastActivityStatus
                    activityConnection(first: 100) { nodes { time status message } }
                }
            }
        }'
        $variables = @{
            filters = @{
                objectFid        = @($DatabaseId)
                lastActivityType = @('BACKUP')
                startTimeGt      = $StartedAfter.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [System.Globalization.CultureInfo]::InvariantCulture)
            }
        }
        $body = @{ query = $query; variables = $variables } | ConvertTo-Json -Depth 10
        $clock = [System.Diagnostics.Stopwatch]::StartNew()

        while ($clock.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
            $series = $null
            try {
                $response = Invoke-RestMethod -Uri $Uri -Method POST -Headers $Headers -Body $body
                if ($response.errors) { Write-Warning ("RSC returned errors: " + ($response.errors | ConvertTo-Json -Compress -Depth 5)) }
                # Newest series first. None yet means the job has not started.
                $series = @($response.data.activitySeriesConnection.nodes)[0]
            }
            catch {
                Write-Warning ("Poll failed, will retry: $($_)")
            }

            if ($series) {
                $events   = @($series.activityConnection.nodes | Sort-Object time)
                $ready    = $events | Where-Object { $_.message -match $ReadyPattern } | Select-Object -First 1
                $complete = $events | Where-Object { $_.message -match $CompletePattern } | Select-Object -First 1
                $failed   = $series.lastActivityStatus -in @('Failure', 'Canceled', 'Canceling')

                $state = $null
                if ($complete) { $state = 'Completed' }
                elseif ($failed) { $state = 'Failed' }
                elseif ($ready -and -not $WaitForCompletion) { $state = 'Ready' }

                if ($state) {
                    return [pscustomobject]@{
                        State          = $state
                        SeriesId       = $series.activitySeriesId
                        ReadyTime      = $ready.time
                        ReadyMessage   = $ready.message
                        LastStatus     = $series.lastActivityStatus
                        ElapsedSeconds = [math]::Round($clock.Elapsed.TotalSeconds)
                    }
                }
            }
            Start-Sleep -Seconds $PollSeconds
        }
        [pscustomobject]@{
            State          = 'TimedOut'
            SeriesId       = $series.activitySeriesId
            ReadyTime      = $null
            ReadyMessage   = $null
            LastStatus     = $series.lastActivityStatus
            ElapsedSeconds = [math]::Round($clock.Elapsed.TotalSeconds)
        }
    }
}
