<#

.SYNOPSIS
This script will trigger an ondemand backup right after the dataloader process is initiated as part of a pre-script process.

.EXAMPLE
./SQLOnDemand.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -DatabaseId1 "9a818023-9f9b-5a91-9228-2ccf18e85926" -DatabaseId2 "210289af-5158-55e9-b23f-4de7727c8995" -SlaId "b5336bdc-6c9b-4784-a848-5d96992ace33"

This will initate an ondemand backup after the dataloader process. 

.EXAMPLE
./SQLOnDemand.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -DatabaseId1 "9a818023-9f9b-5a91-9228-2ccf18e85926" -DatabaseId2 "210289af-5158-55e9-b23f-4de7727c8995" -logBackup

This will initate an ondemand log backup for the identified databases.

.EXAMPLE
./SQLOnDemand.ps1 -ServiceAccountJson /Users/Rubrik/Documents/ServiceAccount.json -DatabaseId1 "9a818023-9f9b-5a91-9228-2ccf18e85926" -DatabaseId2 "210289af-5158-55e9-b23f-4de7727c8995" -SlaId "b5336bdc-6c9b-4784-a848-5d96992ace33" -WaitForSnapshot

This will initiate the ondemand backups, then wait until each database's snapshot is created (the VSS freeze is released and
Rubrik has started reading from the shadow copy) before exiting. Exit code 0 means both snapshots are ready, so a following
unlock/thaw step can run. Exit code 1 means a backup failed, was canceled, or did not reach that point within -WaitTimeoutSeconds.
Configure the calling job to run the unlock/thaw on failure as well. The data transfer continues in the background.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : November 13, 2023
    Company : Rubrik Inc
#>


[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$SlaId,
    [parameter(Mandatory=$true)]
    [string]$DatabaseId1,
    [parameter(Mandatory=$true)]
    [string]$DatabaseId2,
    [parameter(Mandatory=$false)]
    [switch]$logBackup,
    [parameter(Mandatory=$false)]
    [switch]$WaitForSnapshot,
    [parameter(Mandatory=$false)]
    [int]$WaitTimeoutSeconds = 900,
    [parameter(Mandatory=$false)]
    [int]$PollSeconds = 10
)

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json

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
function Start-MSSQLBackup{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$databaseId,
        [parameter(Mandatory=$true)]
        [string]$SLAId

    )
    process{
        try{
            $query = "mutation MssqlTakeOnDemandSnapshotMutation(`$input: CreateOnDemandMssqlBackupInput!) {
                createOnDemandMssqlBackup(input: `$input) {
                  links {
                    href
                    __typename
                  }
                  __typename
                }
            }"
            $variables = "{
                `"input`": {
                  `"config`": {
                    `"baseOnDemandSnapshotConfig`": {
                      `"slaId`": `"${SLAId}`"
                    }
                  },
                  `"id`": `"${databaseId}`",
                  `"userNote`": `"`"
                }
            }"
            $JSON_BODY = @{
                "variables" = $variables
                "query" = $query
            }
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        }
        catch{
            Write-Error("Error $($_)")
        }
    }
    end{
        Write-Host ("Backup Successfully initiated. Please see " + (((($result.Content | convertFrom-Json).data).createOnDemandMssqlBackup).links).href + " for progress information.")
    }
}
function TakeMssqlLogBackup{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string[]]$snappableId
    )
    process{
      try{
        $query = "mutation TakeMssqlLogBackupMutation(`$input: TakeMssqlLogBackupInput!) {takeMssqlLogBackup(input: `$input) {id}}"
        $variables = @{
          input = @{
              id = $snappableId
          }
        }
        $JSON_BODY = @{
          "variables" = $variables
          "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $APIResult = (($result.content | ConvertFrom-Json).data).takeMssqlLogBackup
        Write-Host ("Starting MSSQL Log Job " + $APIResult)
      }
      catch{
        Write-Error("Error $($_)")
      }
    }
  }

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

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

if($logBackup){
    TakeMssqlLogBackup -snappableId $DatabaseId1, $DatabaseId2
    disconnect-polaris
    Exit 0
}
<#
Database Unload Process Here

#>


# Back-dated 60s to tolerate clock skew between this host and RSC
$triggerTime = (Get-Date).AddSeconds(-60)
Start-MSSQLBackup -databaseId $databaseId1 -SLAId $SLAId
Start-MSSQLBackup -databaseId $databaseId2 -SLAId $SLAId

$exitCode = 0
if($WaitForSnapshot){
    foreach($id in $DatabaseId1, $DatabaseId2){
        $wait = Wait-MssqlSnapshotReady -DatabaseId $id -StartedAfter $triggerTime -PollSeconds $PollSeconds -TimeoutSeconds $WaitTimeoutSeconds
        Write-Host ("Database $id : $($wait.State) after $($wait.ElapsedSeconds)s (series $($wait.SeriesId))")
        if($wait.State -notin 'Ready', 'Completed'){
            $exitCode = 1
        }
    }
}

disconnect-polaris
Exit $exitCode
