<#

.SYNOPSIS
This script is used to give a listing of snapshots on the cluster. It will produce a CSV file for each object type with archive and replication information for each snapshot. If desired this script can also be used to pull size estimations on a snapshot level. 
Because this script pulls all of the snapshot data for each object specified it can be used for far more than just Archive lag information. The script currently supports VMware, Fileset, MSSQL, HyperV, EC2, VolumeGroup, Snappable Oracle, Nutanix VMs and ManagedVolume Objects.

If you are unsure of what objects exist in the environment simply specify the all object flag (AllObjects) and the script will check.  

.EXAMPLE

.\Get-SnapshotDetails.ps1 -threadcount 16 -rubrikAddress 10.35.18.192 -VMware -IncludeAll                                                                            
cmdlet Get-SnapshotDetails.ps1 at command pipeline position 1
Supply values for the following parameters:
(Type !? for Help.)
RubrikCredential: admin

PowerShell credential request
Enter your credentials.
Password for user admin: *********

This will run the script with 16 threads to review the snapshots for VMware objects. If a credentials file is not specified the script will request credentials before running. 

.EXAMPLE

.\Get-SnapshotDetails.ps1 -threadcount 16 -rubrikAddress 10.35.18.192 -AllObjects                                                                            
cmdlet Get-SnapshotDetails.ps1 at command pipeline position 1
Supply values for the following parameters:
(Type !? for Help.)
RubrikCredential: admin

PowerShell credential request
Enter your credentials.
Password for user admin: *********

This will run the script and have it look for and determine what object types are in place on the cluster. This is the most commonly used syntax. 

.EXAMPLE

.\Get-SnapshotDetails.ps1 -threadcount 16 -rubrikAddress 10.35.18.192 -SingleObject -ObjectID VirtualMachine:::c4798261-4798-4fcb-877e-d036843cfeab-vm-579                                                                             
cmdlet Get-SnapshotDetails.ps1 at command pipeline position 1
Supply values for the following parameters:
(Type !? for Help.)
RubrikCredential: admin

PowerShell credential request
Enter your credentials.
Password for user admin: *********

This will run the script and have it look at only the VMware VM with ID VirtualMachine:::c4798261-4798-4fcb-877e-d036843cfeab-vm-579.

.EXAMPLE

.\Get-SnapshotDetails.ps1 -threadcount 16 -rubrikAddress 10.35.18.192 -SingleObject -ObjectID VirtualMachine:::c4798261-4798-4fcb-877e-d036843cfeab-vm-579 -StorageStats                                                                             
cmdlet Get-SnapshotDetails.ps1 at command pipeline position 1
Supply values for the following parameters:
(Type !? for Help.)
RubrikCredential: admin

PowerShell credential request
Enter your credentials.
Password for user admin: *********

This will run the script and have it look at only the VMware VM with ID VirtualMachine:::c4798261-4798-4fcb-877e-d036843cfeab-vm-579. By supplying the -StorageStats flag this will also calculate storage space use on a per snapshot level for this individual object. It is suggested that -StorageStats only be used against a single object at a time due to how resource intensive the storage calculation can be.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : July 22, 2021
    Company : Rubrik Inc

#>

[cmdletbinding(DefaultParameterSetName="UseCreds")]
param (
    [parameter(Mandatory=$false)]
    [string]$threadcount,
    [parameter(Mandatory=$true)]
    [string]$rubrikAddress,
    [parameter(Mandatory=$true, ParameterSetName = "UseToken")]
    [string]$rubtok,
    [parameter(Mandatory=$false)]
    [switch]$VMware,
    [parameter(Mandatory=$false)]
    [switch]$Fileset,
    [parameter(Mandatory=$false)]
    [switch]$MSSQL,
    [parameter(Mandatory=$false)]
    [switch]$HyperV,
    [parameter(Mandatory=$false)]
    [switch]$EC2_Instance,
    [parameter(Mandatory=$false)]
    [switch]$VolumeGroup,
    [parameter(Mandatory=$false)]
    [switch]$ManagedVolume,
    [parameter(Mandatory=$false)]
    [switch]$OracleDB,
    [parameter(Mandatory=$false)]
    [switch]$Nutanix,
    [parameter(mandatory=$true, ParameterSetName = "UseCreds",HelpMessage ="Enter admin user ID")]
    $RubrikCredential,
    [parameter(Mandatory=$false)]
    [switch]$IncludeAll,
    [parameter(Mandatory=$false)]
    [switch]$StorageStats,
    [parameter(Mandatory=$false)]
    [switch]$AllObjects,
    [parameter(Mandatory=$false)]
    [switch]$SingleObject,
    [parameter(Mandatory=$false)]
    [string]$ObjectID,
    [parameter(Mandatory=$false)]
    [switch]$GatherSLAInfo,
    [switch]$customlist,
    [switch]$StorageReview,
    [string]$pathtocustomlist
    )



if($AllObjects){
    [System.Boolean]$VMware = $true
    [System.Boolean]$Fileset = $true
    [System.Boolean]$MSSQL = $true
    [System.Boolean]$HyperV = $true
    [System.Boolean]$EC2_Instance = $true
    [System.Boolean]$VolumeGroup = $true
    [System.Boolean]$ManagedVolume = $true
    [System.Boolean]$OracleDB = $true
    [System.Boolean]$Nutanix = $true

}

if($RubrikCredential) {
    try {
        Import-Module Rubrik
    }
    
    catch {
        Write-Output ("Could not import the Rubrik module.  Exiting")
        exit 
    }
    if(!$RubrikCredential) {
        $RubrikCredential = Get-Credential -Message "Enter Rubrik credential"
    }
    Connect-Rubrik $rubrikAddress -Credential $RubrikCredential
    $RubrikToken =  $rubrikConnection.header 
} 
    else {
        $RubrikToken =  @{'Authorization' = ("Bearer $rubtok")}
    }

$PowershellVersion = ($PSVersionTable.PSVersion).major
if($PowershellVersion -lt 6){
    Write-Host "Powershell Core (Powershell version 6 or higher) is required to use this script. Please install Powershell Core using the link below before proceeding:
    
    https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell-core-on-windows?view=powershell-6

    "
    Exit
}
#define where the CSV files will be dropped on the system. 
$Output_directory = (Get-Location).path
$startTime = Get-Date
$mdate = (Get-Date).tostring("yyyyMMddHHmm")
if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("
        Setting thread count to 16 instead")
        $threadCount = 16
    }


$globalThreadCount = $threadcount
if($globalThreadCount -gt 16) {
    Write-Output ("Maximum thread count of 16 is recommended for most use cases. Allowing user override for additional threads")
}

$GetStorageStats = $false
if($StorageStats){
    $GetStorageStats = $true
}
#Clearing the existing job list
Get-Job | Stop-Job
Get-Job | Remove-Job    

Write-Host "GetSnapshotDetails"

if($GatherSLAInfo){
    #Breaking the SLA Domain generation into its own switch so that it can be run separately from the rest of the script and then fed back to the script via CSV to save time.
    $Rubrikinfo = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/cluster/me") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
    $RubrikVersion = $Rubrikinfo.version
    Write-Host "Gathering SLA Domain Information"
    if($RubrikVersion -lt 4){
        $SLA_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/sla_domain") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
    }
    if($RubrikVersion -gt 5){
        $SLA_incomplete_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v2/sla_domain") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
        $SLA_Count = ($SLA_incomplete_list | Measure-Object).Count
        $SLA_list = @()
        Write-Host ("Finished gathering SLA information for " + $SLA_Count + " SLAs. Parsing for the relevant fields.")

        $SLA_IndexCount = 1
        foreach($SLA in $SLA_incomplete_list){
            $SLA_info = New-Object psobject
            Write-Host ("Parsing data for SLA " + $SLA.name + ". This is SLA " + $SLA_IndexCount + " out of " + $SLA_Count + " total SLAs on this cluster.")
            $single_sla_data = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v2/sla_domain/" + $SLA.id) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
            $SLA_info | Add-Member -NotePropertyName "id" -NotePropertyValue $single_sla_data.id
            $SLA_info | Add-Member -NotePropertyName "name" -NotePropertyValue $single_sla_data.name
            $SLA_info | Add-Member -NotePropertyName "frequencies" -NotePropertyValue $single_sla_data.frequencies
            $SLA_info | Add-Member -NotePropertyName "allowedBackupWindows" -NotePropertyValue $single_sla_data.allowedBackupWindows
            $SLA_info | Add-Member -NotePropertyName "firstFullAllowedBackupWindows" -NotePropertyValue $single_sla_data.firstFullAllowedBackupWindows
            $SLA_info | Add-Member -NotePropertyName "localRetentionLimit" -NotePropertyValue $single_sla_data.localRetentionLimit
            $SLA_info | Add-Member -NotePropertyName "maxLocalRetentionLimit" -NotePropertyValue $single_sla_data.maxLocalRetentionLimit
            $SLA_info | Add-Member -NotePropertyName "archivalSpecs" -NotePropertyValue $single_sla_data.archivalSpecs
            $SLA_info | Add-Member -NotePropertyName "replicationSpecs" -NotePropertyValue $single_sla_data.replicationSpecs
            $SLA_info | Add-Member -NotePropertyName "advancedUiConfig" -NotePropertyValue $single_sla_data.advancedUiConfig
            $SLA_list += $SLA_info
            $SLA_IndexCount++
        }
    }
    Write-Host "Finished parsing SLA information. Generating Snapshot details for the specified objects."
    $SLA_list | Export-Csv -Append -NoTypeInformation ($Output_directory + "/SLADomains_" + $mdate + ".csv")
}

$Rubrikinfo = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/cluster/me") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
$RubrikVersion = $Rubrikinfo.version
Write-Host "Gathering SLA Domain Information"
if($RubrikVersion -lt 4){
    $SLA_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/sla_domain") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
}
if($RubrikVersion -gt 5){
    $SLA_incomplete_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v2/sla_domain") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
    $SLA_Count = ($SLA_incomplete_list | Measure-Object).count
    $SLA_list = @()
    Write-Host ("Finished gathering SLA information for " + $SLA_Count + " SLAs. Parsing for the relevant fields.")

    $SLA_IndexCount = 1
    foreach($SLA in $SLA_incomplete_list){
        $SLA_info = New-Object psobject
        Write-Host ("Parsing data for SLA " + $SLA.name + ". This is SLA " + $SLA_IndexCount + " out of " + $SLA_Count + " total SLAs on this cluster.")
        $single_sla_data = $SLA
        #$single_sla_data = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v2/sla_domain/" + $SLA.id) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
        $SLA_info | Add-Member -NotePropertyName "id" -NotePropertyValue $single_sla_data.id
        $SLA_info | Add-Member -NotePropertyName "name" -NotePropertyValue $single_sla_data.name
        $SLA_info | Add-Member -NotePropertyName "frequencies" -NotePropertyValue $single_sla_data.frequencies
        $SLA_info | Add-Member -NotePropertyName "allowedBackupWindows" -NotePropertyValue $single_sla_data.allowedBackupWindows
        $SLA_info | Add-Member -NotePropertyName "firstFullAllowedBackupWindows" -NotePropertyValue $single_sla_data.firstFullAllowedBackupWindows
        $SLA_info | Add-Member -NotePropertyName "localRetentionLimit" -NotePropertyValue $single_sla_data.localRetentionLimit
        $SLA_info | Add-Member -NotePropertyName "maxLocalRetentionLimit" -NotePropertyValue $single_sla_data.maxLocalRetentionLimit
        $SLA_info | Add-Member -NotePropertyName "archivalSpecs" -NotePropertyValue $single_sla_data.archivalSpecs
        $SLA_info | Add-Member -NotePropertyName "replicationSpecs" -NotePropertyValue $single_sla_data.replicationSpecs
        $SLA_info | Add-Member -NotePropertyName "advancedUiConfig" -NotePropertyValue $single_sla_data.advancedUiConfig
        $SLA_list += $SLA_info
        $SLA_IndexCount++
    }
}
$Rubrikname = $Rubrikinfo.name

Write-Host "Finished parsing SLA information. Generating Snapshot details for the specified objects."
if($SingleObject){
    #Need to figure out what kind of snappable the supplied ID is:
    if($ObjectID -match "HypervVirtualMachine"){
        Write-Host "Snappable ID indicates this is a HyperV Virtual Machine"
        $vm_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/hyperv/vm/" + $ObjectID) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
        $vm_snap_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/hyperv/vm/" + $ObjectID + "/snapshot") -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json).data
        $HyperV_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $vm_snap_info)
        {
            $SLA = $SLA_list | where-object {$_.id -eq $snapshot.slaId}

            #convert protected date to Time range by day
            $date1 = $vm.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($vm_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0]     

            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.vmName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $vm_list.id
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $Rubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #Switch to add in storage stats to the snapshot lists
            if($StorageStats -eq $true){
                $storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?SnappableId="+ $vm_list.id) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                $snapshot_stats | Add-Member -NotePropertyName LogicalGBytes -NotePropertyValue ($storage_stats.LogicalBytes/1GB)
                $snapshot_stats | Add-Member -NotePropertyName IngestedGBytes -NotePropertyValue ($storage_stats.IngestedBytes/1GB)
                $snapshot_stats | Add-Member -NotePropertyName PhysicalGBytes -NotePropertyValue ($storage_stats.physicalBytes/1GB)
                $snapshot_stats | Add-Member -NotePropertyName HistoricIngestedGBytes -NotePropertyValue ($storage_stats.historicIngestedBytes/1GB)
            }
            $HyperV_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $HyperV_stats

        $dumpedToCsv = $false
        $retryCount = 0


                $fileTestPath = $Output_directory + "/listof_HyperV_snaps-" + $mdate + ".csv"
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_HyperV_snaps-" + $mdate + ".csv")
                $dumpedToCsv = $true

        
     } 
     if($ObjectID -match "VirtualMachine"){
        Write-Host "Snappable ID indicates this is a VMware Virtual Machine"
        $vm_info = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/vmware/vm/" + $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json

        $vm_snap_info = $vm_info.snapshots
        $VMware_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $vm_snap_info){

            $SLA = $SLA_list | where-object {$_.id -eq $snapshot.slaId}
            #convert protected date to Time range by day
            $date1 = $vm.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($vm_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0]     


            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.vmName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $ObjectID
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $VM.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "VM_Protection_Date" -NotePropertyValue $vm.protectionDate
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Retention_Tag" -NotePropertyValue ""
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }

                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            if($StorageStats){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($RubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            
           
            
#Wait-Debugger
            $VMware_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $VMware_stats

        $dumpedToCsv = $false
        $retryCount = 0
        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "VMwareMutex")
            $mutex.WaitOne();
            Try { 
                $fileTestPath = $Output_directory + "/listof_VMware_snaps-" + $mdate + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_VMware_snaps-" + $mdate + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)
                }
            Finally{
                $mutex.ReleaseMutex()
                }
            

        }
    }
    if($ObjectID -match "VolumeGroup"){
        Write-Host "Snappable ID indicates this is a Windows Host VolumeGroup"
        if($RubrikVersion -lt 5.3){
            $volume_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/volume_group/" + $ObjectID) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
            $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/volume_group/" + $ObjectID + "/snapshot") -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json).data
        }
        if($RubrikVersion -gt 5.3){
            $volume_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/volume_group/" + $ObjectID) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
            $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/volume_group/" + $ObjectID + "/snapshot") -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json).data
        }

                    $volume_stats = @()
                    $Good_Snaps_list = @()
                    ForEach($snapshot in $volume_snap_info)
                    {
                        $SLA = $SLA_list | where-object {$_.id -eq $snapshot.slaId}
                                    #convert protected date to Time range by day
                                    $date1 = $vm.protectionDate
                                    $date2 = $snapshot.date
                                    if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                        $date1 = ($volume_snap_info | select-object -first 1).date
                                    }
                                    $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                                    $timespan = ("$timespan").split('.')[0]  

                                    $replicationSpecs = $SLA.replicationSpecs 

                        $snapshot_stats = New-Object psobject
                        $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $volume_info.name
                        $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $volume_info.id
                        $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                        $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                        $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
                        $replication_ids = $snapshot.replicationLocationIds -join ","
                        if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                        }
                        if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                        }
                        if($snapshot_stats.PrimaryClusterID -ne $Rubrikinfo.id){
                            $snapshot_stats.Replication_Configured = "ReplicaCopy"
                        }
                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                        $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                        $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                        $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

                        if($snapshot_stats.Replication_Configured -eq "TRUE"){
                            if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                                $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                            }
                        }

                        #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
                      #  if(-not [string]::IsNullOrEmpty($SLA.archivalSpecs)){
                            $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                            $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                            $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                            $now = Get-date
                            #Wait-Debugger
                            $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                            $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                            $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                            $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                            #Adding logic to take into account snapshots that have already been archived. 
                            if($snapshot_stats.Cloud_State -ne "0"){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                            }
                            if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                           }    
                        #}
            
                        #Wait-Debugger
                        #Switch to add in storage stats to the snapshot lists
                        if($StorageStats -eq $true){
                            $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                            if($snapshot.cloudState -ne "2"){
                                Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                                $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                                $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                                $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                                $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                                $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
                            }
                        }
                        if($RubrikVersion -ge 5.2){
                            $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                            $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime
            
                        }
                      # Wait-Debugger
                        $volume_stats +=$snapshot_stats
            
                    }
                    #Wait-Debugger
                    $Good_Snaps_list += $volume_stats

                    $dumpedToCsv = $false
                    $retryCount = 0

                            $fileTestPath = $Output_directory + "/listof_VolumeGroup_snaps-" + $mdate + ".csv"
                            $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_VolumeGroup_snaps-" + $mdate + ".csv")
                            $dumpedToCsv = $true
    }
    if($ObjectID -match "ManagedVolume"){
        Write-Host "Snappable ID indicates this is a ManagedVolume"
        $volume_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/managed_volume/" + $ObjectID) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
        $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/managed_volume/" + $ObjectID + "/snapshot") -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json).data
                    $volume_stats = @()
                    $Good_Snaps_list = @()
                    ForEach($snapshot in $volume_snap_info)
                    {
                        $SLA = $SLA_list | where-object {$_.id -eq $snapshot.slaId}
                                    #convert protected date to Time range by day
                                    $date1 = $vm.protectionDate
                                    $date2 = $snapshot.date
                                    if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                        $date1 = ($volume_snap_info | select-object -first 1).date
                                    }
                                    $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                                    $timespan = ("$timespan").split('.')[0]  

                                    $replicationSpecs = $SLA.replicationSpecs 

                        $snapshot_stats = New-Object psobject
                        $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $volume_info.name
                        $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $volume_info.id
                        $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                        $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                        $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
                        $replication_ids = $snapshot.replicationLocationIds -join ","
                        if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                        }
                        if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                        }
                        if($snapshot_stats.PrimaryClusterID -ne $Rubrikinfo.id){
                            $snapshot_stats.Replication_Configured = "ReplicaCopy"
                        }
                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                        $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                        $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                        $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

                        if($snapshot_stats.Replication_Configured -eq "TRUE"){
                            if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                                $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                            }
                        }

                        #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
                      #  if(-not [string]::IsNullOrEmpty($SLA.archivalSpecs)){
                            $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                            $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                            $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                            $now = Get-date
                            #Wait-Debugger
                            $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                            $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                            $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                            $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                            #Adding logic to take into account snapshots that have already been archived. 
                            if($snapshot_stats.Cloud_State -ne "0"){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                            }
                            if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                           }    
                        #}
            
                        #Wait-Debugger
                        #Switch to add in storage stats to the snapshot lists
                        if($StorageStats -eq $true){
                            $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                            if($snapshot.cloudState -ne "2"){
                                Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                                $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                                $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                                $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                                $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                                $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
                            }
                        }
                        if($RubrikVersion -ge 5.2){
                            $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                            $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime
            
                        }
                      # Wait-Debugger
                        $volume_stats +=$snapshot_stats
            
                    }
                    #Wait-Debugger
                    $Good_Snaps_list += $volume_stats

                    $dumpedToCsv = $false
                    $retryCount = 0

                            $fileTestPath = $Output_directory + "/listof_ManagedVolume_snaps-" + $mdate + ".csv"
                            $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_ManagedVolume_snaps-" + $mdate + ".csv")
                            $dumpedToCsv = $true

                        
     

    }
    if($ObjectID -match "MssqlDatabase"){
        Write-Host "Snappable ID indicates this is a MSSQL Database"

        $db_info = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/mssql/db" + $ObjectID)  -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
        $db_snap_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/mssql/db/" + $ObjectID + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json
        $MSSQL_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $db_snap_info)
        {
                        #convert protected date to Time range by day
                        $date1 = $db.protectionDate
                        $date2 = $snapshot.date
                        if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                            $date1 = ($db_snap_info | select-object -first 1).date
                        }
                        $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                        $timespan = ("$timespan").split('.')[0]     

            $SLA = $SLA_list | where-object {$_.id -eq $snapshot.slaId}
            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db_info.name
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $db_info.id
            $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue ($db_info.rootProperties).rootName
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $Rubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""      
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }

            #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
			#Switch to add in storage stats to the snapshot lists
            if($Storage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)

            }
            if($RubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
                        if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
            
			$MSSQL_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $MSSQL_stats
        $dumpedToCsv = $false
        $retryCount = 0

                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_SQL_snaps-" + $startTime + ".csv")
                $dumpedToCsv = $true  


    }
    if($ObjectID -match "Fileset"){
        Write-Host "Snappable ID indicates this is a Fileset"
        $Fileset_info = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/fileset/" + $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck ).content | ConvertFrom-Json
        $Fileset_snap_info = $Fileset_info.snapshots
        $Fileset_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $Fileset_snap_info)
        {
            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            $date1 = $Fileset.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($Fileset_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0] 

            $replicationSpecs = $SLA.replicationSpecs 

            $snapshot_stats = New-Object psobject
            $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.FilesetName
            $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue $Fileset.hostName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $Fileset.id
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            #$snapshot_stats | Add-Member -NotePropertyName "Snapshot_Retention_Tag" -NotePropertyValue ""
            #$snapshot_stats | Add-Member -NotePropertyName "Expiration_Date" -NotePropertyValue ""
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $Rubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
            #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
           # if(-not [string]::IsNullOrEmpty($SLA.archivalSpecs)){
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                #Wait-Debugger
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #}
            #Switch to add in storage stats to the snapshot lists 
            if($Storage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $ObjectID) -Method GET -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($RubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
          # Wait-Debugger
            $Fileset_stats += $snapshot_stats

		} # End of individual Fileset job loop
        $Good_Snaps_list += $Fileset_stats

        $dumpedToCsv = $false
        $retryCount = 0



                $fileTestPath = $Output_directory + "/listof_Fileset_snaps-" + $startTime + ".csv"

                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($Output_directory + "/listof_Fileset_snaps-" + $startTime + ".csv")
                $dumpedToCsv = $true

    }
    if($ObjectID -match "CloudNativeVirtualMachine"){
        Write-Host "Snappable ID indicates this is a Cloud Native VM"
    }
    }







if($VMware){
$threadcount = $globalThreadCount    
Write-Output "
Gathering information on VMware VMs"
$VMware_objects = @()
$VMwareObjectList = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/vmware/vm?limit=9999") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
$VMware_objects += $VMwareObjectList.data
 $offset = "9999"
while ($VMwareObjectList.hasMore -eq $True){
    Write-Host "Additional VMware Objects Found. Issuing another API call to pull up to an additional 9999 VMs."
    $VMwareObjectList = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/vmware/vm?limit=9999&offset=" + $offset) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
    $VMware_objects += $VMwareObjectList.data
    $offset = ($VMware_objects | measure-object).Count
    $lastvmwareObject = ($VMware_objects | Select-Object -Last 1).id
    Write-host ("Need to issue another API call starting after VM " + $lastvmwareObject)
}
$vm_list = $VMware_objects
if(!($IncludeAll)){
    $vmsToCheck = $vm_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
}
if($IncludeAll){
    $vmsToCheck = $vm_list
}
if($customlist){
    $vmsToCheck = Import-Csv $pathtocustomlist
}
$vmCount = ($vmsToCheck | Measure-Object).Count
Write-Output ("Will check " + $vmCount + " VMs")

if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1 because the threadcount was not specified.
    If additional threads are desired please cancel this run and specify the threadcount using the -threadcount flag.")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("Setting thread count to 16 instead")
        $threadCount = 16
    }
        elseif ($vmCount -lt $threadcount) {
            Write-Output ("Setting thread count to match VM count of " + $vmCount)
            $threadcount = $vmCount
        }

if($vmCount -eq 0) {
    Write-Output ("There are no active VMware VMs. Skipping to the end.")
    $threadcount = 1
}
if($vmCount -ne 0) {
[int]$quotient = $vmCount / $threadCount
$quotient = [math]::Floor($quotient)
[int]$loopCount = 0

$masterVMList = @()

#create the list of VMs
while($loopCount -lt $threadCount) {
    $childVMList = @()
    for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $vmCount; $i++) {
        if ($null -eq $vmsTocheck[$i]) {
            break
        }
        $childVMList += $vmsToCheck[$i]
        }
    $masterVMList += @{'vmList' = $childVMList}
    $loopCount++
}

$vmJobsList = @()
for($i = 0;$i -le $threadCount;$i++) {

    $vmJob = Start-Job -name "VMBackup_Job_$i" -ScriptBlock {
     $jRToken = $args[0]
     $jRAddr = $args[1]
     $jVMList = $args[2] 
     $jSLA_list = $args[3]
     $jworkingdirectory = $args[4]
     $jstartTime = $args[5]
     $jStorage_Stats = $args[6]
     $jRubrikinfo = $args[7]

     $jRubrikVersion = $jRubrikinfo.version
     foreach($vm in $jVMList.vmlist){
#Gather a list of good snaps
        $vm_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/vmware/vm/" + $vm.id) -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json

        $vm_snap_info = $vm_info.snapshots
        $VMware_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $vm_snap_info){

            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            #convert protected date to Time range by day
            $date1 = $vm.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($vm_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0]     

            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.vmName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $VM.id
            $snapshot_stats | Add-Member -NotePropertyName "PrimaryClusterID" -NotePropertyValue $VM.primaryClusterId
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $VM.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "VM_Protection_Date" -NotePropertyValue $vm.protectionDate
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Retention_Tag" -NotePropertyValue ""
            #$snapshot_stats | Add-Member -NotePropertyName "Expiration_Date" -NotePropertyValue ""
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
            
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }

                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                #Wait-Debugger
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #Switch to add in storage stats to the snapshot lists
            if($jStorage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $vm.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            $VMware_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $VMware_stats

        $dumpedToCsv = $false
        $retryCount = 0
        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "VMwareMutex")
            $mutex.WaitOne();
            Try { 
                $Rubrikname = $jRubrikinfo.name
                $fileTestPath = $jworkingdirectory + "/listof_VMware_snaps-" +$Rubrikname + $jstartTime + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()
                #$Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_VMware_snaps-" + $jstartTime + ".csv")
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_VMware_snaps-" +$Rubrikname + $jstartTime + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)
                }
            Finally{
                $mutex.ReleaseMutex()
                }
            

        }
     } # End of individual VM job loop 
    
    } -ArgumentList $RubrikToken, $rubrikAddress, $masterVMList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
    $vmJobsList += $vmJob

    } 

    Write-Output "
    Created these jobs..."
    Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "VMBackup_Job"} | Format-Table
    Write-Output "
    Will wait on jobs now"
    $jobsOutputList = @()
    $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "VMBackup_Job"} | Wait-Job | Receive-Job

    Write-Output "Cleaning up job list"
    Get-Job | Remove-Job
    Start-Sleep -Seconds 15

    Write-Output "Calculating Archive lag by number of snapshots"
    $VM_csv_data = Import-Csv ($Output_directory + "/listof_VMware_snaps-" + $Rubrikname + $mdate + ".csv")
    $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}
    $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
    Write-Output ("There are " + $Number_of_VM_snaps_behind + " VMware VM snapshots contributing to Archive Lag") 

} #End of IF Statement
} #End VMware Flag
if($MSSQL){
$threadcount = $globalThreadCount
Write-Output "
Gathering information on SQL DBs"
$db_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/mssql/db?limit=9999")  -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data

$dbsToCheck = $db_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
if(!($IncludeAll)){
    $dbsToCheck = $db_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
}
if($IncludeAll){

    $dbsToCheck = $db_list
}
$dbCount = ($dbsToCheck | Measure-Object).Count
Write-Output ("Will check " + $dbCount + " DBs")

if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("Setting thread count to 16 instead")
        $threadCount = 16
    }
        elseif ($dbCount -lt $threadcount) {
            Write-Output ("Setting thread count to match DB count of " + $dbCount)
            $threadcount = $dbCount
        }

if($dbCount -eq 0) {
    Write-Output ("There are no active MSSQL DBs. Skipping to the end.")
    $threadcount = 1
}
if($dbCount -ne 0) {

[int]$quotient = $dbCount / $threadCount
$quotient = [math]::Floor($quotient)
[int]$loopCount = 0

$masterDBList = @()

#create the list of DBs
while($loopCount -lt $threadCount) {
    $childDBList = @()
    for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $dbCount; $i++) {
        if ($null -eq $dbsToCheck[$i]) {
            break
        }
        $childDBList += $dbsToCheck[$i]
        }
    $masterDBList += @{'dbList' = $childDbList}
    $loopCount++
}

$dbJobsList = @()
for($i = 0;$i -le $threadCount;$i++) {
    $dbJob = Start-Job -name "DBBackup_Job_$i" -ScriptBlock {
     $jRToken = $args[0]
     $jRAddr = $args[1]
     $jDBList = $args[2] 
     $jworkingdirectory = $args[3]
     $jstarttime = $args[4]
     $jStorage_Stats = $args[5]
     $jSLA_list = $args[6]
     $jRubrikinfo = $args[7]
     
     $jRubrikVersion = $jRubrikinfo.version

     foreach($DB in $jDBList.dblist){
#Gather a list of good snaps
        Try{$db_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/mssql/db/" + $DB.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json}
        Catch{
            (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/mssql/db/" + $DB.id + "/snapshot") -ErrorAction Stop -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json| Tee-Object -Append -FilePath ($jworkingdirectory + "\good_backup_error.txt")
        }
        Finally{

        }


$db_snap_info = $db_info.data
        $MSSQL_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $db_snap_info)
        {
                        #convert protected date to Time range by day
                        $date1 = $db.protectionDate
                        $date2 = $snapshot.date
                        if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                            $date1 = ($db_snap_info | select-object -first 1).date
                        }
                        $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                        $timespan = ("$timespan").split('.')[0]     

            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db.name
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $db.id
            $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue ($db.rootProperties).rootName
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $db.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
            #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    

			#Switch to add in storage stats to the snapshot lists
            if($jStorage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $db.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }            
			$MSSQL_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $MSSQL_stats
        $dumpedToCsv = $false
        $retryCount = 0

        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "SQLMutex")
            $mutex.WaitOne();
            Try { 
                $Rubrikname = $jRubrikinfo.name
                $fileTestPath = $jworkingdirectory + "/listof_SQL_snaps-" + $Rubrikname + $jstartTime + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()

                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_SQL_snaps-" + $Rubrikname + $jstartTime + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)

                }
            Finally{
                $mutex.ReleaseMutex()
                }         

        }
     } #End of IF Statement
     } -ArgumentList $RubrikToken, $rubrikAddress, $masterDBList[$i], $Output_directory, $mdate, $GetStorageStats, $SLA_list, $Rubrikinfo # End of individual DB job loop 
    
    }  #End of script block
    $dbJobsList += $dbJob
    
    



    Write-Output "
    Created these jobs..."
    Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "DBBackup_Job"} | Format-Table
    Write-Output "
    Will wait on jobs now"
    $jobsOutputList = @()
    $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "DBBackup_Job"} | Wait-Job | Receive-Job
    Start-sleep -Seconds 5

    Write-Output "
    Cleaning up job list"
    Get-Job | Remove-Job
    Start-Sleep -Seconds 15

    Write-Output "Calculating Archive lag by number of snapshots"
    $SQL_csv_data = Import-Csv ($Output_directory + "/listof_SQL_snaps-"+ $Rubrikname + $mdate + ".csv")
    $SQLDB_snaps_behind = $SQL_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}
    $Number_of_SQLDB_snaps_behind = ($SQLDB_snaps_behind | Measure-Object).count
    Write-Output ("There are " + $Number_of_SQLDB_snaps_behind + " SQL DB snapshots contributing to Archive Lag") 


    $dumpedToCsv = $false
    $retryCount = 0
}
}#End of SQL switch 
#Fix the API calls within the Fileset Loop! They still are still using the VMware VM Templates
if($Fileset){
$threadcount = $globalThreadCount
    Write-Output "
Gathering information on Filesets"
$Fileset_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/fileset") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data

if(!($IncludeAll)){
    $FilesetsToCheck = $Fileset_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
}
if($IncludeAll){
    $FilesetsToCheck = $Fileset_list
}
$FilesetCount = ($FilesetsToCheck | Measure-Object).Count

if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("Setting thread count to 16 instead")
        $threadCount = 16
    }
        elseif ($FilesetCount -lt $threadcount) {
            Write-Output ("Setting thread count to match Fileset count of " + $FilesetCount)
            $threadcount = $FilesetCount
        }

if($FilesetCount -eq 0) {
    Write-Output ("There are no active Filesets. Skipping to the end.")
    $threadcount = 1
}
if($FilesetCount -ne 0) {

Write-Output ("Will check " + $FilesetCount + " Filesets")

[int]$quotient = $FilesetCount / $threadCount
$quotient = [math]::Floor($quotient)
[int]$loopCount = 0

$masterFilesetList = @()

#create the list of Filesets
while($loopCount -lt $threadCount) {
    $childFilesetList = @()
    for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $FilesetCount; $i++) {
        if($null -eq $FilesetsToCheck[$i]) {
            break
        }
        $childFilesetList += $FilesetsToCheck[$i]
        }
    $masterFilesetList += @{'FilesetList' = $childFilesetList}
    $loopCount++
}

$FilesetJobsList = @()



$FilesetJobsList = @()
for($i = 0;$i -le $threadCount;$i++) {

    $FilesetJob = Start-Job -name "FilesetBackup_Job_$i" -ScriptBlock {
     $jRToken = $args[0]
     $jRAddr = $args[1]
     $jFilesetList = $args[2] 
     $jSLA_list = $args[3]
     $jworkingdirectory = $args[4]
     $jstartTime = $args[5]
     $jStorage_Stats = $args[6]
     $jRubrikinfo = $args[7]

     $jRubrikVersion = $jRubrikinfo.version

     foreach($Fileset in $jFilesetList.Filesetlist){
#Gather a list of good snaps
        try{
            $Fileset_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/fileset/" + $fileset.id) -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json
        }
        catch{
            $Fileset_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/fileset/" + $fileset.id) -ErrorAction Stop -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json
        }
        Finally{

        }

        $Fileset_snap_info = $Fileset_info.snapshots
        $Fileset_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $Fileset_snap_info)
        {
            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            #convert protected date to Time range by day
            $date1 = $Fileset.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($Fileset_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0] 

            $replicationSpecs = $SLA.replicationSpecs 

            $snapshot_stats = New-Object psobject
            $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.FilesetName
            $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue $Fileset.hostName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $Fileset.id
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $Fileset.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
            #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
           # if(-not [string]::IsNullOrEmpty($SLA.archivalSpecs)){
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #}
            #Switch to add in storage stats to the snapshot lists 
            if($jStorage_Stats -eq $true){
                #Wait-Debugger
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $fileset.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            $Fileset_stats += $snapshot_stats

		} # End of individual Fileset job loop
        $Good_Snaps_list += $Fileset_stats

        $dumpedToCsv = $false
        $retryCount = 0
        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "FilesetMutex")
            $mutex.WaitOne();
            Try { 
                $Rubrikname = $jRubrikinfo.name
                $fileTestPath = $jworkingdirectory + "/listof_Fileset_snaps-" + $Rubrikname + $jstartTime + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_Fileset_snaps-" + $Rubrikname + $jstartTime + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)
                }
            Finally{
                    $mutex.ReleaseMutex()
                }
            

        } #End of try to append to CSV loop.
          
     } # End of IF Statement
     } -ArgumentList $RubrikToken, $rubrikAddress, $masterFilesetList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo   
    
    }  #End of script block 
    $FilesetJobsList += $FilesetJob
    
    
    Write-Output "
    Created these jobs..."
    Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "FilesetBackup_Job"} | Format-Table
    Write-Output "
    Will wait on jobs now"
    $jobsOutputList = @()
    $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "FilesetBackup_Job"} | Wait-Job | Receive-Job

    Write-Output "
    Cleaning up job list"
    Get-Job | Remove-Job
    Start-Sleep -Seconds 15   
    
    Write-Output "Calculating Archive lag by number of snapshots"
    $Fileset_csv_data = Import-Csv ($Output_directory + "/listof_Fileset_snaps-" + $Rubrikname + $mdate + ".csv")
    $Fileset_snaps_behind = $Fileset_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
    $Number_of_Fileset_snaps_behind = ($Fileset_snaps_behind| Measure-Object).count
    Write-Output ("There are " + $Number_of_Fileset_snaps_behind + " Fileset snapshots contributing to Archive Lag") 
}
} #end of Fileset Loop
if($HyperV){
$threadcount = $globalThreadCount
Write-Output "
Gathering information on HyperV VMs"
$vm_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/hyperv/vm?limit=9999") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data

if(!($IncludeAll)){
    $vmsToCheck = $vm_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
}

if($IncludeAll){
    $vmsToCheck = $vm_list
}
$vmCount = ($vmsToCheck | Measure-Object).Count
Write-Output ("Will check " + $vmCount + " VMs")

if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("Setting thread count to 16 instead")
        $threadCount = 16
    }
        elseif ($vmCount -lt $threadcount) {
            Write-Output ("Setting thread count to match VM count of " + $vmCount)
            $threadcount = $vmCount
        }

if($vmCount -eq 0) {
    Write-Output ("There are no active HyperV VMs. Skipping to the end.")
    $threadcount = 1
}
if($vmCount -ne 0) {

[int]$quotient = $vmCount / $threadCount
$quotient = [math]::Floor($quotient)
[int]$loopCount = 0

$masterVMList = @()

#create the list of VMs
while($loopCount -lt $threadCount) {
    $childVMList = @()
    for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $vmCount; $i++) {
        if ($null -eq $vmsTocheck[$i]) {
            break
        }
        $childVMList += $vmsToCheck[$i]
        }
    $masterVMList += @{'vmList' = $childVMList}
    $loopCount++
}

$vmJobsList = @()

$vmJobsList = @()
for($i = 0;$i -le $threadCount;$i++) {
    $vmJob = Start-Job -name "HyperVBackup_Job_$i" -ScriptBlock {
     $jRToken = $args[0]
     $jRAddr = $args[1]
     $jVMList = $args[2] 
     $jSLA_list = $args[3]
     $jworkingdirectory = $args[4]
     $jstartTime = $args[5]
     $jStorage_Stats = $args[6]
     $jRubrikinfo = $args[7]

     $jRubrikVersion = $jRubrikinfo.version


     foreach($vm in $jVMList.vmlist){
#Gather a list of good snaps
        $vm_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/hyperv/vm/" + $vm.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
        $HyperV_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $vm_snap_info)
        {
            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            #convert protected date to Time range by day
            $date1 = $vm.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($vm_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0]     

            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.vmName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $VM.id
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $VM.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier


            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }
            #Check to see if SLA is tied to an Archive location before adding in the archive fields. 
                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #Switch to add in storage stats to the snapshot lists
            if($jStorage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $VM.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            $HyperV_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $HyperV_stats

        $dumpedToCsv = $false
        $retryCount = 0
        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "HyperVMutex")
            $mutex.WaitOne();
            Try { 
                $Rubrikname = $jRubrikinfo.name
                $fileTestPath = $jworkingdirectory + "/listof_HyperV_snaps-"+ $Rubrikname + $jstartTime + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_HyperV_snaps-"+ $Rubrikname + $jstartTime + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)
                }
            Finally{
                $mutex.ReleaseMutex()
            }

        }
     } # End of individual VM job loop 
    
    } -ArgumentList $RubrikToken, $rubrikAddress, $masterVMList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
    $vmJobsList += $vmJob
    
    } 

    Write-Output "
    Created these jobs..."
    Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "HyperVBackup_Job"} | Format-Table
    Write-Output "
    Will wait on jobs now"
    $jobsOutputList = @()
    $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "HyperVBackup_Job"} | Wait-Job | Receive-Job

    Write-Output "Cleaning up job list"
    Get-Job | Remove-Job
    Start-Sleep -Seconds 15

    Write-Output "Calculating Archive lag by number of snapshots"
    $VM_csv_data = Import-Csv ($Output_directory + "/listof_HyperV_snaps-"+ $Rubrikname + $mdate + ".csv")
    $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
    $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
    Write-Output ("There are " + $Number_of_VM_snaps_behind + " HyperV VM snapshots contributing to Archive Lag") 


} #End of IF Statement

} #End HyperV Switch

if($EC2_Instance){
    $threadcount = $globalThreadCount   
    Write-Output "
    Gathering information on EC2 VMs"
    $vm_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/aws/ec2_instance") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
    
    if(!($IncludeAll)){
        $vmsToCheck = $vm_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
    }
    
    if($IncludeAll){
        $vmsToCheck = $vm_list
    }
    $vmCount = ($vmsToCheck | Measure-Object).Count
    Write-Output ("Will check " + $vmCount + " VMs")
    
    if(!($threadCount)) {
        Write-Output ("
        Setting thread count to 1")
        $threadCount = 1
    }
        elseif ($threadCount -gt 16) {
            Write-Output ("Setting thread count to 16 instead")
            $threadCount = 16
        }
            elseif ($vmCount -lt $threadcount) {
                Write-Output ("Setting thread count to match VM count of " + $vmCount)
                $threadcount = $vmCount
            }
    
    if($vmCount -eq 0) {
        Write-Output ("There are no active EC2 Instances. Skipping to the end.")
        $threadcount = 1
    }

    if($vmCount -ne 0) {
    
    [int]$quotient = $vmCount / $threadCount
    $quotient = [math]::Floor($quotient)
    [int]$loopCount = 0
    
    $masterVMList = @()
    
    #create the list of VMs
    while($loopCount -lt $threadCount) {
        $childVMList = @()
        for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $vmCount; $i++) {
            if ($null -eq $vmsTocheck[$i]) {
                break
            }
            $childVMList += $vmsToCheck[$i]
            }
        $masterVMList += @{'vmList' = $childVMList}
        $loopCount++
    }
    
    $vmJobsList = @()
    for($i = 0;$i -le $threadCount;$i++) {
        $vmJob = Start-Job -name "EC2_Backup_Job_$i" -ScriptBlock {
         $jRToken = $args[0]
         $jRAddr = $args[1]
         $jVMList = $args[2] 
         $jSLA_list = $args[3]
         $jworkingdirectory = $args[4]
         $jstartTime = $args[5]
         $jStorage_Stats = $args[6]
         $jRubrikinfo = $args[7]
    

        $jRubrikVersion = $jRubrikinfo.version

         foreach($vm in $jVMList.vmlist){
    #Gather a list of good snaps
            $vm_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/aws/ec2_instance/" + $vm.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
            $EC2_stats = @()
            $Good_Snaps_list = @()
            ForEach($snapshot in $vm_snap_info)
            {
                $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
                            #convert protected date to Time range by day
                            $date1 = $vm.protectionDate
                            $date2 = $snapshot.date
                            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                $date1 = ($vm_snap_info | select-object -first 1).date
                            }
                            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                            $timespan = ("$timespan").split('.')[0]                     
                
                            $replicationSpecs = $SLA.replicationSpecs 

                $snapshot_stats = New-Object psobject
                $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $VM.instanceName
                $snapshot_stats | Add-Member -NotePropertyName "Instance_ID" -NotePropertyValue $VM.instanceId
                $snapshot_stats | Add-Member -NotePropertyName "Account_Name" -NotePropertyValue $VM.accountName
                $snapshot_stats | Add-Member -NotePropertyName "Region" -NotePropertyValue $VM.Region
                $snapshot_stats | Add-Member -NotePropertyName "Instance_Type" -NotePropertyValue $VM.instanceType
                $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $VM.id
                $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $VM.isRelic
                $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
                $replication_ids = $snapshot.replicationLocationIds -join ","
                if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                    $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                }
                if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                    $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                }
                if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                    $snapshot_stats.Replication_Configured = "ReplicaCopy"
                }
                $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"

                if($snapshot_stats.Replication_Configured -eq "TRUE"){
                    if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                        $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                    }
                }
                    $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                    $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                    $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                    $now = Get-date
                    $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                    $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                    $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                    $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                    if($snapshot_stats.Cloud_State -ne "0"){
                        $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                    }
                    if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                        $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                   }    

              if($jStorage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $VM.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
                $EC2_stats +=$snapshot_stats
    
            }
            $Good_Snaps_list += $EC2_stats
    
            $dumpedToCsv = $false
            $retryCount = 0
            while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
                $mutex = New-Object System.Threading.Mutex($false, "EC2Mutex")
                $mutex.WaitOne();
                Try { 
                    $Rubrikname = $jRubrikinfo.name
                    $fileTestPath = $jworkingdirectory + "/listof_EC2_snaps-" + $Rubrikname + $jstartTime + ".csv"
                    [IO.file]::OpenWrite($fileTestPath).close()
                    $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_EC2_snaps-" + $Rubrikname + $jstartTime + ".csv")
                    $dumpedToCsv = $true
                }
                Catch{
                    Start-Sleep -Seconds 2
                    $retryCount++
                    Write-Output ("retry count at: " + $retryCount)
                    }
                Finally{
                    $mutex.ReleaseMutex()
                }
                
    
            }
         } # End of individual VM job loop 
        
        } -ArgumentList $RubrikToken, $rubrikAddress, $masterVMList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
        $vmJobsList += $vmJob
        
        } 
    
        Write-Output "
        Created these jobs..."
        Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "EC2_Backup_Job"} | Format-Table
        Write-Output "
        Will wait on jobs now"
        $jobsOutputList = @()
        $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "EC2_Backup_Job"} | Wait-Job | Receive-Job
    
        Write-Output "Cleaning up job list"
        Get-Job | Remove-Job
        Start-Sleep -Seconds 15
    
        Write-Output "Calculating Archive lag by number of snapshots"
        $VM_csv_data = Import-Csv ($Output_directory + "/listof_EC2_snaps-" + $mdate + ".csv")
        $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
        $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
        Write-Output ("There are " + $Number_of_VM_snaps_behind + " EC2 Instance snapshots contributing to Archive Lag") 
    
    } #End IF Statement
    } #End EC2 Switch
    
if($VolumeGroup){
    $threadcount = $globalThreadCount
        Write-Output "
        Gathering information on VolumeGroups"
        Write-Host "Gathering SLA Domain Information"
        if($RubrikVersion -lt 5.3){
            $volume_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/volume_group") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
        }
        if($RubrikVersion -gt 5.3){
            $volume_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/volume_group") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
        }

        if(!($IncludeAll)){
            $VolumesToCheck = $volume_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
        }
        
        if($IncludeAll){
            $VolumesToCheck = $volume_list
        }
        $VolumeCount = ($VolumesToCheck | Measure-Object).Count
        Write-Output ("Will check " + $VolumeCount + " Volumes")
        
        if(!($threadCount)) {
            Write-Output ("
            Setting thread count to 1")
            $threadCount = 1
        }
            elseif ($threadCount -gt 16) {
                Write-Output ("Setting thread count to 16 instead")
                $threadCount = 16
            }
                elseif ($VolumeCount -lt $threadcount) {
                    Write-Output ("Setting thread count to match Volume count of " + $VolumeCount)
                    $threadcount = $VolumeCount
                }
        
        if($VolumeCount -eq 0) {
            Write-Output ("There are no active VolumeGroups. Skipping to the end.")
            $threadcount = 1
        }
        if($VolumeCount -ne 0) {
        
        [int]$quotient = $VolumeCount / $threadCount
        $quotient = [math]::Floor($quotient)
        [int]$loopCount = 0
        
        $masterVolumeList = @()
        
        #create the list of Volumes
        while($loopCount -lt $threadCount) {
            $childVolumeList = @()
            for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $VolumeCount; $i++) {
                if ($null -eq $VolumesTocheck[$i]) {
                    break
                }
                $childVolumeList += $VolumesToCheck[$i]
                }
            $masterVolumeList += @{'VolumeList' = $childVolumeList}
            $loopCount++
        }               
        $VolumeJobsList = @()

        for($i = 0;$i -le $threadCount;$i++) {
            $volumeJob = Start-Job -name "Volume_Backup_Job_$i" -ScriptBlock {
             $jRToken = $args[0]
             $jRAddr = $args[1]
             $jVMList = $args[2] 
             $jSLA_list = $args[3]
             $jworkingdirectory = $args[4]
             $jstartTime = $args[5]
             $jStorage_Stats = $args[6]
             $jRubrikinfo = $args[7]

             $jRubrikVersion = $jRubrikinfo.version

             foreach($volume in $jVMList.Volumelist){
        #Gather a list of good snaps
                if($jRubrikVersion -lt 5.3){
                    $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/volume_group/" + $volume.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
                }
                if($jRubrikVersion -gt 5.3){
                    $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/v1/volume_group/" + $volume.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
                }
                
                $volume_stats = @()
                $Good_Snaps_list = @()
                ForEach($snapshot in $volume_snap_info)
                {
                    $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
                                #convert protected date to Time range by day
                                $date1 = $volume.protectionDate
                                $date2 = $snapshot.date
                                if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                    $date1 = ($volume_snap_info | select-object -first 1).date
                                }
                                $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                                $timespan = ("$timespan").split('.')[0]

                                $replicationSpecs = $SLA.replicationSpecs 

                    $snapshot_stats = New-Object psobject
                    $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue $volume.hostname
                    $snapshot_stats | Add-Member -NotePropertyName "Drives" -NotePropertyValue $volume.name
                    $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $volume.id
                    $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                    $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                    $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $volume.isRelic
                    $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date

                    $replication_ids = $snapshot.replicationLocationIds -join ","
                    if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                        $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                    }
                    if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                        $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                    }
                    if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                        $snapshot_stats.Replication_Configured = "ReplicaCopy"
                    }

                    $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                    $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                    $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                    $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                    $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                    $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                    $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                    $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
                    $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier


                    if($snapshot_stats.Replication_Configured -eq "TRUE"){
                        if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                            $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                        }
                    }
                        $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                        $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                        $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                        $now = Get-date
                        $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                        $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                        $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                        $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                        #Adding logic to take into account snapshots that have already been archived. 
                        if($snapshot_stats.Cloud_State -ne "0"){
                            $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                        }
                        if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                            $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                       }    
                    #Switch to add in storage stats to the snapshot lists
                    if($jStorage_Stats -eq $true){
                        $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                            Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                            $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $volume.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                            $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                            $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                            $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                            $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
                    }
                    if($jRubrikVersion -ge 5.2){
                        $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                        $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                        $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                        $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                        $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                        $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime
        
                    }
                    $volume_stats +=$snapshot_stats
        
                }
                $Good_Snaps_list += $volume_stats
        
                $dumpedToCsv = $false
                $retryCount = 0
                while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
                    $mutex = New-Object System.Threading.Mutex($false, "VolumeGroupMutex")
                    $mutex.WaitOne();
                    Try { 
                        $Rubrikname = $jRubrikinfo.name
                        $fileTestPath = $jworkingdirectory + "/listof_VolumeGroup_snaps-" + $Rubrikname + $jstartTime + ".csv"
                        [IO.file]::OpenWrite($fileTestPath).close()
                        $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_VolumeGroup_snaps-" + $Rubrikname + $jstartTime + ".csv")
                        $dumpedToCsv = $true
                    }
                    Catch{
                        Start-Sleep -Seconds 2
                        $retryCount++
                        Write-Output ("retry count at: " + $retryCount)
                        }
                    Finally{
                        $mutex.ReleaseMutex()
                    }
                    
 
                }
             } # End of individual VolumeGroup job loop 
            
            } -ArgumentList $RubrikToken, $rubrikAddress, $masterVolumeList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
            $volumeJobsList += $volumeJob
            
            } 
        
            Write-Output "
            Created these jobs..."
            Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "Volume_Backup_Job"} | Format-Table
            Write-Output "
            Will wait on jobs now"
            $jobsOutputList = @()
            $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "Volume_Backup_Job"} | Wait-Job | Receive-Job
            Write-Output "Cleaning up job list"
            Get-Job | Remove-Job
            Start-Sleep -Seconds 15
        
            Write-Output "Calculating Archive lag by number of snapshots"
            $VM_csv_data = Import-Csv ($Output_directory + "/listof_VolumeGroup_snaps-" + $Rubrikname + $mdate + ".csv")
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " VolumeGroup snapshots contributing to Archive Lag") 
        }#End IF Statement
    } #End VolumeGroup Switch

if($ManagedVolume){
    $threadcount = $globalThreadCount
            Write-Output "
            Gathering information on ManagedVolumes"
            $volume_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/managed_volume") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
            if(!($IncludeAll)){
                $VolumesToCheck = $volume_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
            }
            
            if($IncludeAll){
                $VolumesToCheck = $volume_list
            }
            $VolumeCount = ($VolumesToCheck | Measure-Object).Count
            Write-Output ("Will check " + $VolumeCount + " Volumes")
            
            if(!($threadCount)) {
                Write-Output ("
                Setting thread count to 1")
                $threadCount = 1
            }
                elseif ($threadCount -gt 16) {
                    Write-Output ("Setting thread count to 16 instead")
                    $threadCount = 16
                }
                    elseif ($VolumeCount -lt $threadcount) {
                        Write-Output ("Setting thread count to match Volume count of " + $VolumeCount)
                        $threadcount = $VolumeCount
                    }
            
            if($VolumeCount -eq 0) {
                Write-Output ("There are no active ManagedVolumes. Skipping to the end.")
                $threadcount = 1
            }
            if($VolumeCount -ne 0) {
                            

            [int]$quotient = $VolumeCount / $threadCount
            $quotient = [math]::Floor($quotient)
            [int]$loopCount = 0
            
            $masterVolumeList = @()
            
            #create the list of Volumes
            while($loopCount -lt $threadCount) {
                $childVolumeList = @()
                for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $VolumeCount; $i++) {
                    if ($null -eq $VolumesTocheck[$i]) {
                        break
                    }
                    $childVolumeList += $VolumesToCheck[$i]
                    }
                $masterVolumeList += @{'VolumeList' = $childVolumeList}
                $loopCount++
            }
            
            $VolumeJobsList = @()

            for($i = 0;$i -le $threadCount;$i++) {
                $volumeJob = Start-Job -name "Volume_Backup_Job_$i" -ScriptBlock {
                 $jRToken = $args[0]
                 $jRAddr = $args[1]
                 $jVMList = $args[2] 
                 $jSLA_list = $args[3]
                 $jworkingdirectory = $args[4]
                 $jstartTime = $args[5]
                 $jStorage_Stats = $args[6]
                 $jRubrikinfo = $args[7]
    
                 $jRubrikVersion = $jRubrikinfo.version

                 foreach($volume in $jVMList.Volumelist){
            #Gather a list of good snaps
                    $volume_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/managed_volume/" + $volume.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
                    $volume_stats = @()
                    $Good_Snaps_list = @()
                    ForEach($snapshot in $volume_snap_info)
                    {
                        $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
                                    #convert protected date to Time range by day
                                    $date1 = $vm.protectionDate
                                    $date2 = $snapshot.date
                                    if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                        $date1 = ($volume_snap_info | select-object -first 1).date
                                    }
                                    $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                                    $timespan = ("$timespan").split('.')[0]  

                                    $replicationSpecs = $SLA.replicationSpecs 

                        $snapshot_stats = New-Object psobject
                        $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $volume.name
                        $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $volume.id
                        $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                        $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                        $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
                        $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $volume.isRelic

                        $replication_ids = $snapshot.replicationLocationIds -join ","
                        if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                        }
                        if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                            $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                        }
                        if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                            $snapshot_stats.Replication_Configured = "ReplicaCopy"
                        }

                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                        $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                        $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                        $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                        $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                        $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
                        $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier


                        if($snapshot_stats.Replication_Configured -eq "TRUE"){
                            if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                                $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                            }
                        }
                            $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                            $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                            $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                            $now = Get-date
                            $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                            $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                            $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                            $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                            if($snapshot_stats.Cloud_State -ne "0"){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                            }
                            if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                                $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                           }    

                        #Switch to add in storage stats to the snapshot lists
                        if($jStorage_Stats -eq $true){
                            $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                            $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                                Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                                $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $volume.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                                $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                                $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                                $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                                $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
                        }
                        if($jRubrikVersion -ge 5.2){
                            $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                            $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                            $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime
            
                        }
                        $volume_stats +=$snapshot_stats
            
                    }
                    $Good_Snaps_list += $volume_stats
            
                    $dumpedToCsv = $false
                    $retryCount = 0
                    while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
                        $mutex = New-Object System.Threading.Mutex($false, "ManagedVolumeMutex")
                        $mutex.WaitOne();
                        Try { 
                            $Rubrikname = $jRubrikinfo.name
                            $fileTestPath = $jworkingdirectory + "/listof_ManagedVolume_snaps-" + $Rubrikname + $jstartTime + ".csv"
                            [IO.file]::OpenWrite($fileTestPath).close()
                            $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_ManagedVolume_snaps-" + $Rubrikname + $jstartTime + ".csv")
                            $dumpedToCsv = $true
                        }
                        Catch{
                            Start-Sleep -Seconds 2
                            $retryCount++
                            Write-Output ("retry count at: " + $retryCount)
                            }
                        Finally{
                            $mutex.ReleaseMutex()
                        }
                        
     
                    }
                 } # End of individual ManagedVolume job loop 
                
                } -ArgumentList $RubrikToken, $rubrikAddress, $masterVolumeList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
                $volumeJobsList += $volumeJob
                
                } 
            
                Write-Output "
                Created these jobs..."
                Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "Volume_Backup_Job"} | Format-Table
                Write-Output "
                Will wait on jobs now"
                $jobsOutputList = @()
                $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "Volume_Backup_Job"} | Wait-Job | Receive-Job
                Write-Output "Cleaning up job list"
                Get-Job | Remove-Job
                Start-Sleep -Seconds 15
            
                Write-Output "Calculating Archive lag by number of snapshots"
                $VM_csv_data = Import-Csv ($Output_directory + "/listof_ManagedVolume_snaps-" + $Rubrikname + $mdate + ".csv")
                $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}
                $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
                Write-Output ("There are " + $Number_of_VM_snaps_behind + "ManagedVolume snapshots contributing to Archive Lag") 
            } #End of IF statement that stops job if there are no objects of this type. 
    } #End ManagedVolume Switch   
    
if($OracleDB){
    $threadcount = $globalThreadCount
    Write-Output "
    Gathering information on Oracle Snappable DBs"
    $db_list = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/oracle/db?limit=9999")  -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json).data
    
    $dbsToCheck = $db_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
    if(!($IncludeAll)){
        $dbsToCheck = $db_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
    }
    
    if($IncludeAll){
        $dbsToCheck = $db_list
    }
    $dbCount = ($dbsToCheck | Measure-Object).Count
    Write-Output ("Will check " + $dbCount + " DBs")
    
    if(!($threadCount)) {
        Write-Output ("
        Setting thread count to 1")
        $threadCount = 1
    }
        elseif ($threadCount -gt 16) {
            Write-Output ("Setting thread count to 16 instead")
            $threadCount = 16
        }
            elseif ($dbCount -lt $threadcount) {
                Write-Output ("Setting thread count to match DB count of " + $dbCount)
                $threadcount = $dbCount
            }
    
    if($dbCount -eq 0) {
        Write-Output ("There are no active Oracle Snappable DBs. Skipping to the end.")
        $threadcount = 1
    }
    if($dbCount -ne 0) {
    
    [int]$quotient = $dbCount / $threadCount
    $quotient = [math]::Floor($quotient)
    [int]$loopCount = 0
    
    $masterDBList = @()
    
    #create the list of DBs
    while($loopCount -lt $threadCount) {
        $childDBList = @()
        for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $dbCount; $i++) {
            if ($null -eq $dbsToCheck[$i]) {
                break
            }
            $childDBList += $dbsToCheck[$i]
            }
        $masterDBList += @{'dbList' = $childDbList}
        $loopCount++
    }

    $dbJobsList = @()
    for($i = 0;$i -le $threadCount;$i++) {
        $dbJob = Start-Job -name "DBBackup_Job_$i" -ScriptBlock {
         $jRToken = $args[0]
         $jRAddr = $args[1]
         $jDBList = $args[2] 
         $jworkingdirectory = $args[3]
         $jstarttime = $args[4]
         $jStorage_Stats = $args[5]
         $jSLA_list = $args[6]
         $jRubrikinfo = $args[7]
         
         $jRubrikVersion = $jRubrikinfo.version

         foreach($DB in $jDBList.dblist){
    #Gather a list of good snaps
            Try{$db_info = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/oracle/db/" + $DB.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json}
            Catch{
                (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/oracle/db/" + $DB.id + "/snapshot") -ErrorAction Stop -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json| Tee-Object -Append -FilePath ($jworkingdirectory + "\good_backup_error.txt")
            }
            Finally{
    
            }
    
    
    $db_snap_info = $db_info.data
            $Oracle_stats = @()
            $Good_Snaps_list = @()
            ForEach($snapshot in $db_snap_info)
            {
                            #convert protected date to Time range by day
                            $date1 = $db.protectionDate
                            $date2 = $snapshot.date
                            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                                $date1 = ($db_snap_info | select-object -first 1).date
                            }
                            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
                            $timespan = ("$timespan").split('.')[0]     
    
                $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}

                $replicationSpecs = $SLA.replicationSpecs 

                $snapshot_stats = New-Object psobject
                $snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db.name
                $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $db.id
                $snapshot_stats | Add-Member -NotePropertyName "Hostname" -NotePropertyValue $db.standaloneHostName
                $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
                $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
                $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $db.isRelic
                $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
                $replication_ids = $snapshot.replicationLocationIds -join ","
                if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                    $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
                }
                if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                    $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
                }
                if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                    $snapshot_stats.Replication_Configured = "ReplicaCopy"
                }
                $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
                $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
                $snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
                $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
                $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
                $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
                $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier


                if($snapshot_stats.Replication_Configured -eq "TRUE"){
                    if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                        $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                    }
                }
                    $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                    $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                    $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                    $now = Get-date
                    $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                    $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                    $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                    $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                    #Adding logic to take into account snapshots that have already been archived. 
                    if($snapshot_stats.Cloud_State -ne "0"){
                        $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                    }
                    if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                        $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                   }    
                #Switch to add in storage stats to the snapshot lists
                if($jStorage_Stats -eq $true){
                    $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                    $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                        Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                        $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $db.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                        $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                        $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                        $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                        $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
                }
                if($jRubrikVersion -ge 5.2){
                    $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                    $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                    $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                    $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                    $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                    $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime
    
                }
                
                $Oracle_stats +=$snapshot_stats
    
            }
            $Good_Snaps_list += $Oracle_stats
            $dumpedToCsv = $false
            $retryCount = 0
    
            while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
                $mutex = New-Object System.Threading.Mutex($false, "OracleSnapMutex")
                $mutex.WaitOne();
                Try { 
                    $Rubrikname = $jRubrikinfo.name
                    $fileTestPath = $jworkingdirectory + "/listof_Oracle_snaps-" + $Rubrikname + $jstartTime + ".csv"
                    [IO.file]::OpenWrite($fileTestPath).close()
                    $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_Oracle_snaps-" + $Rubrikname + $jstartTime + ".csv")
                    $dumpedToCsv = $true
                }
                Catch{
                    Start-Sleep -Seconds 2
                    $retryCount++
                    Write-Output ("retry count at: " + $retryCount)
                    }
                Finally{
                    $mutex.ReleaseMutex()
                 }
                
    
            }
         } #End of IF Statement
         } -ArgumentList $RubrikToken, $rubrikAddress, $masterDBList[$i], $Output_directory, $mdate, $GetStorageStats, $SLA_list, $Rubrikinfo # End of individual DB job loop 
        
        }  #End of script block
        $dbJobsList += $dbJob
        
        
    
    
    
        Write-Output "
        Created these jobs..."
        Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "DBBackup_Job"} | Format-Table
        Write-Output "
        Will wait on jobs now"
        $jobsOutputList = @()
        $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "DBBackup_Job"} | Wait-Job | Receive-Job
        Start-sleep -Seconds 5
    
        Write-Output "
        Cleaning up job list"
        Get-Job | Remove-Job
        Start-Sleep -Seconds 15
    
        Write-Output "Calculating Archive lag by number of snapshots"
        $Oracle_csv_data = Import-Csv ($Output_directory + "/listof_Oracle_snaps-" + $Rubrikname + $mdate + ".csv")
        $OracleDB_snaps_behind = $Oracle_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
        $Number_of_OracleDB_snaps_behind = ($OracleDB_snaps_behind | Measure-Object).count
        Write-Output ("There are " + $Number_of_OracleDB_snaps_behind + " Oracle DB snapshots contributing to Archive Lag") 
    
    
        $dumpedToCsv = $false
        $retryCount = 0
    }
} # End of Oracle Snappable Switch
if($Nutanix){
$threadcount = $globalThreadCount    
Write-Output "
Gathering information on Nutanix VMs"
$VMware_objects = @()
$VMwareObjectList = ((Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/nutanix/vm?limit=9999") -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json)
$VMware_objects += $VMwareObjectList.data
 $offset = "9999"
while ($VMwareObjectList.hasMore -eq $True){
    Write-Host "Additional VMware Objects Found. Issuing another API call to pull up to an additional 9999 VMs."
    $VMwareObjectList = (Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/nutanix/vm?limit=9999&offset=" + $offset) -Method Get -Headers $RubrikToken -skipcertificateCheck).content | ConvertFrom-Json
    $VMware_objects += $VMwareObjectList.data
    $offset = ($VMware_objects | measure-object).Count
    $lastvmwareObject = ($VMware_objects | Select-Object -Last 1).id
    Write-host ("Need to issue another API call starting after VM " + $lastvmwareObject)
}
$vm_list = $VMware_objects
if(!($IncludeAll)){
    $vmsToCheck = $vm_list | Where-Object{$_.effectiveSlaDomainId -ne "UNPROTECTED"}
}
if($IncludeAll){
    $vmsToCheck = $vm_list
}
if($customlist){
    $vmsToCheck = Import-Csv $pathtocustomlist
}
$vmCount = ($vmsToCheck | Measure-Object).Count
Write-Output ("Will check " + $vmCount + " VMs")

if(!($threadCount)) {
    Write-Output ("
    Setting thread count to 1 because the threadcount was not specified.
    If additional threads are desired please cancel this run and specify the threadcount using the -threadcount flag.")
    $threadCount = 1
}
    elseif ($threadCount -gt 16) {
        Write-Output ("Setting thread count to 16 instead")
        $threadCount = 16
    }
        elseif ($vmCount -lt $threadcount) {
            Write-Output ("Setting thread count to match VM count of " + $vmCount)
            $threadcount = $vmCount
        }

if($vmCount -eq 0) {
    Write-Output ("There are no active Nutanix VMs. Skipping to the end.")
    $threadcount = 1
}
if($vmCount -ne 0) {
[int]$quotient = $vmCount / $threadCount
$quotient = [math]::Floor($quotient)
[int]$loopCount = 0

$masterVMList = @()

#create the list of VMs
while($loopCount -lt $threadCount) {
    $childVMList = @()
    for($i = $loopCount * $quotient;$i -lt (($loopCount + 1) * $quotient) -and $vmCount; $i++) {
        if ($null -eq $vmsTocheck[$i]) {
            break
        }
        $childVMList += $vmsToCheck[$i]
        }
    $masterVMList += @{'vmList' = $childVMList}
    $loopCount++
}

$vmJobsList = @()
for($i = 0;$i -le $threadCount;$i++) {

    $vmJob = Start-Job -name "NutanixBackup_Job_$i" -ScriptBlock {
     $jRToken = $args[0]
     $jRAddr = $args[1]
     $jVMList = $args[2] 
     $jSLA_list = $args[3]
     $jworkingdirectory = $args[4]
     $jstartTime = $args[5]
     $jStorage_Stats = $args[6]
     $jRubrikinfo = $args[7]

     $jRubrikVersion = $jRubrikinfo.version
     foreach($vm in $jVMList.vmlist){
#Gather a list of good snaps
        $vm_snap_info = ((Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/nutanix/vm/" + $vm.id + "/snapshot") -Method GET -Headers $jRToken -skipcertificateCheck ).content | ConvertFrom-Json).data
        $VMware_stats = @()
        $Good_Snaps_list = @()
        ForEach($snapshot in $vm_snap_info){

            $SLA = $jSLA_list | where-object {$_.id -eq $snapshot.slaId}
            #convert protected date to Time range by day
            $date1 = $vm.protectionDate
            $date2 = $snapshot.date
            if($date1 -gt $date2 -or [string]::IsNullOrEmpty($date1)){
                $date1 = ($vm_snap_info | select-object -first 1).date
            }
            $timespan = (New-TimeSpan -Start $date1 -End $date2).TotalDays
            $timespan = ("$timespan").split('.')[0]     

            $replicationSpecs = $SLA.replicationSpecs 

			$snapshot_stats = New-Object psobject
			$snapshot_stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $snapshot.vmName
            $snapshot_stats | Add-Member -NotePropertyName "SnappableId" -NotePropertyValue $VM.id
            $snapshot_stats | Add-Member -NotePropertyName "PrimaryClusterID" -NotePropertyValue $VM.primaryClusterId
            $snapshot_stats | Add-Member -NotePropertyName "SLAName" -NotePropertyValue $SLA.name
            $snapshot_stats | Add-Member -NotePropertyName "SLA_ID" -NotePropertyValue $snapshot.slaId
            $snapshot_stats | Add-Member -NotePropertyName "Is_Relic" -NotePropertyValue $VM.isRelic
            $snapshot_stats | Add-Member -NotePropertyName "VM_Protection_Date" -NotePropertyValue $vm.protectionDate
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Date" -NotePropertyValue $snapshot.date
            $snapshot_stats | Add-Member -NotePropertyName "Snapshot_Retention_Tag" -NotePropertyValue ""
            #$snapshot_stats | Add-Member -NotePropertyName "Expiration_Date" -NotePropertyValue ""
            $replication_ids = $snapshot.replicationLocationIds -join ","
            if(-not [string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "TRUE"
            }
            if([string]::IsNullOrEmpty($replicationSpecs.locationId)){
                $snapshot_stats | Add-Member -NotePropertyName "Replication_Configured" -NotePropertyValue "FALSE"
            }
            if($snapshot_stats.PrimaryClusterID -ne $jRubrikinfo.id){
                $snapshot_stats.Replication_Configured = "ReplicaCopy"
            }
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationLocationID" -NotePropertyValue $replication_ids
            $snapshot_stats | Add-Member -NotePropertyName "ReplicationRetentionLimit" -NotePropertyValue $replicationSpecs.retentionLimit
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Eligible_Date" -NotePropertyValue ""
            
			$snapshot_stats | Add-Member -NotePropertyName "SnapshotId" -NotePropertyValue $snapshot.id
			$snapshot_stats | Add-Member -NotePropertyName "IsOnDemandSnapshot" -NotePropertyValue $snapshot.isOnDemandSnapshot
            $snapshot_stats | Add-Member -NotePropertyName "Consistency_level" -NotePropertyValue $snapshot.consistencyLevel
            $snapshot_stats | Add-Member -NotePropertyName "Cloud_State" -NotePropertyValue $snapshot.cloudState
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_Based_On_SLA_assignment" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Threshold_in_seconds" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Archive_Location_ID_of_individual_snapshot" -NotePropertyValue ""
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Archive_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "Contributing_to_Replication_Lag" -NotePropertyValue "FALSE"
            $snapshot_stats | Add-Member -NotePropertyName "cloudStorageTier" -NotePropertyValue $snapshot.cloudStorageTier

            if($snapshot_stats.Replication_Configured -eq "TRUE"){
                if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                    $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                }
            }

                $snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment = ($SLA.archivalSpecs).locationId
                $snapshot_stats.Archive_Threshold_in_seconds = ($SLA.archivalSpecs).archivalThreshold
                $snapshot_stats.Archive_Location_ID_of_individual_snapshot = ($snapshot.archivalLocationIds)[0]
                $now = Get-date
                #Wait-Debugger
                $archival_threshold = ($SLA.archivalSpecs).archivalThreshold
                $archive_lag = $now -gt ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold) 
                $snapshot_stats.Archive_Eligible_Date = ($snapshot_stats.Snapshot_Date).AddSeconds($archival_threshold)
                $snapshot_stats.Contributing_to_Archive_Lag = $archive_lag
                #Adding logic to take into account snapshots that have already been archived. 
                if($snapshot_stats.Cloud_State -ne "0"){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
                }
                if([string]::IsNullOrEmpty($snapshot_stats.Archive_Location_ID_Based_On_SLA_assignment)){
                    $snapshot_stats.Contributing_to_Archive_Lag = "FALSE"
               }    
            #Switch to add in storage stats to the snapshot lists
            if($jStorage_Stats -eq $true){
                $snapshot_stats | Add-Member -NotePropertyName "LogicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "IngestedGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "PhysicalGBytes" -NotePropertyValue ""
                $snapshot_stats | Add-Member -NotePropertyName "HistoricIngestedGBytes" -NotePropertyValue ""
                    Write-Host ("Pulling storage stats for snapshot " + $snapshot.id)
                    $snapshot_storage_stats = (Invoke-WebRequest -Uri ("https://" + $jRAddr + "/api/internal/snapshot/" + $snapshot.id + "/storage/stats?snappable_id="+ $VM.id) -Method GET -Headers $jRToken -skipcertificateCheck).content | ConvertFrom-Json
                    $snapshot_stats.LogicalGBytes = ($snapshot_storage_stats.LogicalBytes/1GB)
                    $snapshot_stats.IngestedGBytes = ($snapshot_storage_stats.IngestedBytes/1GB)
                    $snapshot_stats.PhysicalGBytes = ($snapshot_storage_stats.physicalBytes/1GB)
                    $snapshot_stats.HistoricIngestedGBytes = ($snapshot_storage_stats.historicIngestedBytes/1GB)
            }
            if($jRubrikVersion -ge 5.2){
                $snapshot_stats | Add-Member -NotePropertyName "Local Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).name
                $snapshot_stats | Add-Member -NotePropertyName "Local Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.localInfo).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Replication Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Replication Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.replicationInfos).expirationTime
                $snapshot_stats | Add-Member -NotePropertyName "Archival Location Name" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).name
                $snapshot_stats | Add-Member -NotePropertyName "Archival Expiration Date" -NotePropertyValue ($snapshot.snapshotRetentionInfo.archivalInfos).expirationTime

            }
            $VMware_stats +=$snapshot_stats

		}
        $Good_Snaps_list += $VMware_stats

        $dumpedToCsv = $false
        $retryCount = 0
        while($dumpedToCsv -eq $false -and $retryCount -lt 20) {
            $mutex = New-Object System.Threading.Mutex($false, "VMwareMutex")
            $mutex.WaitOne();
            Try { 
                $Rubrikname = $jRubrikinfo.name
                $fileTestPath = $jworkingdirectory + "/listof_Nutanix_snaps-" +$Rubrikname + $jstartTime + ".csv"
                [IO.file]::OpenWrite($fileTestPath).close()
                $Good_Snaps_list| Export-Csv -Append -NoTypeInformation ($jworkingdirectory + "/listof_Nutanix_snaps-" +$Rubrikname + $jstartTime + ".csv")
                $dumpedToCsv = $true
            }
            Catch{
                Start-Sleep -Seconds 2
                $retryCount++
                Write-Output ("retry count at: " + $retryCount)
                }
            Finally{
                $mutex.ReleaseMutex()
                }
            

        }
     } # End of individual VM job loop 
    
    } -ArgumentList $RubrikToken, $rubrikAddress, $masterVMList[$i], $SLA_list, $Output_directory, $mdate, $GetStorageStats, $Rubrikinfo #End of script block
    $vmJobsList += $vmJob

    } 

    Write-Output "
    Created these jobs..."
    Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "NutanixBackup_Job"} | Format-Table
    Write-Output "
    Will wait on jobs now"
    $jobsOutputList = @()
    $jobsOutputList += Get-Job | Where-Object{$_.state -eq "Running" -and $_.name -match "NutanixBackup_Job"} | Wait-Job | Receive-Job

    Write-Output "Cleaning up job list"
    Get-Job | Remove-Job
    Start-Sleep -Seconds 15

    Write-Output "Calculating Archive lag by number of snapshots"
    $VM_csv_data = Import-Csv ($Output_directory + "/listof_Nutanix_snaps-" + $Rubrikname + $mdate + ".csv")
    $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}
    $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
    Write-Output ("There are " + $Number_of_VM_snaps_behind + " Nutanix VM snapshots contributing to Archive Lag") 

} #End of IF Statement
} #End Nutanix Flag

$endTime = Get-Date
Write-Output ("This run took " + ($endTime - $startTime).totalseconds + " seconds")


Write-Host "

Archive and Replication Lag Summary
"                                                
$DownloadedSnap = @()
if($VMware){
    $csv_path = ($Output_directory + "/listof_VMware_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)

        if($StorageReview){
            $VMDuplicates = $VM_csv_data 
            $unique_VMs = $VMs.Name | Get-Unique
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

            
        
        
        Write-Output "VMware VM Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " VMware VM snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total VMware VM snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("VMware Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " VMware VM snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total VMware VM snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("Vmware Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($MSSQL){
    $csv_path = ($Output_directory + "/listof_SQL_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $SQL_csv_data = Import-Csv $csv_path
        $Total_number_of_DBs = (($SQL_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($SQL_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($SQL_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)


        if($StorageReview){
            $SQL_Servers = $SQL_csv_data 
            $unique_SQL_servers =  $SQL_Servers.Hostname | Get-Unique
            $filtered_unique_SQL = foreach($SQL in $unique_SQL_servers){$SQL.Split('.')[0]}
            $filtered_unique_SQL = foreach($SQL in $filtered_unique_SQL){$SQL.Split('\')[0]}
            $wastedObjects = $SQL_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }
        Write-Output "MSSQL DB Snapshot Report:
        "
		Write-Output ("There are a total of " + $Total_number_of_DBs + " Snapshots for this object type
		")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $SQLDB_replication_behind = $SQL_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_SQLDB_Snaps_Behind_replication = ($SQLDB_replication_behind | Measure-Object).Count
            Write-Output ("There are " + $Number_of_SQLDB_Snaps_Behind_replication + " MSSQL DB snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total MSSQL DB snapshots.")
            $replication_lag_percentage = ($Number_of_SQLDB_Snaps_Behind_replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("MSSQL Snapshot Replication is " +  "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $SQLDB_snaps_behind = $SQL_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
            $Number_of_SQLDB_snaps_behind = ($SQLDB_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_SQLDB_snaps_behind + " MSSQL DB snapshots contributing to Archive Lag out of " + $TotalNumberOfSnapsConfigureArchive + " total MSSQL DB snapshots")  
            $Archive_lag_percentage = ($Number_of_SQLDB_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100 
            Write-Output ("MSSQL Snapshot Archives are " +  "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($Fileset){
    $csv_path = ($Output_directory + "/listof_Fileset_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $Fileset_csv_data = Import-Csv $csv_path
        $Total_number_of_Filesets = (($Fileset_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($Fileset_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($Fileset_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)


        if($StorageReview){
            $Fileset_Servers = $Fileset_csv_data 
            $unique_Fileset_servers =  $Fileset_Servers.Hostname | Get-Unique
            $filtered_unique_Fileset = foreach($Filer in $unique_Fileset_servers){$Filer.Split('.')[0]}
            $wastedObjects = $Fileset_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

        Write-Output "Fileset Snapshot Report:
        "
		Write-Output ("There are a total of " + $Total_number_of_Filesets + " Snapshots for this object type
		")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $Fileset_replication_behind = $Fileset_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_Fileset_Snaps_Behind_replication = ($Fileset_replication_behind | Measure-Object).Count
            Write-Output ("There are " + $Number_of_Fileset_Snaps_Behind_replication + " Fileset snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total Fileset snapshots.")
            $replication_lag_percentage = ($Number_of_Fileset_Snaps_Behind_replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("Fileset Snapshot Replication is " +  "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $Fileset_snaps_behind = $Fileset_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"} 
            $Number_of_Fileset_snaps_behind = ($Fileset_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_Fileset_snaps_behind + " Fileset snapshots contributing to Archive Lag out of " + $TotalNumberOfSnapsConfigureArchive + " total Fileset snapshots")  
            $Archive_lag_percentage = ($Number_of_Fileset_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100 
            Write-Output ("Fileset Snapshot Archives are " +  "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($HyperV){
    $csv_path = ($Output_directory + "/listof_HyperV_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)
            

        if($StorageReview){
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

        Write-Output "HyperV VM Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " HyperV VM snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total HyperV VM snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("HyperV Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " HyperV VM snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total HyperV VM snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("HyperV Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($EC2_Instance){
    $csv_path = ($Output_directory + "/listof_EC2_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)
            
        if($StorageReview){
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }
        
        Write-Output "EC2 VM Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " EC2 VM snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total EC2 VM snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("EC2 Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " EC2 VM snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total EC2 VM snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("EC2 Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($VolumeGroup){
    $csv_path = ($Output_directory + "/listof_VolumeGroup_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)
            

        if($StorageReview){
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }


        Write-Output "VolumeGroup Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " VolumeGroup snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total VolumeGroup snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("VolumeGroup Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " VolumeGroup snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total VolumeGroup snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("VolumeGroup Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($ManagedVolume){
    $csv_path = ($Output_directory + "/listof_ManagedVolume_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data = Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)
            

        if($StorageReview){
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

        
        Write-Output "ManagedVolume Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " ManagedVolume  snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total ManagedVolume snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("ManagedVolume Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " ManagedVolume snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total ManagedVolume snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("ManagedVolume Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }

}
if($OracleDB){
    $csv_path = ($Output_directory + "/listof_Oracle_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)
            
        
        
        if($StorageReview){
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

        Write-Output "Oracle DB Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " Oracle  snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total Oracle snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("Oracle Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " Oracle snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total Oracle snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("Oracle Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}
if($Nutanix){
    $csv_path = ($Output_directory + "/listof_Nutanix_snaps-" + $Rubrikname + $mdate + ".csv")
    if([System.IO.File]::Exists($csv_path) -eq $true){
        $VM_csv_data =Import-Csv $csv_path
        $Total_number_of_VMs = (($VM_csv_data | Measure-Object).count)
        $TotalNumberOfSnapsConfigureArchive = ($VM_csv_data | Where-Object {$_.Archive_Location_ID_Based_On_SLA_assignment -ne ""} | Measure-Object).Count
        $TotalNumberOfSnapsConfigureRepl = (($VM_csv_data | Where-Object{$_.Replication_Configured -eq "TRUE"}|Measure-Object).count)

        if($StorageReview){
            $VMDuplicates = $VM_csv_data 
            $unique_VMs = $VMs.Name | Get-Unique
            $wastedObjects = $VM_csv_data | Where-Object {$_.Cloud_State -eq "3"}
            $wastedObjects = $wastedObjects | Select-Object name,snappableId,Hostname,SnapshotId,SLAName,Cloud_State,IsOnDemandSnapshot
            $DownloadedSnap += $wastedObjects

        }

            
        
        
        Write-Output "Nutanix VM Snapshot Report:
        "
        Write-Output ("There are a total of " + $Total_number_of_VMs + " Snapshots for this object type
        ")
        if($TotalNumberOfSnapsConfigureRepl -ge "1"){
            $VM_replication_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Replication_Lag -eq "True"}
            $Number_of_VM_snaps_Behind_Replication = ($VM_replication_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_Behind_Replication + " Nutanix VM snapshots contributing to Replication Lag out of " +$TotalNumberOfSnapsConfigureRepl+ " total Nutanix VM snapshots configured for replication.")
            $replication_lag_percentage = ($Number_of_VM_snaps_Behind_Replication/$TotalNumberOfSnapsConfigureRepl)*100
            Write-Output ("Nutanix Snapshot Replication is " + "{0:N1}" -f $replication_lag_percentage + "% out of sync")
            Write-Output "
            "
        }
        if($TotalNumberOfSnapsConfigureArchive -ge "1"){
            $VM_snaps_behind = $VM_csv_data | Where-Object{$_.Contributing_to_Archive_Lag -eq "True"}  
            $Number_of_VM_snaps_behind = ($VM_snaps_behind | Measure-Object).count
            Write-Output ("There are " + $Number_of_VM_snaps_behind + " Nutanix VM snapshots contributing to Archive Lag out of " +$TotalNumberOfSnapsConfigureArchive+ " total Nutanix VM snapshots configured for archive.")
            $Archive_lag_percentage = ($Number_of_VM_snaps_behind/$TotalNumberOfSnapsConfigureArchive)*100
            Write-Output ("Nutanix Snapshot Archives are " + "{0:N1}" -f $Archive_lag_percentage + "% out of sync")

        }      
        Write-Output "
        
        
        "
    }
}

Write-Output ("For additional detail on Snapshots CSVs have been generated for each Object type and placed at " + $Output_directory)

if($StorageReview){
    Write-Host "The following snapshots have been downloaded and should be manually removed once they are no longer needed."
    $DownloadedSnap

    Write-Host "Reviewing CSVs for Double Dippers."
    foreach($server in $filtered_unique_SQL){

        foreach($VM_object in $unique_VMs){
            if ($server -icontains $VM_object){$list_of_duplicates += $VM_object}
        }
    }
    
        $SQL_list = $list_of_duplicates | Get-Unique 

        $fileset_list = $list_of_duplicate_filesets | Get-Unique 
        $list_of_duplicate_SQL = @()
        foreach($Fileset_object in $filtered_unique_Fileset){
        foreach($SQL in $filtered_unique_SQL){
            if ($Fileset_object -icontains $SQL){$list_of_duplicate_SQL += $SQL}
        }

    }
    
        $File_SQL_list = $list_of_duplicate_SQL | Get-Unique 
        Write-Host "This is a list of Objects that are protected at the SQL and VMware VM level:"
        $SQL_list 

        Write-Host "
        This is a list of Objects that are protected at the VMware VM and Fileset level"
        $fileset_list 
        Write-Host "
        This list contains servers that are protected by Filesets and SQL level snaps. While worth reviewing if SQL file types are being excluded there is no double dipping occurring here."
        $File_SQL_list 
<#  

TAKE SECTION OUT AFTER TESTING!!!!

Need to add error handling in the event not all objects are checked, or if there are no double dippers detected. 
Sample to check for empty variable:
                        if([string]::IsNullOrEmpty($snapshot_stats.ReplicationLocationID)){
                            $snapshot_stats.Contributing_to_Replication_Lag = "TRUE"
                        }




Pulled from Existing double dipper script. Some review might be needed to fit into Get-SnapshotDetails.ps1

$list_of_duplicates = @()
$VMDuplicates = $CSV_data | Where-Object{$_."Object Type" -eq "vSphere VM"}
$unique_VMs = $VMs."Object Name" | Get-Unique
$SQL_Servers = $CSV_data | Where-Object{$_."Object Type" -eq "SQL Server DB"}
$unique_SQL_servers =  $SQL_Servers.Location | Get-Unique
$filtered_unique_SQL = foreach($SQL in $unique_SQL_servers){$SQL.Split('.')[0]}
$filtered_unique_SQL = foreach($SQL in $filtered_unique_SQL){$SQL.Split('\')[0]}


foreach($server in $filtered_unique_SQL){
foreach($VM in $unique_VMs){
if ($server -icontains $VM){$list_of_duplicates += $VM}
}
}

$SQL_list = $list_of_duplicates | Get-Unique 



$Fileset_Servers = $CSV_data | Where-Object{$_."Object Type" -eq "Windows Fileset"}
$unique_Fileset_servers =  $Fileset_Servers.Location | Get-Unique
$filtered_unique_Fileset = foreach($Filer in $unique_Fileset_servers){$Filer.Split('.')[0]}

$list_of_duplicate_filesets = @()
foreach($Fileset in $filtered_unique_Fileset){
foreach($VM in $unique_VMs){
if ($Fileset -icontains $VM){$list_of_duplicate_filesets += $VM}
}
}

$fileset_list = $list_of_duplicate_filesets | Get-Unique 
$list_of_duplicate_SQL = @()
foreach($Fileset in $filtered_unique_Fileset){
foreach($SQL in $filtered_unique_SQL){
if ($Fileset -icontains $SQL){$list_of_duplicate_SQL += $SQL}
}
}

$File_SQL_list = $list_of_duplicate_SQL | Get-Unique 

Remove-Item $output



Write-Host "This is a list of Objects that are protected at the SQL and VMware VM level:"
$SQL_list 

Write-Host "
This is a list of Objects that are protected at the VMware VM and Fileset level"
$fileset_list 

Write-Host "
This list contains servers that are protected by Filesets and SQL level snaps. While worth reviewing if SQL file types are being excluded there is no double dipping occurring here."
$File_SQL_list 

#>

<#

Ideas for Storage Review Flag:
CloudState 3
FOREVER Retention 

SQL VMs
DoubleDippers 

#>


}



<#
CLOUD_STATE Reference Guide:
          0:NOT_ON_CLOUD
          1:LATEST_SNAPSHOT_ON_CLOUD
          2:ON_CLOUD
          3:REHYDRATED_FROM_CLOUD
          4:DOWNLOAD_IN_PROGRESS
          5:DELETED_FROM_CLOUD
          6:LOCAL_AND_ON_CLOUD
#>