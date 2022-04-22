<#

.SYNOPSIS
This script will provide a mapping of SQL DBs back to their hosts as well as provide transaction log information. 

.EXAMPLE

.\Get-SQLHostMapping.ps1 -ExportToCsv

Provides a mapping of SQL DBs and then prints this information to the foreground. 

.EXAMPLE
.\Get-SQLHostMapping.ps1 -ExportToCsv

Provides a mapping of SQL DBs and then exports this information out to CSV. 

#>
param ([cmdletbinding()]
    [parameter(Mandatory=$false)]
    [switch]$ExportToCSV
    )
#Import-Module -Name Rubrik
#Connect-Rubrik
$mdate = (Get-Date).tostring("yyyyMMddHHmm")
$DB_list = Get-RubrikDatabase
$DB_list = $DB_list | Where-Object {$_.EffectiveSLADomainName -ne "UNPROTECTED"}
$DBCount = ($DB_list | Measure-Object).Count
$DB_Info = @()
$objectindex = 1 
foreach($db in $DB_list){
    Write-Host ("Checking MSSQL DB " + $db.name + " which is " + $objectindex + " out of " + $DBCount + " protected databases")
    #Check for Availability Groups
    $AGDetails = @()
    if(($db.rootProperties).rootType -eq "MssqlAvailabilityGroup"){
        $DB_Stats = New-Object psobject
        #Resolve hosts back to Availability group here
        $availabilityGroupId = ($db.rootProperties).rootId 
        #Wait-Debugger
        $AGHosts = (Invoke-RubrikRestCall -Endpoint ("mssql/hierarchy/" + $availabilityGroupId) -Method GET -api 1 -ErrorAction SilentlyContinue -ErrorVariable ScriptErrror).hosts 
        if($ScriptError){
            Write-Warning ("Unable to Resolve Host/DB mapping for DB " + $db.id)
        }
        $DB_Stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db.Name
        $DB_Stats | Add-Member -NotePropertyName "ID" -NotePropertyValue $db.id
        $DB_Stats | Add-Member -NotePropertyName "DBType" -NotePropertyValue "AvailabilityGroup"
        $DB_Stats | Add-Member -NotePropertyName "Hosts" -NotePropertyValue ($AGHosts.rootName -join ",")
        $DB_Stats | Add-Member -NotePropertyName "HostIds" -NotePropertyValue ($AGHosts.rootId -join ",")
        $DB_Stats | Add-Member -NotePropertyName "EffectiveSLADomainName" -NotePropertyValue $db.effectiveSlaDomainName
        $DB_Stats | Add-Member -NotePropertyName "logBackupFrequencyInSeconds" -NotePropertyValue $db.logBackupFrequencyInSeconds
        $DB_Stats | Add-Member -NotePropertyName "logBackupRetentionHours" -NotePropertyValue $db.logBackupRetentionHours
        $AGDetails += $DB_Stats
    } 
    $WFCDetails = @()
    if(($db.rootProperties).rootType -eq "WindowsCluster"){
        $DB_Stats = New-Object psobject
        #Resolve hosts back to Availability group here
        $WindowsClusterId = ($db.rootProperties).rootId 
        #Wait-Debugger
        $WFHosts = (Invoke-RubrikRestCall -Endpoint ("windows_cluster/" + $WindowsClusterId) -Method GET -api 1 -ErrorAction SilentlyContinue -ErrorVariable ScriptErrror).hostIds 
        if($ScriptError){
            Write-Warning ("Unable to Resolve Host/DB mapping for DB " + $db.id)
        }
        $WFCHostList = @()
        foreach($hostid in $WFHosts){
            $WFCHostname = (Invoke-RubrikRESTCall -Endpoint ("host/" + $hostId) -Method GET -api 1).name
            $WFCHostList += $WFCHostname
        }
        $DB_Stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db.Name
        $DB_Stats | Add-Member -NotePropertyName "ID" -NotePropertyValue $db.id
        $DB_Stats | Add-Member -NotePropertyName "DBType" -NotePropertyValue "WindowsFailoverCluster"
        $DB_Stats | Add-Member -NotePropertyName "Hosts" -NotePropertyValue ($WFCHostList -join ",")
        $DB_Stats | Add-Member -NotePropertyName "HostIds" -NotePropertyValue ($WFHosts -join ",")
        $DB_Stats | Add-Member -NotePropertyName "EffectiveSLADomainName" -NotePropertyValue $db.effectiveSlaDomainName
        $DB_Stats | Add-Member -NotePropertyName "logBackupFrequencyInSeconds" -NotePropertyValue $db.logBackupFrequencyInSeconds
        $DB_Stats | Add-Member -NotePropertyName "logBackupRetentionHours" -NotePropertyValue $db.logBackupRetentionHours
        $WFCDetails += $DB_Stats
    } 
    $StandAloneDetails = @()
    if(($db.rootProperties).rootType -eq "Host"){
        $DB_Stats = New-Object psobject
        $DB_Stats | Add-Member -NotePropertyName "Name" -NotePropertyValue $db.Name
        $DB_Stats | Add-Member -NotePropertyName "ID" -NotePropertyValue $db.id
        $DB_Stats | Add-Member -NotePropertyName "DBType" -NotePropertyValue "StandAlone"
        $DB_Stats | Add-Member -NotePropertyName "Hosts" -NotePropertyValue ($db.rootProperties).rootName
        $DB_Stats | Add-Member -NotePropertyName "HostIds" -NotePropertyValue ($db.rootProperties).rootId
        $DB_Stats | Add-Member -NotePropertyName "EffectiveSLADomainName" -NotePropertyValue $db.effectiveSlaDomainName
        $DB_Stats | Add-Member -NotePropertyName "logBackupFrequencyInSeconds" -NotePropertyValue $db.logBackupFrequencyInSeconds
        $DB_Stats | Add-Member -NotePropertyName "logBackupRetentionHours" -NotePropertyValue $db.logBackupRetentionHours
        $StandAloneDetails += $DB_Stats
    }
    $DB_Info += $StandAloneDetails
    $DB_Info += $WFCDetails
    $DB_Info += $AGDetails
    $objectindex++
}
$DB_Info | Format-Table

if($ExportToCSV){
    Write-Host ("Exporting output to MSSQLInformation" +$mdate + ".csv")
    $DB_Info | Export-Csv -NoTypeInformation ("MSSQLInformation" +$mdate + ".csv")
}



