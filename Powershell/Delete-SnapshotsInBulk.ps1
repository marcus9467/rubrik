<#
.SYNOPSIS
This script makes the assumption that the CSV being provided has the headers "SnappableId" and "SnapshotId" to denote the desired snapshots that need to be removed. It will first update their retention SLA to "UNPROTECTED" before deleting them.  

.EXAMPLE
Delete-SnapshotsInBulk.ps1 -filename SnapsToDelete.csv

This will delete all the snapshots specified in the CSV file for all data locations

.EXAMPLE
Delete-SnapshotsInBulk.ps1 -filename SnapsToDelete.csv -locationId 34c6173f-3892-45fb-8713-81a3369f6eb6

This will delete all the snapshots specified in the CSV file for just data location 34c6173f-3892-45fb-8713-81a3369f6eb6. 

#>

[CmdletBinding()]
param(
    [parameter(Mandatory=$true)]
    [string]$file_name,
    [parameter(Mandatory=$false)]
    [string]$locationId
)
if($null -eq $rubrikconnection){
    Connect-Rubrik
}
#Get list of snapshots:
$snapslist = Import-Csv $file_name
$snapslist = $snapslist | Where-Object { $_.PSObject.Properties.Value -ne '' }
$unique_objects = $snapslist.SnappableId | sort| get-unique 

#Modify the snapshots so that they are "UNPROTECTED"
ForEach($object in $unique_objects){
    Write-Host ("Modifying marked snapshots for object " + $object)
    $filteredsnaplist = $snapslist | Where-Object {$_.SnappableId -eq "$object"}
    $snapstomodify = $filteredsnaplist.SnapshotId
    $snapstomodify = $snapstomodify -join '","'
    $jsonpayload = @"
    {
        "objectId": "$object",
        "slaDomainId": "UNPROTECTED",
        "snapshotIds": [
            "$snapstomodify"
        ]
    }
"@
(Invoke-RubrikRESTCall -Endpoint "sla_domain/assign_to_snapshot" -Method POST -Body ($jsonpayload | ConvertFrom-Json) -api 2 -Verbose).responses

#Feed the same snapshots back in for deletion
Write-Host ("Marking snapshots for object " + $object + " for deletion.")

#If a location Id is supplied, build the JSON to only mark the snaps for deletion in the specified location.
if(($locationId)) {
    Write-Host ("Data Location " + $locationId + "specified, only marking snapshots for deletion for this location.")
    $deletion_json = @"
{
    "snapshotIds": [
        "$snapstomodify" 
    ],
    "locationId": "$locationId"
}
"@
}

#If no location ID is supplied, the provided snapshots are expired in all data locations. 
if(!($locationId)) {
    Write-Host "No Location ID specified. Marking listed snapshots for deletion in all data locations"
    $deletion_json = @"
{
    "snapshotIds": [
        "$snapstomodify"
    ]
}
"@
}
Invoke-RubrikRESTCall -Endpoint ("data_source/" + $object + "/snapshot/bulk_delete") -Method POST -Body ($deletion_json | ConvertFrom-Json) -api 1 -Verbose
}