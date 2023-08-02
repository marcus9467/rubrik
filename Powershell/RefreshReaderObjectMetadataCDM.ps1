<#

.SYNOPSIS
This script is used to schedule automated archive reader metadata refreshes for a subset of critical objects. 

.EXAMPLE

.\RefreshReaderObjectMetadata.ps1 -RefreshObjectMetadata -ObjectList ObjectList.txt -ArchiveLocationID fad722f1-757c-4c68-82cb-0901ba36fbf3

This will refresh the list of objects contained in ObjectList.txt for the reader archive location fad722f1-757c-4c68-82cb-0901ba36fbf3


.EXAMPLE
.\RefreshReaderObjectMetadata.ps1 -ListArchiveLocations

This will generate a list of reader archive locations. 

.EXAMPLE    

.\RefreshReaderObjectMetadata.ps1 -RefreshArchiveLocation -ArchiveLocationID fad722f1-757c-4c68-82cb-0901ba36fbf3

This will refresh all objects contained in the supplied archive location ID. 

.Notes 
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : August 2, 2023
    Company : Rubrik Inc
#>

[cmdletbinding(DefaultParameterSetName="UseCreds")]
param (
    [parameter(Mandatory=$false)]
    [string]$ObjectList,
    [parameter(Mandatory=$false)]
    [switch]$ListArchiveLocations,
    [parameter(Mandatory=$false)]
    [switch]$RefreshObjectMetadata,
    [parameter(Mandatory=$true)]
    [string]$ArchiveLocationID,
    [parameter(Mandatory=$false)]
    [switch]$RefreshArchiveLocation
)

if([string]::IsNullOrWhiteSpace($rubrikConnection)){
    Connect-Rubrik
}

if($ListArchiveLocations){
    Write-Host "Fetching a list of reader archive locations known by the Rubrik cluster."
    Get-RubrikArchive |Where-Object {$_.ownershipStatus -eq "Reader"}

    Write-Host "Please make note of the desired archive location's ID for use in the next step.
    "
}
if($RefreshObjectMetadata){
    if([string]::IsNullOrWhiteSpace($ObjectList)){
        Write-Host "Please supply a list of object IDs to be refreshed"
        exit
    }
    if([string]::IsNullOrWhiteSpace($ArchiveLocationID)){
        Write-Host "Please supply an Archive Location ID."
        Write-Host "
        
        "
        Write-Host "If you are unsure of the ID please re-run the script with the -ListArchiveLocations flag for more information."
        exit
    }
    $IDsToRefresh = Get-Content $ObjectList
    $FormatedIdList = @()
    forEach($object in $IDsToRefresh){
        if($object -match ":::"){
            $newObjectID = $object.Split(":")[-1]

        }
        elseif ($object -notmatch ":::") {
            $newObjectID = $object
        } 
        $FormatedIdList += $newObjectID

    }
    $FormatedIdList = $FormatedIdList -join "," 
    $jsonBody = @{}
    $localDataSourceIds = New-Object System.Collections.ArrayList
    $localDataSourceIds.add($FormatedIdList)
    $jsonBody.Add("localDataSourceIds",$localDataSourceIds)

    Write-Host ("Refreshing archive location " + $ArchiveLocationID + " with JSON:")
    Write-Host "
    "
    $jsonBody | convertTo-Json
    Invoke-RubrikRESTCall -Endpoint ("archive/location/"+ $ArchiveLocationID + "/reader/refresh/data_sources") -Method POST -Body $jsonBody
}
if($RefreshArchiveLocation){
    Write-Host ("Refreshing archive location " + $ArchiveLocationID)
    Invoke-RubrikRESTCall -Endpoint ("archive/location/"+ $ArchiveLocationID + "/reader/refresh") -Method POST -api internal
}
