<#
.SYNOPSIS
The purpose of this script is to accept a list of VMs via CSV input, register the agent for use with file level recovery, and then assign an SLA to them.

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


.EXAMPLE
OnboardVms.ps1 -rubrikAddress 10.11.12.13 -CSV VMList.csv -slaName "Bronze" -ServiceAccountId "User:::c813f521-c7cd-4ec3-9516-ddaba2369c2e" -ServiceAccountSecret "AB9duc+bXVUwfsCqk14norv98ubcdQazyWU8DSWBZxPZpRnOGb72x8wav6eGmkAK7ZLbpFAKjuHUe/C5QMxA"

This will import the list of VMs from the referenced CSV, register the agent to the individual VM, and apply SLA Bronze to the VMs

.NOTES
Requires the Rubrik CDM Powershell Module 

    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : June 27, 2023
    Company : Rubrik Inc
#>
[CmdletBinding()]
param(
    [parameter(Mandatory=$true)]
    [string]$rubrikAddress,
    [parameter(Mandatory=$true)]
    [string]$slaName,
    [parameter(Mandatory=$true)]
    [string]$CSV,
    [parameter(Mandatory=$true)]
    [string]$serviceAccountId,
    [parameter(Mandatory=$true)]
    [string]$serviceAccountSecret
)

#Note, we'll likely need to modify this a bit from the straight import to allow for custom headers. This script needs a list of names, so we'll want to filter for that column. 
$VMList = Import-Csv $CSV
$VMCount = ($VMList | Measure-Object).Count
$Output_directory = (Get-Location).path
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

Connect-Rubrik -Server $rubrikAddress -Id $serviceAccountId -Secret $serviceAccountSecret
$clusterInfo = Get-RubrikClusterInfo
$clusterName = $clusterinfo.Name

$IndexCount = 1
$MissingVMList = @()
ForEach($VM in $VMList){
    Write-Host ("Gathering information on VM " + $VM)
    try{
        $VmInfo = Get-RubrikVM -Name $VM
        Write-Host ("Registering and Assigning Protection to VM " + $VM)
        Register-RubrikBackupService -id $VmInfo.id
        Protect-RubrikVM -id $VmInfo.id -SLA $slaName -Confirm:$false
        Write-Host "Finished Processing " + $IndexCount + "of " + $VMCount + " VMs"
    }
    catch{
        Write-Host ("Unable to Find information on VM " + $VM)
        Write-Host "Appending to a CSV for later review"
        $MissingVMList += $VM  
    }
    $IndexCount++
}
Write-Host ("Writing CSV file to "  + $Output_directory + "/MissingVMsReport_" + $clusterName + "_" +$mdate + ".csv")
$MissingVMList| Export-Csv -NoTypeInformation ($Output_directory + "/MissingVMsReport_" + $clusterName + "_" +$mdate + ".csv")
Disconnect-Rubrik
