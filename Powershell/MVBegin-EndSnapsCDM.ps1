<#

.SYNOPSIS
This script is to serve as a template for Powershell initiated MV backups with token based authentication

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.Example
./MVBegin-EndSnapsCDM.ps1 -rubrikAddress "10.8.48.104" -serviceAccountJson $serviceAccountJson -MV_ID "ManagedVolume:::30a5645e-c329-443c-82f8-b2b7f5392320"

This initiates a begin snapshot, executes the specified script, and then closes the snapshot for MV ManagedVolume:::30a5645e-c329-443c-82f8-b2b7f5392320. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : August 20, 2021
    Updated : January 09, 2025
    Company : Rubrik Inc

#>
[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$rubrikAddress,
    [parameter(Mandatory=$true)]
    [string]$MV_ID
)

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json


#Setup the script to be able to ignore self signed certificate errors
try {
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
"@}

catch 
    {
    Write-Output "Trust cert policy already set"
    }

#Bypass self-signed certificate check issue
$AllProtocols = [System.Net.SecurityProtocolType]'Ssl3,Tls,Tls11,Tls12'
[System.Net.ServicePointManager]::SecurityProtocol = $AllProtocols
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
function Connect-RubrikCdm{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$rubrikAddress,
        [parameter(Mandatory=$true)]
        [string]$serviceAccountJson
    )
    try{
        $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
          $connectionData = [ordered]@{
              'serviceAccountId' = $serviceAccountObj.client_id
              'secret' = $serviceAccountObj.client_secret
          } | ConvertTo-Json
          $uriString = "https://$($rubrikAddress)/api/v1/service_account/session"
  
      $rubrikCdm = Invoke-RestMethod -Method Post -uri $uriString -ContentType application/json -body $connectionData #-skipcertificateCheck
    }
    catch{
      Write-Error("Error $($_)")
    }
    finally{
      Write-Output $rubrikCdm
    }
  }

$connectionInfo = Connect-RubrikCdm -rubrikAddress $rubrikAddress -serviceAccountJson $ServiceAccountJson
$token = $connectionInfo.token
#Setup Auth context in headers
#Using basic auth just for ease, but in production this should be converted to something more secure. 
$headers = @{ Authorization = "Bearer "+ $token }



#Open Volume for writes
(Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/managed_volume/" + $MV_ID + "/begin_snapshot") -Method POST -Headers $headers)

#Insert code to write to volume here


#Close Volume for Writes
(Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/v1/managed_volume/" + $MV_ID + "/end_snapshot") -Method POST -Headers $headers)
