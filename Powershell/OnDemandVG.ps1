<#

.SYNOPSIS
This script is to serve as a template for Powershell initiated VG backups with service account authentication

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.Example
./OndemandVG.ps1 -rubrikAddress "10.8.48.104" -ServiceAccountJson $serviceAccountJson -VG_ID "VolumeGroup:::c35db8d3-cee3-4212-9c87-554579f6d02c"

This initiates a snapshot for VG VolumeGroup:::c35db8d3-cee3-4212-9c87-554579f6d02c.

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : March 05, 2025
    Company : Rubrik Inc

#>
[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$rubrikAddress,
    [parameter(Mandatory=$true)]
    [string]$VG_ID
)

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json

<#
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
"@

}
catch {
    Write-Output "Trust cert policy already set"
}

#Bypass self-signed certificate check issue
$AllProtocols = [System.Net.SecurityProtocolType]'Ssl3,Tls,Tls11,Tls12'
[System.Net.ServicePointManager]::SecurityProtocol = $AllProtocols
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy

#>

function Connect-RubrikCdm {
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$rubrikAddress,
        [parameter(Mandatory=$true)]
        [string]$serviceAccountJson
    )
    try {
        $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
        $connectionData = [ordered]@{
            'serviceAccountId' = $serviceAccountObj.client_id
            'secret' = $serviceAccountObj.client_secret
        } | ConvertTo-Json
        $uriString = "https://$($rubrikAddress)/api/v1/service_account/session"

        $rubrikCdm = Invoke-RestMethod -Method Post -uri $uriString -ContentType application/json -body $connectionData -SkipCertificateCheck
        $global:connectionInfo = $rubrikCdm
        $global:authTime = Get-Date
    }
    catch {
        Write-Error("Error $($_)")
    }
    finally {
        Write-Output $rubrikCdm
    }
}

function New-RubrikHeader {
    # Refresh the token if it is older than 3 hours
    if ((Get-Date) - $global:authTime).TotalHours -ge 3 {
        Write-Output "Token expired, re-authenticating..."
        $global:connectionInfo = Connect-RubrikCdm -rubrikAddress $rubrikAddress -serviceAccountJson $ServiceAccountJson
    }
    $token = $global:connectionInfo.token
    return @{ Authorization = "Bearer $token" }
}

$connectionInfo = Connect-RubrikCdm -rubrikAddress $rubrikAddress -serviceAccountJson $ServiceAccountJson
$authTime = Get-Date

# Setup Auth context in headers
$headers = New-RubrikHeader

# Take VG Backup
$request = (Invoke-WebRequest -SkipCertificateCheck -Uri ("https://" + $rubrikAddress + "/api/v1/volume_group/" + $VG_ID + "/snapshot") -Method POST -Headers $headers)
$request.content | ConvertFrom-Json
$href = (($request.content | ConvertFrom-Json).links).href

# Loop to check status until snapshot process ends
$endStatuses = "SUCCEEDED", "SUCCESSWITHWARNINGS", "FAILED", "CANCELED"
$status = ""

while ($endStatuses -notcontains $status) {
    $headers = New-RubrikHeader
    $hrefRequest = (Invoke-WebRequest -SkipCertificateCheck -Uri $href -Method GET -Headers $headers)
    $status = ($hrefRequest.Content | ConvertFrom-Json).status
    Write-Output "Current status: $status"
    Start-Sleep -Seconds 30 # Wait for 30 seconds before checking again
}

Write-Output "Final status: $status"
