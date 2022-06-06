<#

.SYNOPSIS
This script is to serve as a template for Powershell initiated MV backups with token based authentication

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : August 20, 2021
    Company : Rubrik Inc

#>

#Define Rubrik IP and MV ID
$rubrikAddress = "Put IP here"
$MV_ID = "Put MV ID HERE"
$token = "Put Token Here"

#Bypass self-signed certificate check issue
$AllProtocols = [System.Net.SecurityProtocolType]'Ssl3,Tls,Tls11,Tls12'
[System.Net.ServicePointManager]::SecurityProtocol = $AllProtocols
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy

#Setup Auth context in headers
#Using basic auth just for ease, but in production this should be converted to something more secure. 
$headers = @{ Authorization = "Bearer "+ $token }



#Open Volume for writes
(Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/managed_volume/" + $MV_ID + "/begin_snapshot") -Method POST -Headers $headers)

#Insert code to write to volume here


#Close Volume for Writes
(Invoke-WebRequest -Uri ("https://" + $rubrikAddress + "/api/internal/managed_volume/" + $MV_ID + "/end_snapshot") -Method POST -Headers $headers)
