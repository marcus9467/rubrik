<#
.SYNOPSIS
This script is to serve as an example for how to poll Rubrik Security Cloud regarding Azure VMs.

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.EXAMPLE
./AzureVMInfo.ps1 -ServiceAccountJson $serviceAccountJson

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com
    Created : August 12, 2024
    Company : Rubrik Inc
#>


[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson
)
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
function Get-RubrikAzureVM{
    try{
        $query = "query AzureVMQuery {azureNativeVirtualMachines{
            nodes{
              name
              objectType
              id
              slaAssignment
              effectiveSlaDomain {
                id
                name
              }
              availabilitySetNativeId
              tags{
                value
                key
              }
            }
            pageInfo{
              endCursor
                    startCursor
                    hasNextPage
                    hasPreviousPage
                    __typename
            }
          }
    }"
    $JSON_BODY = @{
        "query" = $query
    }
    $JSON_BODY = $JSON_BODY | ConvertTo-Json
    $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
    $JSON_Result = ($result.Content) | ConvertFrom-Json
    $JSON = (($JSON_Result.data).azureNativeVirtualMachines).nodes
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $JSON
    }

}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$Output_directory = (Get-Location).path
$mdate = (Get-Date).tostring("yyyyMMddHHmm")



<#
Parsing data here
#>

disconnect-polaris
