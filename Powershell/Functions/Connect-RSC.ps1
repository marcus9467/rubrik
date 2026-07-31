function connect-RSC {
    <#
    .SYNOPSIS
    Function to provide the initial authorization to Rubrik Security Cloud using a service account JSON file. 
    
    CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

    .EXAMPLE
    $polSession = connect-polaris -ServiceAccountJson "ServiceAccount.json"
    $rubtok = $polSession.access_token
    $headers = @{
        'Content-Type'  = 'application/json';
        'Accept'        = 'application/json';
        'Authorization' = $('Bearer ' + $rubtok);
    }
    
    Establishes a session with RSC and creates the initial header information for later Invoke-WebRequest calls. 
    
    .NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : May 08, 2023
    Company : Rubrik Inc
    #>

    [CmdletBinding()]

    param (
        [parameter(Mandatory=$true)]
        [string]$ServiceAccountJson
        # Service account JSON file

    )

   

    begin {

        # Parse the JSON and build the connection string
        $serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
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
                $global:Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
                $global:logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

            } else {

                Write-Error("Unable to connect")

            }

           

        }

}
