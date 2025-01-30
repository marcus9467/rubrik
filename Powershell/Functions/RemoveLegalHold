function RemoveLegalHoldMutation {
  <#

  .SYNOPSIS
  This is a function to programmatically remove legal hold in bulk. Snapshots IDs can be specified one at a time, or as a comma separated listing 
  Example:
  --snapshotId "snap1", "snap2", "etc"

  CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
  FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
  CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

  .EXAMPLE
  RemoveLegalHoldMutation -snapshotIds "2c15fefe-4e30-5ee5-9056-068b0270f4ea"

  .EXAMPLE
  RemoveLegalHoldMutation -snapshotIds "2c15fefe-4e30-5ee5-9056-068b0270f4ea", "3d25ffee-5f30-6ee5-7056-078b0271d4fb"


  .NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : January 30, 2025
    Company : Rubrik Inc
  #>
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$true)]
      [string[]]$snapshotIds
  )
  process {
    try{
      $query = "mutation RemoveLegalHoldMutation(`$snapshotFids: [String!]!, `$userNote: String) {dissolveLegalHold(input: {snapshotIds: `$snapshotFids, userNote: `$userNote}) {snapshotIds}}"
      $variables = @{
        snapshotFids = $snapshotIds
        userNote = ""
    }
      $JSON_BODY = @{
        "variables" = $variables
        "query" = $query
      }
      $JSON_BODY = $JSON_BODY | ConvertTo-Json
      $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
      $resultContent = ((($result.Content | ConvertFrom-Json).data).dissolveLegalHold).snapshotIds
      Write-Output ("Removing legal hold from snapshot(s): " + ($resultContent -join ", "))    }
    catch{
      Write-Error("Error $($_)")
    }
  }
}
