<#

.SYNOPSIS
This script is meant to be an example of how to automate protection of Azure VMs.

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.EXAMPLE
./ProtectAzureVM.ps1 -slaId $slaId -vmName $vmName -ServiceAccountJson $serviceAccountJson

This will apply protection to any VM named $vmName. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com>
    Created : August 01, 2024
    Company : Rubrik Inc
#>

[cmdletbinding()]
param (
    [parameter(Mandatory=$true)]
    [string]$ServiceAccountJson,
    [parameter(Mandatory=$true)]
    [string]$slaId,
    [parameter(Mandatory=$true)]
    [string]$vmName
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
function Get-AzureVM {
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$vmName
    )

    process {
        try {
            $allResults = @()
            $hasNextPage = $true
            $currentCursor = $null

            while ($hasNextPage) {
                $query = "query AzureVMListQuery(`$first: Int, `$after: String, `$sortBy: AzureNativeVirtualMachineSortFields, `$sortOrder: SortOrder, `$filters: AzureNativeVirtualMachineFilters, `$descendantTypeFilters: [HierarchyObjectTypeEnum!], `$isMultitenancyEnabled: Boolean = false) {
                  azureNativeVirtualMachines(first: `$first, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder, virtualMachineFilters: `$filters, descendantTypeFilter: `$descendantTypeFilters) {
                    edges {
                      cursor
                      node {
                        id
                        name
                        resourceGroup {
                          id
                          name
                          subscription {
                            id
                            name
                            status: azureSubscriptionStatus
                            nativeId: azureSubscriptionNativeId
                            __typename
                          }
                          __typename
                        }
                        region
                        vnetName
                        subnetName
                        sizeType
                        isRelic
                        ...EffectiveSlaColumnFragment
                        ...OrganizationsColumnFragment @include(if: `$isMultitenancyEnabled)
                        slaAssignment
                        authorizedOperations
                        effectiveSlaSourceObject {
                          fid
                          name
                          objectType
                          __typename
                        }
                        isAppConsistencyEnabled
                        vmAppConsistentSpecs {
                          preSnapshotScriptPath
                          preScriptTimeoutInSeconds
                          postSnapshotScriptPath
                          postScriptTimeoutInSeconds
                          cancelBackupIfPreScriptFails
                          rbaStatus
                          __typename
                        }
                        isExocomputeConfigured
                        isFileIndexingEnabled
                        isAdeEnabled
                        hostInfo {
                          ...AppTypeFragment
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    pageInfo {
                      endCursor
                      hasNextPage
                      hasPreviousPage
                      __typename
                    }
                    __typename
                  }
                }

                fragment OrganizationsColumnFragment on HierarchyObject {
                  allOrgs {
                    name
                    __typename
                  }
                  __typename
                }

                fragment EffectiveSlaColumnFragment on HierarchyObject {
                  id
                  effectiveSlaDomain {
                    ...EffectiveSlaDomainFragment
                    ... on GlobalSlaReply {
                      description
                      __typename
                    }
                    __typename
                  }
                  ... on CdmHierarchyObject {
                    pendingSla {
                      ...SLADomainFragment
                      __typename
                    }
                    __typename
                  }
                  __typename
                }

                fragment EffectiveSlaDomainFragment on SlaDomain {
                  id
                  name
                  ... on GlobalSlaReply {
                    isRetentionLockedSla
                    retentionLockMode
                    __typename
                  }
                  ... on ClusterSlaDomain {
                    fid
                    cluster {
                      id
                      name
                      __typename
                    }
                    isRetentionLockedSla
                    retentionLockMode
                    __typename
                  }
                  __typename
                }

                fragment SLADomainFragment on SlaDomain {
                  id
                  name
                  ... on ClusterSlaDomain {
                    fid
                    cluster {
                      id
                      name
                      __typename
                    }
                    __typename
                  }
                  __typename
                }

                fragment AppTypeFragment on PhysicalHost {
                  id
                  cluster {
                    id
                    name
                    status
                    timezone
                    __typename
                  }
                  connectionStatus {
                    connectivity
                    __typename
                  }
                  descendantConnection {
                    edges {
                      node {
                        objectType
                        effectiveSlaDomain {
                          ...EffectiveSlaDomainFragment
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }"

                $variables = @{
                    isMultitenancyEnabled = $true
                    first = 50
                    after = $currentCursor
                    filters = @{
                        nameSubstringFilter = @{
                            nameSubstring = $vmName
                        }
                    }
                    descendantTypeFilters = @()
                    sortBy = "NAME"
                    sortOrder = "ASC"
                }

                $JSON_BODY = @{
                    query = $query
                    variables = $variables
                } | ConvertTo-Json -Depth 5

                $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
                $content = $result.Content | ConvertFrom-Json

                $allResults += $content.data.azureNativeVirtualMachines.edges | ForEach-Object { $_.node }

                $hasNextPage = $content.data.azureNativeVirtualMachines.pageInfo.hasNextPage
                $currentCursor = $content.data.azureNativeVirtualMachines.pageInfo.endCursor
            }

            return $allResults
        }
        catch {
            Write-Error("Error $($_)")
        }
    }
}
function Protect-AzureVM {
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$snappableId,
        [parameter(Mandatory=$true)]
        [string]$slaId
    )
    process {
        try {
            $query = "mutation BulkAssignSlasMutation(`$input: BulkAssignSlasInput!) {
              bulkAssignSlas(input: `$input) {
                slaAssignResults {
                  success
                  __typename
                }
                __typename
              }
            }"

            $variables = "{
                `"input`": {
                  `"assignSlaRequests`": [
                    {
                      `"slaDomainAssignType`": `"protectWithSlaId`",
                      `"objectIds`": [
                        `"${snappableId}`"
                      ],
                      `"shouldApplyToExistingSnapshots`": null,
                      `"shouldApplyToNonPolicySnapshots`": false,
                      `"slaOptionalId`": `"${slaId}`",
                      `"existingSnapshotRetention`": `"RETAIN_SNAPSHOTS`"
                    }
                  ],
                  `"parentObjectIdToConflictObjectIdsMap`": [],
                  `"userNote`": `"`"
                }
              }"

            $JSON_BODY = @{
                query = $query
                variables = $variables
            } | ConvertTo-Json -Depth 5

            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $result.Content | ConvertFrom-Json
        }
        catch {
            Write-Error ("Error $($_)")
        }
    }
}


$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$mdate = (Get-Date).tostring("yyyyMMddHHmm")

$polSession = connect-polaris
$rubtok = $polSession.access_token
$headers = @{
    'Content-Type'  = 'application/json';
    'Accept'        = 'application/json';
    'Authorization' = $('Bearer ' + $rubtok);
}
$Polaris_URL = ($serviceAccountObj.access_token_uri).replace("client_token", "graphql")
$logoutUrl = ($serviceAccountObj.access_token_uri).replace("client_token", "session")

$AzureVmToProtect = Get-AzureVM -vmName $vmName

ForEach($VM in $AzureVmToProtect){
    Write-Host ("Protecting Azure VM " + $VM.name)
    $protectionresult = Protect-AzureVM -slaId $slaId -snappableId $VM.id
    $protectionresult = ((($protectionresult.data).bulkAssignSlas).slaAssignResults).success
    Write-Host ("Applied SLA " + $slaId + " to VM " + $VM.name + " - " + $protectionresult)
}

disconnect-polaris
