<#
.SYNOPSIS
This script will pull the Cloud Native Tagging that Rubrik ingests and align it with the capacity endpoints of the objects, including local, replica, archive.
Please note that this will provide an object multiple times as objects that have more than one tag will report the capcity of the object for each tag
associated with that object. After you receive the output of this script you can create a pivot table to sort the sum of each capacity type by tag.

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.EXAMPLE
./CloudChargeBack.ps1 -ServiceAccountJson $serviceAccountJson

.NOTES
    Author  : Tony Koziana <Tony.Koziana@rubrik.com> and Marcus Henderson <marcus.henderson@rubrik.com
    Created : May 23, 2024
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
#Put your additional functions here with your query or mutations.
function Get-AzureVMAndTag{
    try{
        $query = "query TonyChargeBack1 {azureNativeVirtualMachines{
            nodes{
              name
              objectType
              id
              availabilitySetNativeId
              tags{
                value
                key
                __typename
              }
            }
            pageInfo{
              endCursor
                    startCursor
                    hasNextPage
                    hasPreviousPage
                    __typename
            }
          }}"
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
function Get-CloudNativeCapacity{
    try{
        $query = "query TonyChargeBack2(`$first: Int!, `$filter: SnappableFilterInput, `$after: String, `$sortBy: SnappableSortByEnum, `$sortOrder: SortOrder) {
            snappableConnection(first: `$first, filter: `$filter, after: `$after, sortBy: `$sortBy, sortOrder: `$sortOrder) {
              edges {
                node {
                  id
                  name
                  objectType
                  usedBytes
                  physicalBytes
                  replicaStorage
                  archiveStorage
                  ncdArchiveStorage: archiveStorage
                  pullTime
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
          $variables = "{
            `"first`": 50,
            `"filter`": {
              `"objectType`": [
                `"AzureNativeVm`",
                `"AzureNativeManagedDisk`",
                `"AZURE_SQL_DATABASE_DB`",
                `"AZURE_SQL_MANAGED_INSTANCE_DB`",
                `"AwsNativeEbsVolume`",
                `"AwsNativeRdsInstance`",
                `"Ec2Instance`"
                ],
              `"complianceStatus`": [],
              `"protectionStatus`": [],
              `"orgId`": []
            },
            `"sortBy`": `"ArchiveStorage`",
            `"sortOrder`": `"DESC`"
          }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $JSON_Result = ($result.Content) | ConvertFrom-Json
        $RubrikCapacityUsage = @()
        $RubrikCapacityUsage += ((($JSON_Result.data).snappableConnection).edges).node
        while ((((($result.Content | ConvertFrom-Json).data).snappableConnection).pageInfo).hasNextPage -eq "True") {
            $endCursor = (((($result.content | convertFrom-Json).data).snappableConnection).pageInfo).endCursor
  Write-Host ("Looking at End Cursor " + $endCursor)
  $variables = "{
    `"first`": 1000,
    `"filter`": {
      `"objectType`": [
        `"AzureNativeVm`",
        `"AzureNativeManagedDisk`",
        `"AZURE_SQL_DATABASE_DB`",
        `"AZURE_SQL_MANAGED_INSTANCE_DB`"
      ],
      `"complianceStatus`": [],
      `"protectionStatus`": [],
      `"orgId`": []
    },
    `"sortBy`": `"ArchiveStorage`",
    `"sortOrder`": `"DESC`",
    `"after`": `"${endCursor}`"
  }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $JSON_Result = ($result.Content) | ConvertFrom-Json
        $Capadd = ((($JSON_Result.data).snappableConnection).edges).node
        $RubrikCapacityUsage += $Capadd
    }
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $RubrikCapacityUsage
    }
}
function Get-AWSVMAndTag{
    try{
        $query = "query AWSChargeBack {awsNativeEc2Instances{
            nodes{
              name
              objectType
              id
              tags{
                value
                key
                __typename
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
        $variables = "{
            `"filter`": [],
            `"sortBy`": `"NAME`",
            `"sortOrder`": `"ASC`",
            `"first`": 100
        }"
        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
          }
          $AWSInfo = @()
          $JSON_BODY = $JSON_BODY | ConvertTo-Json
          $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $AWSInfo += ((($result.Content | ConvertFrom-Json).data).awsNativeEc2Instances).Nodes
        while((((($result.Content | ConvertFrom-Json).data).awsNativeEc2Instances).pageInfo).hasNextPage -eq "True"){
            $endCursor = (((($result.content | convertFrom-Json).data).snappableConnection).pageInfo).endCursor
            Write-Host ("Looking at End Cursor " + $endCursor)
            $query = "query AWSChargeBack {awsNativeEc2Instances{
                nodes{
                  name
                  objectType
                  id
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
                }
              }
            }"
            $variables = "{
                `"filter`": [],
                `"sortBy`": `"NAME`",
                `"sortOrder`": `"ASC`",
                `"first`": 100
                `"after`": `"${endCursor}`"
            }"
            $JSON_BODY = $JSON_BODY | ConvertTo-Json
            $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
            $AWSInfo += ((($result.Content | ConvertFrom-Json).data).awsNativeEc2Instances).Nodes
        }
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $AWSInfo
    }
}
#Service Account for Authorization
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


$AzureTagsList = Get-AzureVMAndTag
$AWSTagsList = Get-AWSVMAndTag
$masterTagsList = $AzureTagsList + $AWSTagsList
$CloudCapacityUsage = Get-CloudNativeCapacity

$chargebackInfo = @()
#$AssetCount = ($masterTagsList | Measure-Object).count
#$index = 1
ForEach ($id in $masterTagsList){
    $VMCapacityInfo = $CloudCapacityUsage | Where-Object {$_.id -eq $id.id}
    ForEach($tag in $id.tags){
        Write-Host ("Collecting information on Object " + $id.name)
        $CloudSummaryInfo = New-Object PSobject
        $CloudSummaryInfo | Add-Member -NotePropertyName "TagValue" -NotePropertyValue $tag.value
        $CloudSummaryInfo | Add-Member -NotePropertyName "TagKey" -NotePropertyValue $tag.key
        $CloudSummaryInfo | Add-Member -NotePropertyName "ObjectID" -NotePropertyValue $id.id
        $CloudSummaryInfo | Add-Member -NotePropertyName "VM_Name" -NotePropertyValue $id.name
        $CloudSummaryInfo | Add-Member -NotePropertyName "objectType" -NotePropertyValue $id.objectType
        $CloudSummaryInfo | Add-Member -NotePropertyName "usedBytes" -NotePropertyValue $VMCapacityInfo.usedBytes
        $CloudSummaryInfo | Add-Member -NotePropertyName "PhysicalBytes" -NotePropertyValue $VMCapacityInfo.physicalBytes
        $CloudSummaryInfo | Add-Member -NotePropertyName "ReplicaBytes" -NotePropertyValue $VMCapacityInfo.replicaStorage
        $CloudSummaryInfo | Add-Member -NotePropertyName "ArchiveBytes" -NotePropertyValue $VMCapacityInfo.archiveStorage
        $chargebackInfo += $CloudSummaryInfo
        #$index++
    }
}
Write-Output ("CSV has been written to " + $Output_directory + "/RubrikCloudNativeCapacityReport-" + $mdate + ".csv")
$chargebackInfo | Export-Csv -NoTypeInformation ($Output_directory + "/RubrikCloudNativeCapacityReport-" + $mdate + ".csv")
