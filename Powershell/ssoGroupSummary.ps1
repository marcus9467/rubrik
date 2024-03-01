<#

.SYNOPSIS
This script will extract SSO Group information as it exists within RSC and dump it into a CSV file. 

.EXAMPLE
./ssoGroupSummary.ps1 -ServiceAccountJson $serviceAccountJson

This will extract the SSO group information and dump to CSV within the local directory. 

.NOTES
    Author  : Marcus Henderson <marcus.henderson@rubrik.com> 
    Created : March 01, 2024
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
function Get-ssoGroupsInfo{
    try{
        $query = "query UserGroupsOrgQuery(`$after: String, `$before: String, `$first: Int, `$last: Int, `$filter: GroupFilterInput, `$sortBy: GroupSortByParam, `$shouldIncludeGroupsWithoutRole: Boolean = false, `$isOrgDataVisible: Boolean = false) {
          groupsInCurrentAndDescendantOrganization(after: `$after, before: `$before, first: `$first, last: `$last, filter: `$filter, sortBy: `$sortBy, shouldIncludeGroupsWithoutRole: `$shouldIncludeGroupsWithoutRole) {
            edges {
              node {
                groupId
                groupName
                roles {
                  id
                  name
                  description
                  effectivePermissions {
                    objectsForHierarchyTypes {
                      objectIds
                      snappableType
                      __typename
                    }
                    operation
                    __typename
                  }
                  __typename
                }
                users {
                  email
                  __typename
                }
                ...OrganizationGroupFragment @include(if: `$isOrgDataVisible)
                __typename
              }
              __typename
            }
            __typename
          }
        }

        fragment OrganizationGroupFragment on Group {
          allOrgs {
            name
            __typename
          }
          __typename
        }"
        $variables = "{
            `"shouldIncludeGroupsWithoutRole`": false,
            `"isOrgDataVisible`": true,
            `"filter`": {
              `"roleIdsFilter`": [],
              `"orgIdsFilter`": []
            }
        }"

        $JSON_BODY = @{
            "variables" = $variables
            "query" = $query
        }

        $UserInfo = @()
        $JSON_BODY = $JSON_BODY | ConvertTo-Json
        $result = Invoke-WebRequest -Uri $POLARIS_URL -Method POST -Headers $headers -Body $JSON_BODY
        $UserInfo += (((($result.Content | convertFrom-Json).data).groupsInCurrentAndDescendantOrganization).edges).node
    }
    catch{
        Write-Error("Error $($_)")
    }
    finally{
        Write-Output $UserInfo
    }
}

$serviceAccountObj = Get-Content $ServiceAccountJson | ConvertFrom-Json
$Output_directory = (Get-Location).path
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

$ssoGroups = Get-ssoGroupsInfo

$GroupInfo = @()
foreach($group in $ssoGroups){
    $GroupSummary = New-Object PSobject
    $GroupSummary | Add-Member -NotePropertyName "GroupName" -NotePropertyValue $group.groupName
    $GroupSummary | Add-Member -NotePropertyName "GroupId" -NotePropertyValue $group.groupId
    $roles = ($group.roles).name -join ","
    $GroupSummary | Add-Member -NotePropertyName "Roles" -NotePropertyValue $roles
    $users = ($ssoGroups.users).email -join ","
    $GroupSummary | Add-Member -NotePropertyName "UsersInSsoGroup" -NotePropertyValue $users
    $GroupInfo += $GroupSummary
}
$groupInfo | Export-Csv -NoTypeInformation ($Output_directory + "/SSOGroupSummary-" + $mdate + ".csv")

disconnect-polaris
