function Get-UserInfo{
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
