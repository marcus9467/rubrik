#! /usr/bin/env python3


"""
Kick off Polaris driven CDM upgrades programatically!

python3 ./PolarisUpgrade.py --clusteruuid <Cluster UUID here> -k <Service account JSON here> --Upgrade --CDMversion 7.0.1-p2-15336

Kicks off an upgrade to CDM version 7.0.1-p2-15336 on the specified UUID

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import argparse
import json
import os
import pprint
import requests

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)



def parseArguments():
    parser = argparse.ArgumentParser(description='Issue Upgrade Requests via Polaris')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    parser.add_argument('--clusteruuid', dest='ClusterUUID', help="ClusterUUID", default=None)
    parser.add_argument('--packageUrl', dest='packageUrl', help="packageUrl for upgrade files", default=None)
    parser.add_argument('--packageMd5sum', dest='packageMd5sum', help="packageMd5sum for upgrade files", default=None)
    parser.add_argument('--downloadAndPrecheck', help='Kickoff a Download and precheck for the selected CDM Cluster(s)', action="store_true")
    parser.add_argument('--Upgrade', help='Kickoff a upgrade for the selected CDM Cluster(s)', action="store_true")
    parser.add_argument('--CDMversion', dest='CDMversion', help="specify the version of CDM to upgrade to", default=None)
    parser.add_argument('--CheckDownloadedVersion', dest='CheckDownloadedVersion', help="Check which versions of CDM are available on the cluster(s)", action="store_true")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    json_keyfile = args.json_keyfile
    packageMd5sum = args.packageMd5sum
    packageUrl = args.packageUrl
    ClusterUUID = args.ClusterUUID
    downloadAndPrecheck = args.downloadAndPrecheck
    Upgrade = args.Upgrade
    CDMversion = args.CDMversion
    CheckDownloadedVersion = args.CheckDownloadedVersion

    #Setup token auth 
    json_file = open(json_keyfile)
    json_key = json.load(json_file)
    json_file.close()
    session_url = json_key['access_token_uri']
    payload = {
        "client_id": json_key['client_id'],
        "client_secret": json_key['client_secret'],
        "name": json_key['name']
    }
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Accept': 'application/json, text/plain'
    }
    request = requests.post(session_url, json=payload, headers=headers, verify=False)
    del payload
    response_json = request.json()
    if 'access_token' not in response_json:
        print("Authentication failed!")
    access_token = response_json['access_token']
    #Setup token auth for direct graphql queries external to the SDK. 
    POLARIS_URL = session_url.rsplit("/", 1)[0]
    PolarisToken = access_token
    PolarisUri = POLARIS_URL + '/graphql'
    PolarisHeaders = {
    'Content-Type':'application/json',
    'Accept':'application/json',
    'Authorization':PolarisToken
    }

    if downloadAndPrecheck == True:
        print("Starting Download and precheck cycle for the selected Rubrik Cluster(s)")
          
        #Setup query and filters  
        variables = {}
        listClusterUuid = [ClusterUUID]
        variables['listClusterUuid'] = listClusterUuid
        variables['packageUrl'] = packageUrl
        variables['md5checksum'] = packageMd5sum

        query = """mutation DownloadCdmPackageMutation($listClusterUuid: [UUID!]!, $version: String, $packageUrl: String!, $md5checksum: String!, $size: Long) {
          startDownloadPackageBatchJob(listClusterUuid: $listClusterUuid, downloadVersion: $version, packageUrl: $packageUrl, md5checksum: $md5checksum, size: $size) {
            uuid
            jobId
            __typename
          }
        }"""
  
        JSON_BODY = {
            "query": query,
            "variables": variables
        }

        #Issue query to download and precheck the Upgrade package
        PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
  
        print("Starting Download and pre-check operations")     
        print(PolarisQuery.json())
    if CheckDownloadedVersion == True:
        print("Checking for available CDM packages on Clusters", ClusterUUID)
        query = """query UpgradesDownloadedVersionList {
            downloadedVersionList {
                count
                group
                __typename
                }
            }"""
        
        JSON_BODY = {
            "query": query
        }
        PolarisDownloadQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
        print(PolarisDownloadQuery.json())


    if Upgrade == True:
        print("Kicking off upgrade for the selected Rubrik Cluster(s)")
        variables = {}
        listClusterUuid = [ClusterUUID]
        variables['listClusterUuid'] = listClusterUuid
        variables['action'] = "START"
        variables['mode'] = "normal"
        variables['version'] = CDMversion
        variables['context_tag'] = "{\"should_rollback_on_error\":true,\"client\":\"polaris\"}"

        query = """mutation StartUpgradeJobsMutation($listClusterUuid: [UUID!]!, $mode: String!, $action: ActionType!, $version: String!, $context_tag: String) {
            startUpgradeBatchJob(listClusterUuid: $listClusterUuid, mode: $mode, action: $action, version: $version, context_tag: $context_tag) {
                upgradeJobReply {
                    message
                    success
                    __typename
                    }
                __typename
            }
        }"""
        
        JSON_BODY = {
            "query": query,
            "variables": variables
        }

        #Issue the Upgrade Request
        PolarisUpgradeQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
        print(PolarisUpgradeQuery.json())

















        """
        Queries captured


            mutation StartUpgradeJobsMutation($listClusterUuid: [UUID!]!, $mode: String!, $action: ActionEnum!, $version: String!, $context_tag: String) {
  startUpgradeBatchJob(listClusterUuid: $listClusterUuid, mode: $mode, action: $action, version: $version, context_tag: $context_tag) {
    UpgradeJobReply {
      message
      success
      __typename
    }
    __typename
  }
}

{
  "listClusterUuid": [
    "39b92c18-d897-4b55-a7f9-17ff178616d0"
  ],
  "action": "START",
  "mode": "normal",
  "version": "7.0.0-p1-14932",
  "context_tag": "{\"should_rollback_on_error\":true,\"client\":\"polaris\"}"
}
        """

    """
query UpgradesDownloadedVersionList {
  downloadedVersionList {
    count
    group
    __typename
  }
}

{
  "data": {
    "downloadedVersionList": [
      {
        "count": 2,
        "group": "7.0.0-p1-14932",
        "__typename": "GroupCount"
      }
    ]
  }
}









{
  "data": {
    "startUpgradeBatchJob": [
      {
        "UpgradeJobReply": {
          "message": "Succesfully started upgrade job action: START upgrade",
          "success": true,
          "__typename": "UpgradeJobReply"
        },
        "__typename": "UpgradeJobReplyWithUuid"
      }
    ]
  }
}



Status call 

query UpgradesClusterListQuery($first: Int, $after: String, $filter: CdmUpgradeInfoFilterInput, $sortBy: UpgradeInfoSortByEnum, $sortOrder: SortOrderEnum) {
  clusterWithUpgradesInfo(first: $first, after: $after, upgradeFilter: $filter, sortBy: $sortBy, sortOrder: $sortOrder) {
    edges {
      cursor
      node {
        id
        name
        type
        status
        version
        cdmUpgradeInfo {
          authorizedOperations {
            operations
            __typename
          }
          clusterJobStatus
          clusterUuid
          clusterStatus {
            message
            status
            statusInfo {
              finishedStates
              pendingStates
              currentTask
              currentState
              currentStateProgress
              overallProgress
              totalNodes
              completedNodes
              currentNode
              currentNodeState
              downloadVersion
              downloadProgress
              downloadRemainingTimeEstimateInSeconds
              downloadJobStatus
              __typename
            }
            __typename
          }
          currentStateProgress
          downloadedVersion
          finishedStates
          overallProgress
          pendingStates
          scheduleUpgradeAction
          scheduleUpgradeAt
          scheduleUpgradeMode
          stateMachineStatus
          stateMachineStatusAt
          upgradeStartAt
          upgradeEndAt
          previousVersion
          version
          versionStatus
          upgradeRecommendationInfo {
            recommendation
            releaseNotesLink
            __typename
          }
          upgradeEventSeriesId
          lastUpgradeDuration {
            rollingUpgradeDuration
            fastUpgradeDuration
            __typename
          }
          __typename
        }
        geoLocation {
          address
          __typename
        }
        lastConnectionTime
        metric {
          totalCapacity
          availableCapacity
          __typename
        }
        __typename
      }
      __typename
    }
    pageInfo {
      startCursor
      endCursor
      hasNextPage
      hasPreviousPage
      __typename
    }
    __typename
  }
}

{
  "filter": {
    "id": [
      "39b92c18-d897-4b55-a7f9-17ff178616d0"
    ]
  }
}

or use events endpoint with the following filters:
{
  "filters": {
    "objectType": [],
    "lastActivityStatus": [],
    "lastActivityType": [
      "Upgrade"
    ],
    "severity": [],
    "cluster": {
      "id": [
        "39b92c18-d897-4b55-a7f9-17ff178616d0"
      ]
    },
    "lastUpdated_gt": "2022-02-28T01:15:14.293Z",
    "objectName": ""
  },
  "first": 40
}
    """
        