#! /usr/bin/env python3


"""
CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""





import argparse
import json
import os
import pprint
import requests
import numpy as np

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

def parseArguments():
    parser = argparse.ArgumentParser(description='Issue Upgrade Requests via Polaris')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    parser.add_argument('--sourceClusterFid', dest='sourceClusterFid', help="sourceClusterFid", default=None)
    parser.add_argument('--targetClusterFid', dest='targetClusterFid', help="targetClusterFid", default=None)
    parser.add_argument('--csvFile', dest='csvFile', help="csv File with VMware configs", default=None)
    parser.add_argument('--effectiveSLA', dest='effectiveSLA', help="Filter blueprint assignment based on effectiveSLA", default=None)
    parser.add_argument('--substring', dest='substring', help="Filter blueprint assignment based on VM naming convention", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    json_keyfile = args.json_keyfile
    substring = args.substring
    sourceClusterFid = args.sourceClusterFid
    targetClusterFid = args.targetClusterFid
    #csvFile = args.csvFile
    #effectiveSLA = args.effectiveSLA
    
    #isHydrationEnabled = args.RecoveryOptimized


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



    #Grab IDs of desired VMs
    query = """query VSphereVMsListQuery($first: Int!, $after: String, $filter: [Filter!]!, $sortBy: HierarchySortByField, $sortOrder: HierarchySortOrder) {
          vSphereVmNewConnection(filter: $filter, first: $first, after: $after, sortBy: $sortBy, sortOrder: $sortOrder) {
            edges {
              cursor
              node {
                id
                ...VSphereNameColumnFragment
                ...CdmClusterColumnFragment
                ...EffectiveSlaColumnFragment
                ...VSphereSlaAssignmentColumnFragment
                isRelic
                primaryClusterLocation {
                  id
                  name
                }
                physicalPath{
                  fid
                  name
                  objectType
                }
              }
            }
            pageInfo {
              startCursor
              endCursor
              hasNextPage
              hasPreviousPage
            }
          }
        }

        fragment VSphereNameColumnFragment on HierarchyObject {
          id
          name
          ...HierarchyObjectTypeFragment
        }

        fragment HierarchyObjectTypeFragment on HierarchyObject {
          objectType
        }

        fragment EffectiveSlaColumnFragment on HierarchyObject {
          id
          effectiveSlaDomain {
            ...EffectiveSlaDomainFragment
            ... on GlobalSla {
              description
            }
          }
          ... on CdmHierarchyObject {
            pendingSla {
              ...SLADomainFragment
            }
          }
        }

        fragment EffectiveSlaDomainFragment on SlaDomain {
          id
          name
          ... on GlobalSla {
            isRetentionLockedSla
          }
          ... on ClusterSlaDomain {
            fid
            cluster {
              id
              name
            }
            isRetentionLockedSla
          }
        }

        fragment SLADomainFragment on SlaDomain {
          id
          name
          ... on ClusterSlaDomain {
            fid
            cluster {
              id
              name
            }
          }
        }

        fragment CdmClusterColumnFragment on CdmHierarchyObject {
          replicatedObjectCount
          cluster {
            id
            name
            version
            status
          }
        }

        fragment VSphereSlaAssignmentColumnFragment on HierarchyObject {
          effectiveSlaSourceObject {
            fid
            name
            objectType
          }
          ...SlaAssignmentColumnFragment
        }

        fragment SlaAssignmentColumnFragment on HierarchyObject {
          slaAssignment
        }"""

    #SLA ID is hard coded here in the interest of time
    VMvariables = {}
    VMvariables['first'] = 100
    Filter = []
    RelicFilter = {}
    RelicFilter['field'] = "IS_RELIC"
    RelicFilter['texts'] = ["false"]
    Filter.append(RelicFilter)
    ReplicatedFilter = {}
    ReplicatedFilter['field'] = "IS_REPLICATED"
    ReplicatedFilter['texts'] = ["false"]
    Filter.append(ReplicatedFilter)
    isActiveFilter = {}
    isActiveFilter['field'] = "IS_ACTIVE"
    isActiveFilter['texts'] = ["true"]
    Filter.append(isActiveFilter)
    """
    #Optional code to filter based on effective SLA
    #effectiveSLAFilter = {}
    #effectiveSLAFilter['field'] = "EFFECTIVE_SLA"
    #effectiveSLAFilter['texts'] = ["effectiveSLA"]
    """
    #Filter.append(effectiveSLAFilter)
    VMvariables['filter'] = Filter
    VMvariables['sortBy'] = "NAME"
    VMvariables['sortOrder'] = "ASC"
    VMvariablesJSON = json.dumps(VMvariables)

    JSON_BODY = {
    "query": query,
    "variables": VMvariablesJSON
    }
    PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
    pageInfo = (PolarisQuery.json())['data']['vSphereVmNewConnection']['pageInfo']
    VMMasterList = []

    ShortList = PolarisQuery.json()['data']['vSphereVmNewConnection']['edges']
    for x in ShortList:
        if substring in x['node']['name']:
            VMMasterList.append(x)
    while(pageInfo['hasNextPage'] == True):
        afterFilter = pageInfo['endCursor']
        NewVariables = VMvariables
        NewVariables['after'] = afterFilter
        VMvariablesJSON = json.dumps(NewVariables)
        JSON_BODY = {
        "query": query,
        "variables": VMvariablesJSON
        }
        PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
        ShortList = PolarisQuery.json()['data']['vSphereVmNewConnection']['edges']
        for x in ShortList:
            if substring in x['node']['name']:
                VMMasterList.append(x)
        #VMMasterList.append((PolarisQuery.json())['data']['vSphereVmNewConnection']['edges'])
        pageInfo = (PolarisQuery.json())['data']['vSphereVmNewConnection']['pageInfo']
    FilterList = []

    #correct for this maybe use an arg to pass source cluster instead. 
    clusterName = "perfpod-cdm02"
    for x in VMMasterList:
        if clusterName in x['node']['cluster']['name']:
            FilterList.append(x)

    #Split Master List into 4 groups
    MasterList = np.array_split(FilterList, 4)
    ScaleName = 0
    for VMList in MasterList:
        #VMList =  VMList[0]

        #Setup mutation and filters  
        variables = {}
        variables['isHydrationEnabled'] = True
        ScaleNameStr = str(ScaleName)
        baseName = "ScaleTest"
        BluePrintName = baseName + ScaleNameStr
        variables['name'] = BluePrintName
        variables['sourceLocationId'] = sourceClusterFid
        variables['targetLocationId'] = targetClusterFid
        children = []
        for VM in VMList:
            childarray = {}
            childarray['fid'] = VM['node']['id']
            childarray['snappableType'] = "VmwareVirtualMachine"
            childarray['bootPriority'] = 0
            children.append(childarray)
        #End loop for initial blueprint variable build
        variables['children'] = children
        variables['status'] = "NotConfigured"

        query = """mutation CreateBlueprintMutation($name: String!, $sourceLocationId: String!, $targetLocationId: String, $children: [AppBlueprintChildInput!]!, $status: BlueprintStatusEnum!, $isHydrationEnabled: Boolean) {
        createBlueprint(children: $children, name: $name, sourceLocationId: $sourceLocationId, targetLocationId: $targetLocationId, status: $status, enableHydration: $isHydrationEnabled) {
          id
          name
          status
          __typename
            }
        }"""

        JSON_BODY = {
        "query": query,
        "variables": variables
        }
        PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
        print(PolarisQuery.json())

        BluePrintInfo = PolarisQuery.json()
        blueprintId = BluePrintInfo['data']['createBlueprint']['id']

        querynew = """mutation CreateBlueprintResourcesMappingMutation($blueprintId: UUID!, $recoverySpecType: RecoverySpecTypeEnum!, $recoverySpecs: [AppBlueprintRecoverySpecCreateReqInputType!]!) {
            createBlueprintRecoverySpec(blueprintId: $blueprintId, specType: $recoverySpecType, recoveryConfigs: $recoverySpecs) {
              planType
              childRecoverySpecs {
                snappableFid
                recoverySpecFid
                __typename
              }
              __typename
            }
        }"""
        BluePrintVariables = {}
        BluePrintVariables['blueprintId'] = blueprintId
        BluePrintVariables['recoverySpecType'] = "VMWARE_VM"
        recoverySpecs = []

        prodPlan = {}
        prodPlan['planType'] = "PROD"
        #List of VM Configs here in array
        childRecoverySpecs = [] 
        count = 0
        for VM in VMList:
            snappableArray = {}
            snappableArray['snappableId'] = VM['node']['id']
            # This is the fid of the SLA for the target location, need to make this more robust, likely need another graphql call to provide choices around target SLAs
            snappableArray['slaId'] = "730a314e-e5a9-5e35-82b3-a8983d7ca8e0" 
            vSphereSpec = {}
            target = {}
            target['vcenterId'] = "69c8301e-de84-5039-abce-f43217f3d19d"
            target['vcenterName'] = "rp-vcsa01.perf.rubrik.com"
            target['computeClusterId'] = "152229b8-1810-5434-ad2f-17b84eb2ffd4"
            target['computeClusterName'] = "perf-cluster"
            vSphereSpec['target'] = target
            volumes = []
            volumearray1 = {"key": "0", "dataStoreId": None, "datastoreClusterId": "5276153f-2a41-5126-a28a-4d4e0690222a"}
            volumearray2 = {"key": "2000", "dataStoreId": None, "label": "Hard disk 1", "datastoreClusterId": "5276153f-2a41-5126-a28a-4d4e0690222a"}
            volumes.append(volumearray1)
            volumes.append(volumearray2)
            vSphereSpec['volumes'] = volumes
            nics = []
            nicsArray = {"isPrimaryNic": True, "key": "4000", "networkType": "DHCP", "networkId": "c0262fde-dd5a-50c7-919c-eb85109dc97c"}
            if(BluePrintName == "ScaleTest0"):
                nicsArray = {"isPrimaryNic": True, "key": "4000", "networkType": "DHCP", "networkId": "c2965024-e8a9-564b-b584-07d6ca3926af"}
            nics.append(nicsArray)
            vSphereSpec['nics'] = nics
            recoverySpec = {}
            recoverySpec['vSphereSpec'] = vSphereSpec
            snappableArray['recoverySpec'] = recoverySpec
            childRecoverySpecs.append(snappableArray)
            count+=1
        #End loop for prod and test recovery specs
        prodPlan['childRecoverySpecs'] = childRecoverySpecs

        #Copy Prod into Test
        testPlan = {}
        testPlan['planType'] = "TEST"
        testPlan['childRecoverySpecs'] = childRecoverySpecs

        PROD_LOCAL = {}
        PROD_LOCAL['planType'] = "PROD_LOCAL"
        localrecoveryspecs = []
        for VM in VMList:
            snappableArray = {}
            snappableArray['snappableId'] = VM['node']['id']
            snappableArray['slaId'] = "730a314e-e5a9-5e35-82b3-a8983d7ca8e0" 
            vSphereSpec = {}
            esxHostname = VM['node']['physicalPath'][0]['name']
            esxHostId = VM['node']['physicalPath'][0]['fid']
            target = {}
            target['hostId'] = esxHostId
            target['hostName'] = esxHostname
            vSphereSpec['target'] = target
            nics = {"key": "4000"}
            volumes = {"key": "2000"}
            vSphereSpec['nics'] = nics
            vSphereSpec['volumes'] = volumes
            recoverySpec = {}
            recoverySpec['vSphereSpec'] = vSphereSpec
            snappableArray['recoverySpec'] = recoverySpec
            localrecoveryspecs.append(snappableArray)
        #End loop for PROD_LOCAL Build
        PROD_LOCAL['childRecoverySpecs'] = localrecoveryspecs

        #Build out the recovery plans
        recoverySpecs.append(prodPlan)
        recoverySpecs.append(testPlan)
        recoverySpecs.append(PROD_LOCAL)

        BluePrintVariables['recoverySpecs'] = recoverySpecs
        BluePrintVariablesJSON = json.dumps(BluePrintVariables)
        JSON_BODYNew = {
        "query": querynew,
        "variables": BluePrintVariablesJSON
        }
        PolarisQueryNew = requests.post(PolarisUri, json=JSON_BODYNew, headers=PolarisHeaders)
        print(PolarisQueryNew.json())
        print("Finished creating Blueprint called" + BluePrintName)
        ScaleName+=1
