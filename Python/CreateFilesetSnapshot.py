#! /usr/bin/env python3


"""
CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Example:

python3 CreateFilesetSnapshot.py --keyfile ServiceAccount.json --filesetId "ce6137d2-5204-59e6-9dc2-99437ca421db" --slaId "6c28b114-e1b7-5486-89cb-dc8d0cf849b7"

"""

import argparse
import json
import requests

requests.packages.urllib3.disable_warnings()

def parseArguments():
    parser = argparse.ArgumentParser(description='Issue Upgrade Requests via Polaris')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    parser.add_argument('--filesetId', dest='filesetId', help="filesetId", default=None)
    parser.add_argument('--slaId', dest='slaId', help="slaId", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    json_keyfile = args.json_keyfile
    filesetId = args.filesetId
    slaId = args.slaId

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
    POLARIS_URL = session_url.rsplit("/", 1)[0]
    PolarisToken = access_token
    PolarisUri = POLARIS_URL + '/graphql'
    PolarisHeaders = {
    'Content-Type':'application/json',
    'Accept':'application/json',
    'Authorization':PolarisToken
    }

    #Define Fileset Mutation for Ondemand Backup
    query = """mutation TakeFilesetSnapshotMutation($config: BaseOnDemandSnapshotConfigInput!, $id: String!, $userNote: String) {
      createFilesetSnapshot(input: {config: $config, id: $id, userNote: $userNote}) {
        id
        status
      }
    }"""
    variables ={"id": filesetId,"config": {"slaId": slaId},"userNote": ""}
    variablesJson = json.dumps(variables)
          
    JSON_BODY = {"query": query,"variables": variablesJson}
    PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
    print("Initiating Ondemand Backup")
    print(PolarisQuery.json())
