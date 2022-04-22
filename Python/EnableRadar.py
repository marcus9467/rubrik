#! /usr/bin/env python3


"""
Script to enable Radar functionality on a per cluster basis.  

Example:
python3 EnableRadar.py --keyfile RadarAutomation.json --clusterUUID 486a581e-3c5d-47c2-8cd0-9cc294438b87

Enables Radar functionality on cluster 486a581e-3c5d-47c2-8cd0-9cc294438b87

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import argparse
import json
import requests

requests.packages.urllib3.disable_warnings()

def parseArguments():
    parser = argparse.ArgumentParser(description='Parse Radar alerts from Polaris and send to syslog')
    parser.add_argument('--clusterUUID', dest='clusterUUID', help='specify the clusterUUID to enable')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    json_keyfile = args.json_keyfile
    clusterUUID = args.clusterUUID
    
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

    mutation = """mutation ToggleRadarPrefsMutation($clusterId: UUID!, $enabled: Boolean!) {
  enableAutomaticFmdUpload(clusterUuid: $clusterId, enabled: $enabled) {
    clusterId
    enabled
    __typename
  }
}"""
    variables = {}
    variables['enabled'] = True
    variables['clusterId'] = clusterUUID

    JSON_BODY = {
    "query": mutation,
    "variables": variables
    }
    PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
    Result = PolarisQuery.json()
    print(Result)
