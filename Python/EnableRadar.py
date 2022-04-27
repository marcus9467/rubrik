#! /usr/bin/env python3


"""
Script to enable Radar functionality on a per cluster basis.  

Example:
EnableRadarPassword.py --PolarisURL https://testpolaris.my.rubrik.com --username myuser@rubrik.com  --clusterUUID 486a581e-3c5d-47c2-8cd0-9cc294438b87
Password:
{'data': {'enableAutomaticFmdUpload': {'clusterId': '486a581e-3c5d-47c2-8cd0-9cc294438b87', 'enabled': True}}}

Enables Radar functionality on cluster 486a581e-3c5d-47c2-8cd0-9cc294438b87

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import argparse
import json
import requests
import getpass
requests.packages.urllib3.disable_warnings()

def parseArguments():
    parser = argparse.ArgumentParser(description='Parse Radar alerts from Polaris and send to syslog')
    parser.add_argument('--clusterUUID', dest='clusterUUID', help='specify the clusterUUID to enable')
    parser.add_argument('--username', dest='username', help='specify the username for login')
    #parser.add_argument('--password', dest='password', help='specify the password to login')
    parser.add_argument('--PolarisURL', dest='PolarisURL', help='specify the polaris URL to login')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    username = args.username
    #password = args.password
    password = getpass.getpass()
    PolarisURL = args.PolarisURL
    clusterUUID = args.clusterUUID
    
    #Setup auth session 
    session_url = PolarisURL + "/api/session"
    payload = {
        "username": username,
        "password": password,
        "domain_type": "localOrSSO",
        "mfa_remember_token": ""
    }
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Accept': 'application/json, text/plain'
    }
    request = requests.post(
    session_url,
    json=payload,
    headers=headers,
    )
    #request = requests.post(session_url, json=payload, headers=headers, verify=False)
    del payload
    response_json = request.json()
    #print(response_json) 
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

    mutation = """mutation EnableAutomaticFmdUpload($clusterUuid: UUID!, $enabled: Boolean!) {
  enableAutomaticFmdUpload(clusterUuid: $clusterUuid, enabled: $enabled) {
    clusterId
    enabled
  }
}"""
    variables = {}
    variables['enabled'] = True
    variables['clusterUuid'] = clusterUUID

    JSON_BODY = {
        "query": mutation,
        "variables": variables
    }
    PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
    Result = PolarisQuery.json()
    print(Result)
