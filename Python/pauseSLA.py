#! /usr/bin/env python3
"""
Pauses, or unpauses the specified SLA on the specified cluster

Example:
python3 pauseSLA.py --keyfile SampleKeyFile.json --pause true --cluster_uuid 86da734b-2fee-4fdc-bdc8-a73ab5648fb1 --sla_id 09425d87-40fa-4903-8a4a-3fc9677e044f

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

import json
import requests
requests.packages.urllib3.disable_warnings()
import datetime
import argparse

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parseArguments():
    parser = argparse.ArgumentParser(description='Parse Radar alerts from Polaris and send to syslog')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    # Use the custom str2bool function for the --pause argument
    parser.add_argument('-p', '--pause', dest='pause_sla', help="pause the sla true/false", type=str2bool, default=None)
    parser.add_argument('-s', '--sla_id', dest='sla_id', help="id of the sla to be paused", default=None)
    parser.add_argument('-c', '--cluster_uuids', dest='cluster_uuids', help="id of the cluster to be paused", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parseArguments()
    json_keyfile = args.json_keyfile
    pause_sla = args.pause_sla
    sla_id = args.sla_id
    cluster_uuids = args.cluster_uuids
    token_time = datetime.datetime.utcnow()
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
    def pause_resume_sla(cluster_uuids, pause_sla, sla_id):
      """
      Function to pause or resume an SLA for a set of clusters.

      :param cluster_uuids: A list of UUID strings for the clusters.
      :param pause_sla: Boolean indicating whether to pause (True) or resume (False) the SLA.
      :param sla_id: The SLA's unique identifier string.
      """

      # The mutation query template
      mutation = """
      mutation PauseResumeSLAMutation($clusterUuids: [String!]!, $pauseSla: Boolean!, $slaId: String!) {
        pauseSla(input: {clusterUuids: $clusterUuids, pauseSla: $pauseSla, slaId: $slaId}) {
          success
        }
      }
      """
    
      # Variables corresponding to the GraphQL mutation
      variables = {
          "clusterUuids": [cluster_uuids],
          "pauseSla": pause_sla,
          "slaId": sla_id
      }
    
      # Making the request
      response = requests.post(PolarisUri, json={'query': mutation, 'variables': variables}, headers=PolarisHeaders)
    
      # Assuming the API returns JSON, parse the response
      result = response.json()
    
      # You can add error handling and response parsing as needed
      return result

    # Example usage
    """
    cluster_uuids = "86da734b-2fee-4fdc-bdc8-a73ab5648fb1"
    pause_sla = True  # Or False to resume
    sla_id = "09425d87-40fa-4903-8a4a-3fc9677e044f"
    """
    # Call the function
    result = pause_resume_sla(cluster_uuids, pause_sla, sla_id)
    print(result)
