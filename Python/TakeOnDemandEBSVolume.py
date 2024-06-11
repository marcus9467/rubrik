import json
import requests
import argparse

def get_access_token(service_account_file):
    # Load service account JSON file
    with open(service_account_file, 'r') as file:
        service_account = json.load(file)

    # Extract necessary fields
    client_id = service_account['client_id']
    client_secret = service_account['client_secret']
    access_token_uri = service_account['access_token_uri']

    # Prepare the connection data
    connection_data = {
        'client_id': client_id,
        'client_secret': client_secret
    }

    # Send a request to get the access token
    response = requests.post(access_token_uri, json=connection_data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Failed to obtain access token: {response.status_code} {response.text}")

def take_ebs_volume_snapshot(api_url, access_token, ebs_volume_id, sla_id):
    # Define the GraphQL mutation and variables
    mutation = """
    mutation TakeEBSVolumeSnapshotMutation($input: StartCreateAwsNativeEbsVolumeSnapshotsJobInput!) {
      startCreateAwsNativeEbsVolumeSnapshotsJob(input: $input) {
        jobIds {
          rubrikObjectId
          jobId
          __typename
        }
        errors {
          error
          __typename
        }
        __typename
      }
    }
    """

    variables = {
        "input": {
            "ebsVolumeIds": [ebs_volume_id],
            "retentionSlaId": sla_id
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    # Send the request to the Rubrik API
    response = requests.post(api_url, json={'query': mutation, 'variables': variables}, headers=headers)

    if response.status_code == 200:
        # Parse and return the JSON response
        return response.json()
    else:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

def main():
    parser = argparse.ArgumentParser(description='Take an EBS volume snapshot using Rubrik GraphQL API.')
    parser.add_argument('--api-url', required=True, help='Rubrik API endpoint URL.')
    parser.add_argument('--service-account-file', required=True, help='Path to the service account JSON file.')
    parser.add_argument('--ebs-volume-id', required=True, help='EBS volume ID to snapshot.')
    parser.add_argument('--sla-id', required=True, help='SLA ID for the snapshot retention.')

    args = parser.parse_args()

    # Get the access token
    access_token = get_access_token(args.service_account_file)

    # Call the function and print the result
    result = take_ebs_volume_snapshot(args.api_url, access_token, args.ebs_volume_id, args.sla_id)
    print(result)

if __name__ == '__main__':
    main()
