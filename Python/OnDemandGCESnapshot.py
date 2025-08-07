"""
Rubrik Security Cloud (RSC) On-Demand Snapshot Script

This script provides functionality to connect to Rubrik Security Cloud using a service account JSON file,
initiate on-demand snapshots for multiple GCP VMs, and then disconnect from the session. It also includes
a feature to monitor the status of these backup jobs until completion, reporting snapshot creation times
and the delta between them.

Additionally, it now supports an optional flag to list all discoverable GCP VM IDs and their names.

It leverages Python's 'requests' library for API communication and 'argparse' for command-line argument parsing.

Usage for On-Demand Snapshot:
    python3 OnDemandGCESnapshot.py --VMIDs "vm_id_1,vm_id_2,..." --slaId "your_sla_id" --serviceAccountJson "path/to/your/serviceAccount.json"

Usage for Listing GCP VM IDs:
    python3 OnDemandGCESnapshot.py --listVMIDs --serviceAccountJson "path/to/your/serviceAccount.json"

Arguments:
    --VMIDs (str, required for snapshot): A comma-separated list of UUIDs for the GCP VMs you want to back up.
                                         Example: "37771f07-a608-47e5-a2eb-061289ab456b,36e3ae95-0b48-454d-be9b-c8569f63b62c"
    --slaId (str, required for snapshot): The UUID of the SLA Domain to use for retention of the on-demand snapshot.
                                          Example: "00000000-0000-0000-0000-000000000002" (often a 'Do not retain' SLA)
    --serviceAccountJson (str, required): The file path to your Rubrik Security Cloud service account JSON file.
                                          This file contains your client_id, client_secret, and access_token_uri.
    --pollInterval (int, optional): Interval in seconds for polling backup status (default: 15).
    --timeout (int, optional): Maximum time in minutes to wait for monitoring backup status (default: 60).
    --listVMIDs (flag, optional): If present, the script will list all GCP VM IDs and their names and then exit.
                                  When this flag is used, --VMIDs and --slaId are not required.

Example Service Account JSON Structure:
{
    "client_id": "client|XXXXXXXXX",
    "client_secret": "XXXXXXXXXXX",
    "name": "XXXXXXXXXX",
    "access_token_uri": "https://your.rsc.domain/api/client_token"
}

Error Handling:
The script includes robust error handling for file not found, invalid JSON, connection failures,
and API response issues.

Author: Marcus Henderson (original PowerShell concept)
Python Conversion & Enhancements: Gemini AI
"""

import json
import requests
import os
import argparse
import time
import sys # Import sys for exiting the script
from datetime import datetime, timedelta, timezone

class RSCSession:
    """
    Manages authentication and session with Rubrik Security Cloud (RSC).
    """
    def __init__(self):
        self.access_token = None
        self.headers = {}
        self.polaris_url = None
        self.logout_url = None
        # Dictionary to store VM names for human-readable output
        self.vm_names = {}

    def connect_rsc(self, service_account_json_path: str):
        """
        Provides the initial authorization to Rubrik Security Cloud using a service account JSON file.

        Args:
            service_account_json_path (str): Path to the service account JSON file.
        """
        # Check if the service account JSON file exists
        if not os.path.exists(service_account_json_path):
            raise FileNotFoundError(f"Service account JSON file not found at: {service_account_json_path}")

        try:
            # Open and load the service account JSON file
            with open(service_account_json_path, 'r') as f:
                service_account_obj = json.load(f)
        except json.JSONDecodeError:
            # Handle cases where the JSON file is malformed
            raise ValueError(f"Invalid JSON in file: {service_account_json_path}")

        # Validate required fields in the service account JSON
        required_fields = ['client_id', 'client_secret', 'access_token_uri']
        for field in required_fields:
            if field not in service_account_obj or not service_account_obj[field]:
                raise ValueError(f"The provided JSON is missing or has an empty field: '{field}'. "
                                 "Please check the service account JSON from Polaris.")

        # Prepare the connection data payload for the POST request
        connection_data = {
            'client_id': service_account_obj['client_id'],
            'client_secret': service_account_obj['client_secret']
        }

        access_token_uri = service_account_obj['access_token_uri']

        print(f"Attempting to connect to RSC using: {access_token_uri}")
        try:
            # Make the POST request to the access token URI
            # The 'json' parameter automatically sets Content-Type to application/json
            response = requests.post(access_token_uri, json=connection_data)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            polaris_data = response.json() # Parse the JSON response
        except requests.exceptions.RequestException as e:
            # Catch any request-related errors (network issues, invalid URL, etc.)
            raise ConnectionError(f"Unable to connect to RSC: {e}")
        except json.JSONDecodeError:
            # Handle cases where the response from the server is not valid JSON
            raise ValueError("Failed to parse JSON response from RSC. Unexpected response format.")

        # Check if the access token was successfully received
        if 'access_token' in polaris_data and polaris_data['access_token']:
            self.access_token = polaris_data['access_token']
            # Construct the headers for subsequent API calls
            self.headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            }
            # Derive GraphQL and logout URLs based on the access token URI, similar to PowerShell's .replace()
            self.polaris_url = service_account_obj['access_token_uri'].replace("client_token", "graphql")
            self.logout_url = service_account_obj['access_token_uri'].replace("client_token", "session")
            print("Successfully connected to Rubrik Security Cloud.")
            return polaris_data # Return the full response data
        else:
            # If no access token is found in the response
            raise ConnectionError("Authentication failed: No access_token received.")

    def disconnect_rsc(self):
        """
        Disconnects the previously established session with Rubrik Security Cloud.
        """
        if not self.logout_url:
            print("Warning: No logout URL found. Session might not have been established or already disconnected.")
            return

        if not self.headers:
            print("Warning: No headers found. Cannot send logout request without authentication headers.")
            return

        print(f"Attempting to disconnect from RSC at: {self.logout_url}")
        try:
            # Perform a DELETE request to the logout URL using the established headers
            response = requests.delete(self.logout_url, headers=self.headers)
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

            if response.status_code == 204:
                print("Successfully logged out from Rubrik Security Cloud.")
                # Clear session-related attributes upon successful logout
                self.access_token = None
                self.headers = {}
                self.polaris_url = None
                self.logout_url = None
            else:
                print(f"Logout failed with status code: {response.status_code}")
                # Optionally print response content for debugging
                # print(f"Response content: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to logout due to a request error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during logout: {e}")

    def take_on_demand_snapshot(self, snappable_ids: list[str], retention_sla_id: str):
        """
        Initiates an on-demand snapshot for one or more VMs.

        Args:
            snappable_ids (list[str]): A list of UUIDs (strings) for the VMs to be snapshotted.
            retention_sla_id (str): The UUID (string) of the SLA Domain to use for retention.

        Returns:
            dict: The JSON response from the API call.
        """
        if not self.polaris_url or not self.headers:
            raise ConnectionError("Not connected to RSC. Please call connect_rsc first.")

        # Define the GraphQL query and variables as provided in the example
        graphql_payload = {
            "operationName": "TakeOnDemandSnapshotMutation",
            "variables": {
                "snappableIds": snappable_ids,
                "retentionSlaId": retention_sla_id
            },
            "query": """
mutation TakeOnDemandSnapshotMutation($retentionSlaId: String!, $snappableIds: [UUID!]!) {
  takeOnDemandSnapshot(
    input: {slaId: $retentionSlaId, workloadIds: $snappableIds}
  ) {
    errors {
      error
      __typename
    }
    __typename
  }
}
"""
        }

        print(f"Initiating on-demand snapshot for {len(snappable_ids)} VMs...")
        try:
            # Send the POST request to the GraphQL endpoint
            response = requests.post(self.polaris_url, headers=self.headers, json=graphql_payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            response_data = response.json()
            print("On-demand snapshot request sent.")
            return response_data
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to take on-demand snapshot: {e}")
        except json.JSONDecodeError:
            raise ValueError("Failed to parse JSON response for on-demand snapshot. Unexpected response format.")

    def monitor_backup_status(self, vm_ids: list[str], poll_interval_seconds: int = 30, timeout_minutes: int = 60):
        """
        Monitors the backup status of specified VMs until all are complete or a timeout occurs.
        It also tracks and reports the snapshot creation time and the delta between snapshots.

        Args:
            vm_ids (list[str]): A list of VM UUIDs to monitor.
            poll_interval_seconds (int): How often to poll the API (in seconds).
            timeout_minutes (int): Maximum time to wait for backups to complete (in minutes).

        Returns:
            dict: A dictionary with VM ID as key and its final status as value.
        """
        if not self.polaris_url or not self.headers:
            raise ConnectionError("Not connected to RSC. Please call connect_rsc first.")

        # Define terminal states for backup activities
        TERMINAL_STATES = {"Failed", "Succeeded", "SucceededWithWarnings", "Success", "Canceled"}
        SNAPSHOT_CREATED_MESSAGE_PART = "Successfully created on-demand snapshot"

        # Initialize status tracking for each VM
        vm_statuses = {vm_id: "Pending" for vm_id in vm_ids}
        # Dictionary to store snapshot creation timestamps (datetime objects)
        snapshot_creation_times = {vm_id: None for vm_id in vm_ids}

        # Populate vm_names for human-readable output
        for vm_id in vm_ids:
            self.vm_names[vm_id] = self.vm_names.get(vm_id, "Unknown VM Name") # Use existing name if available, else placeholder

        start_time = datetime.now(timezone.utc)
        # Set initial 'lastUpdatedTimeGt' to a few minutes before script start to catch recent events
        # This helps ensure we don't miss events that happened just before monitoring started.
        latest_activity_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        print(f"\n--- Monitoring backup status for {len(vm_ids)} VMs (polling every {poll_interval_seconds}s, timeout {timeout_minutes}m) ---")

        consecutive_bad_requests = 0 # Counter for consecutive 400 errors

        while True:
            current_time = datetime.now(timezone.utc)
            if (current_time - start_time).total_seconds() > timeout_minutes * 60:
                print(f"Timeout reached ({timeout_minutes} minutes). Exiting status monitoring.")
                break

            all_completed = True
            for vm_id in vm_ids:
                # Check if all VMs have reached any of the defined terminal states
                if vm_statuses[vm_id] not in TERMINAL_STATES:
                    all_completed = False
                    break # No need to check further if one is not in a terminal state

            if all_completed:
                print("All requested VM backups have reached a final status.")
                break # Exit the main polling loop

            # Exit if 3 consecutive 400 Bad Request errors occur
            if consecutive_bad_requests >= 3:
                print("Exiting status monitoring due to 3 consecutive '400 Bad Request' errors.")
                break

            print(f"Polling for updates... (Elapsed: {int((current_time - start_time).total_seconds() / 60)}m {int((current_time - start_time).total_seconds() % 60)}s)")

            graphql_payload = {
                "operationName": "EventSeriesListQuery",
                "variables": {
                    "filters": {
                        "objectType": ["GCP_NATIVE_GCE_INSTANCE"],
                        "lastActivityType": ["BACKUP"],
                        "lastUpdatedTimeGt": latest_activity_time, # Only get events newer than this
                        # Removed "objectIds" filter as it caused 400 Bad Request errors
                    },
                    "first": 50 # Fetch up to 50 events per poll
                },
                "query": """
query EventSeriesListQuery($after: String, $filters: ActivitySeriesFilter, $first: Int, $sortBy: ActivitySeriesSortField,
                           $sortOrder: SortOrder) {
  activitySeriesConnection(
    after: $after
    first: $first
    filters: $filters
    sortBy: $sortBy
    sortOrder: $sortOrder
  ) {
    edges {
      cursor
      node {
        ...EventSeriesFragment
        cluster {
          id
          name
          timezone
          __typename
        }
        activityConnection(first: 1) {
          nodes {
            id
            message
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    pageInfo {
      endCursor
      hasNextPage
      hasPreviousPage
      __typename
    }
    __typename
  }
}

fragment EventSeriesFragment on ActivitySeries {
  id
  fid
  activitySeriesId
  lastUpdated
  lastActivityType
  lastActivityStatus
  objectId
  objectName
  objectType
  severity
  progress
  isCancelable
  isPolarisEventSeries
  location
  effectiveThroughput
  dataTransferred
  logicalSize
  organizations {
    id
    name
    __typename
  }
  clusterUuid
  clusterName
  __typename
}
"""
            }

            try:
                response = requests.post(self.polaris_url, headers=self.headers, json=graphql_payload)
                response.raise_for_status()
                response_data = response.json()
                consecutive_bad_requests = 0 # Reset counter on successful request

                new_latest_activity_time = latest_activity_time # Initialize with current latest_activity_time

                if response_data and 'data' in response_data and 'activitySeriesConnection' in response_data['data']:
                    edges = response_data['data']['activitySeriesConnection'].get('edges', [])
                    for edge in edges:
                        node = edge.get('node')
                        if node:
                            obj_id = node.get('objectId')
                            status = node.get('lastActivityStatus')
                            obj_name = node.get('objectName')
                            updated_time_str = node.get('lastUpdated')
                            activity_message = ""
                            if node.get('activityConnection') and node['activityConnection'].get('nodes'):
                                activity_message = node['activityConnection']['nodes'][0].get('message', '')

                            # Process only the VMs we are interested in
                            if obj_id in vm_ids:
                                # Store VM name for later use in final report
                                self.vm_names[obj_id] = obj_name

                                if updated_time_str:
                                    try:
                                        # Parse the time and update new_latest_activity_time if this event is newer
                                        current_event_time = datetime.fromisoformat(updated_time_str.replace('Z', '+00:00'))
                                        if current_event_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z') > new_latest_activity_time:
                                            new_latest_activity_time = current_event_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                                        # Check for snapshot creation message and store timestamp
                                        if SNAPSHOT_CREATED_MESSAGE_PART in activity_message and snapshot_creation_times[obj_id] is None:
                                            snapshot_creation_times[obj_id] = current_event_time
                                            print(f"VM: {obj_name} ({obj_id}) - Snapshot Created At: {updated_time_str}")

                                    except ValueError:
                                        pass # Handle malformed timestamps

                                # Only print if status changed or it's the first time we see this VM's status
                                if vm_statuses[obj_id] != status:
                                    print(f"VM: {obj_name} ({obj_id}) - Status: {status} - Message: {activity_message} (Last Updated: {updated_time_str})")
                                vm_statuses[obj_id] = status

                latest_activity_time = new_latest_activity_time # Update for the next poll

            except requests.exceptions.HTTPError as e:
                # Catch specific HTTP errors like 400 Bad Request
                print(f"HTTP Error during status polling: {e}")
                if e.response is not None and e.response.status_code == 400:
                    consecutive_bad_requests += 1
                    print(f"Consecutive 400 Bad Request errors: {consecutive_bad_requests}/3")
                else:
                    consecutive_bad_requests = 0 # Reset if it's a different HTTP error
            except requests.exceptions.RequestException as e:
                # Catch other request-related errors (network, timeout, etc.)
                print(f"Network/Request Error during status polling: {e}")
                consecutive_bad_requests = 0 # Reset on non-400 error
            except json.JSONDecodeError:
                print("Error parsing JSON response during status polling. Response might not be valid JSON.")
                consecutive_bad_requests = 0 # Reset on non-400 error
            except Exception as e:
                print(f"An unexpected error occurred during status polling: {e}")
                consecutive_bad_requests = 0 # Reset on non-400 error

            time.sleep(poll_interval_seconds)

        # Calculate and print snapshot delta after the loop
        completed_snapshot_times = [t for t in snapshot_creation_times.values() if t is not None]
        if len(completed_snapshot_times) >= 2:
            earliest_snapshot = min(completed_snapshot_times)
            latest_snapshot = max(completed_snapshot_times)
            delta_seconds = (latest_snapshot - earliest_snapshot).total_seconds()
            print(f"\n--- Snapshot Creation Time Delta ---")
            print(f"Earliest Snapshot: {earliest_snapshot.isoformat(timespec='seconds')}")
            print(f"Latest Snapshot:   {latest_snapshot.isoformat(timespec='seconds')}")
            print(f"Time Delta: {delta_seconds:.2f} seconds")
        elif len(completed_snapshot_times) == 1:
            print(f"\nOnly one VM ({self.vm_names.get(list(snapshot_creation_times.keys())[0], 'Unknown VM')}) completed snapshot successfully. No delta to calculate.")
        else:
            print("\nNo successful snapshot creation times recorded for the monitored VMs.")


        return vm_statuses

    def list_gcp_vms(self):
        """
        Fetches and prints a list of all GCP VM IDs and their native names.
        """
        if not self.polaris_url or not self.headers:
            raise ConnectionError("Not connected to RSC. Please call connect_rsc first.")

        print("\n--- Fetching GCP VM IDs and Names ---")
        all_vms = []
        has_next_page = True
        after_cursor = None

        while has_next_page:
            graphql_payload = {
                "operationName": "GCPInstancesListQuery",
                "variables": {
                    "first": 50, # Fetch 50 VMs per request
                    "after": after_cursor,
                    "sortBy": "GCP_INSTANCE_NATIVE_NAME",
                    "sortOrder": "ASC",
                    "filters": {
                        "effectiveSlaFilter": None,
                        "nameOrIdSubstringFilter": None,
                        "relicFilter": {"relic": False},
                        "regionFilter": None,
                        "networkFilter": None,
                        "labelFilter": None,
                        "projectFilter": None,
                        "machineTypeFilter": None,
                        "orgFilter": None,
                        "fileIndexingFilter": None
                    },
                    "isMultitenancyEnabled": True,
                    "includeRscNativeObjectPendingSla": True
                },
                "query": """
query GCPInstancesListQuery($first: Int, $after: String, $sortBy: GcpNativeGceInstanceSortFields, $sortOrder: SortOrder, $filters: GcpNativeGceInstanceFilters, $isMultitenancyEnabled: Boolean = false, $includeRscNativeObjectPendingSla: Boolean!) {
  gcpNativeGceInstances(first: $first, after: $after, sortBy: $sortBy, sortOrder: $sortOrder, gceInstanceFilters: $filters) {
    edges {
      cursor
      node {
        id
        nativeId
        nativeName
        vpcName
        networkHostProjectNativeId
        region
        zone
        isRelic
        machineType
        ...EffectiveSlaColumnFragment
        effectiveSlaDomain {
          ... on GlobalSlaReply {
            archivalSpecs {
              __typename
            }
            __typename
          }
          __typename
        }
        ...OrganizationsColumnFragment @include(if: $isMultitenancyEnabled)
        gcpNativeProject {
          id
          name
          nativeId
          status
          __typename
        }
        authorizedOperations
        fileIndexingStatus
        ...GcpSlaAssignmentColumnFragment
        fileIndexingStatus
        __typename
      }
      __typename
    }
    pageInfo {
      endCursor
      hasNextPage
      hasPreviousPage
      __typename
    }
    __typename
  }
}

fragment OrganizationsColumnFragment on HierarchyObject {
  allOrgs {
    fullName
    __typename
  }
  __typename
}

fragment EffectiveSlaColumnFragment on HierarchyObject {
  id
  effectiveSlaDomain {
    ...EffectiveSlaDomainFragment
    ... on GlobalSlaReply {
      description
      __typename
    }
    __typename
  }
  ... on CdmHierarchyObject {
    pendingSla {
      ...SLADomainFragment
      __typename
    }
    __typename
  }
  ... on PolarisHierarchyObject {
    rscNativeObjectPendingSla @include(if: $includeRscNativeObjectPendingSla) {
      ...CompactSLADomainFragment
      __typename
    }
    __typename
  }
  __typename
}

fragment EffectiveSlaDomainFragment on SlaDomain {
  id
  name
  ... on GlobalSlaReply {
    isRetentionLockedSla
    retentionLockMode
    __typename
  }
  ... on ClusterSlaDomain {
    fid
    cluster {
      id
      name
      __typename
    }
    isRetentionLockedSla
    retentionLockMode
    __typename
  }
  __typename
}

fragment SLADomainFragment on SlaDomain {
  id
  name
  ... on ClusterSlaDomain {
    fid
    cluster {
      id
      name
      __typename
    }
    __typename
  }
  __typename
}

fragment CompactSLADomainFragment on CompactSlaDomain {
  id
  name
  __typename
}

fragment GcpSlaAssignmentColumnFragment on HierarchyObject {
  effectiveSlaSourceObject {
    fid
    name
    objectType
    __typename
  }
  slaAssignment
  __typename
}
"""
            }

            try:
                response = requests.post(self.polaris_url, headers=self.headers, json=graphql_payload)
                response.raise_for_status()
                response_data = response.json()

                if response_data and 'data' in response_data and 'gcpNativeGceInstances' in response_data['data']:
                    connection = response_data['data']['gcpNativeGceInstances']
                    edges = connection.get('edges', [])
                    for edge in edges:
                        node = edge.get('node')
                        if node:
                            vm_id = node.get('id')
                            vm_name = node.get('nativeName')
                            if vm_id and vm_name:
                                all_vms.append({"id": vm_id, "name": vm_name})

                    page_info = connection.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    after_cursor = page_info.get('endCursor')
                else:
                    print("No GCP instances found or unexpected API response structure.")
                    has_next_page = False # Stop polling if response is malformed

            except requests.exceptions.RequestException as e:
                print(f"Error fetching GCP VM list: {e}")
                has_next_page = False # Stop polling on error
            except json.JSONDecodeError:
                print("Error parsing JSON response while listing GCP VMs.")
                has_next_page = False # Stop polling on error
            except Exception as e:
                print(f"An unexpected error occurred while listing GCP VMs: {e}")
                has_next_page = False # Stop polling on error

        if all_vms:
            print("\n--- Discovered GCP VM IDs and Names ---")
            for vm in all_vms:
                print(f"VM Name: {vm['name']} | VM ID: {vm['id']}")
            print(f"\nTotal Discovered VMs: {len(all_vms)}")
        else:
            print("\nNo GCP VMs found.")


# Example Usage:
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Rubrik Security Cloud On-Demand Snapshot Script")
    parser.add_argument(
        "--VMIDs",
        type=str,
        required=False, # Not required if --listVMIDs is used
        help="Comma-separated list of VM IDs (UUIDs) to be backed up."
    )
    parser.add_argument(
        "--slaId",
        type=str,
        required=False, # Not required if --listVMIDs is used
        help="The SLA ID (UUID) to use for retention of the on-demand snapshot."
    )
    parser.add_argument(
        "--serviceAccountJson",
        type=str,
        required=True,
        help="Path to the Rubrik Security Cloud service account JSON file."
    )
    parser.add_argument(
        "--pollInterval",
        type=int,
        default=15,
        help="Interval in seconds for polling backup status (default: 15)."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout in minutes for monitoring backup status (default: 60)."
    )
    parser.add_argument(
        "--listVMIDs",
        action='store_true', # This makes it a boolean flag
        help="List all GCP VM IDs and their names and exit."
    )

    args = parser.parse_args()

    rsc_session = RSCSession()
    try:
        # Connect to RSC first, as it's needed for both listing and snapshot operations
        print("\n--- Attempting to connect to RSC ---")
        rsc_session.connect_rsc(args.serviceAccountJson)
        print("\nConnection successful!")
        print(f"Access Token (first 10 chars): {rsc_session.access_token[:10]}...")
        print(f"Polaris GraphQL URL: {rsc_session.polaris_url}")
        print(f"Logout URL: {rsc_session.logout_url}")

        if args.listVMIDs:
            # If --listVMIDs is present, list VMs and exit
            rsc_session.list_gcp_vms()
            sys.exit(0) # Exit cleanly after listing VMs

        # --- Proceed with snapshot and monitoring if --listVMIDs is NOT present ---
        # Validate that VMIDs and slaId are provided for snapshot operation
        if not args.VMIDs or not args.slaId:
            parser.error("--VMIDs and --slaId are required for snapshot operations when --listVMIDs is not used.")

        vm_ids_to_backup = [vm_id.strip() for vm_id in args.VMIDs.split(',') if vm_id.strip()]
        sla_id_for_retention = args.slaId
        poll_interval = args.pollInterval
        timeout = args.timeout

        # 2. Take On-Demand Snapshot
        print("\n--- Attempting to take on-demand snapshots ---")
        if rsc_session.polaris_url and rsc_session.headers:
            snapshot_response = rsc_session.take_on_demand_snapshot(vm_ids_to_backup, sla_id_for_retention)
            print("\nOn-demand snapshot API response:")
            # Only print the errors section if there are errors, otherwise keep it concise
            if snapshot_response and 'data' in snapshot_response and \
               'takeOnDemandSnapshot' in snapshot_response['data'] and \
               snapshot_response['data']['takeOnDemandSnapshot'].get('errors'):
                print(json.dumps(snapshot_response['data']['takeOnDemandSnapshot']['errors'], indent=2))
            else:
                print("Snapshot request acknowledged successfully (no immediate errors reported).")
        else:
            print("Skipping on-demand snapshot as session is not active.")
            # If snapshot request failed, we can't monitor, so exit
            raise ConnectionError("Failed to initiate snapshot, cannot monitor status.")


        # 3. Monitor Backup Status
        final_vm_statuses = rsc_session.monitor_backup_status(vm_ids_to_backup, poll_interval, timeout)
        print("\n--- Final Backup Status Report ---")
        for vm_id, status in final_vm_statuses.items():
            vm_name = rsc_session.vm_names.get(vm_id, "Unknown VM Name")
            print(f"VM: {vm_name} ({vm_id}) -> Final Status: {status}")


    except (FileNotFoundError, ValueError, ConnectionError) as e:
        print(f"\nError during RSC session management: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        # 4. Disconnect from RSC - always attempt to disconnect
        print("\n--- Attempting to disconnect from RSC ---")
        rsc_session.disconnect_rsc()
        print(f"Access Token after disconnect: {rsc_session.access_token}") # Should be None
        print(f"Headers after disconnect: {rsc_session.headers}") # Should be empty
