import requests
import json
import time
from datetime import datetime
import argparse
requests.packages.urllib3.disable_warnings()

"""
This script performs two main functions related to MSSQL databases using the Rubrik API on local CDM:
1. Triggers an on-demand backup of an MSSQL database.
2. Searches for MSSQL databases by name.

The script performs the following steps for the backup action:
1. Authenticates with the Rubrik API using service account credentials.
2. Checks if there are any currently running backups for the specified database.
3. If no backups are running, triggers an on-demand backup.
4. Optionally monitors the progress of the backup until it completes.

The script performs the following steps for the search action:
1. Authenticates with the Rubrik API using service account credentials.
2. Searches for MSSQL databases by name using the specified search parameters.
3. Parses and displays specific fields from the search results in a table format, including:
   - id
   - name
   - rootType (from rootProperties)
   - rootName (from rootProperties)
   - rootId (from rootProperties)
   - instanceName
   - instanceId
   - isInAvailabilityGroup
   - lastSnapshotTime
   - effectiveSlaDomainId

Example usage:
python script_name.py --action backup --secret <SECRET> --userid <USERID> --rubrik <RUBRIK> --dbid <DBID> --slaid <SLAID> [--monitor <MONITOR>]
Shell: python3
Launch Time: 070924_111131
Debug: Response Status Code: 202
Debug: Response JSON: {'id': 'MSSQL_DB_BACKUP_04c3ee66-79fd-4253-bfc1-f297d8289a83_7bcf3162-4757-4c33-962c-6f5e5b30e2e5:::0', 'status': 'QUEUED', 'progress': 0.0, 'startTime': '2024-07-09T15:11:33.480Z', 'links': [{'href': 'https://<RUBRIK IP HERE>/api/v1/mssql/request/MSSQL_DB_BACKUP_04c3ee66-79fd-4253-bfc1-f297d8289a83_7bcf3162-4757-4c33-962c-6f5e5b30e2e5:::0', 'rel': 'self'}]}
Backup triggered successfully. Monitoring: 0

python script_name.py --action search --secret <SECRET> --userid <USERID> --rubrik <RUBRIK> --searchname <SEARCHNAME>
Shell: python3
Launch Time: 070924_104908
id                                                   | name                   | rootType               | rootName                       | rootId                                                        | instanceName         | instanceId                                           | isInAvailabilityGroup | lastSnapshotTime         | effectiveSlaDomainId
-----------------------------------------------------+------------------------+------------------------+--------------------------------+---------------------------------------------------------------+----------------------+------------------------------------------------------+-----------------------+--------------------------+-------------------------------------
MssqlDatabase:::295efa93-69b6-413c-97d8-65ced49bf4ab | AdventureWorks2017     | Host                   | rp-sql19sl-01.perf.rubrik.com  | Host:::2b50e122-b03d-4cfa-afe2-3434268958a3                   | MSSQLSERVER          | MssqlInstance:::f560aafe-ed95-4c2c-941a-c20527b3f8fc | False                 | 2024-07-08T23:00:16.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::459ed96c-c70a-4fdd-b7be-dfc0560ccba0 | AdventureWorks2017     | Host                   | rp-sql22sl-01.perf.rubrik.com  | Host:::74bb2659-3335-44a0-a87e-0928b63d8151                   | MSSQLSERVER          | MssqlInstance:::c1126480-684b-4db4-9856-26baa4befa96 | False                 |                          | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::fef964ed-c450-4ede-ade3-24ccfd98e686 | AdventureWorks2017     | Host                   | rp-sql22sl-02.perf.rubrik.com  | Host:::d5e208ba-569c-4116-a34e-3f3187cd4a6e                   | MSSQLSERVER          | MssqlInstance:::aa59725c-0e32-4880-a482-09276de286de | False                 |                          | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::42527305-5e85-4777-8689-955237d3d951 | AdventureWorks2019     | Host                   | rp-sql19s-001.perf.rubrik.com  | Host:::93c5aed0-2140-4c3a-a464-d57b8a8dcfbb                   | MSSQLSERVER          | MssqlInstance:::8c66675e-8c84-45b6-a136-73dce79d2372 | False                 | 2024-07-08T23:00:14.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::6f3b4a70-eef0-413e-8617-0b981f7b7c0c | AdventureWorks2019     | MssqlAvailabilityGroup | rp-sql19ags-g1                 | MssqlAvailabilityGroup:::7c9d4846-e840-49df-b1b1-3efbe87429b4 |                      |                                                      | True                  | 2024-07-09T13:16:41.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::8f521022-04c0-4023-98bb-8ef424b685f0 | AdventureWorks2019     | MssqlAvailabilityGroup | test                           | MssqlAvailabilityGroup:::6cc2c30e-811f-420c-bb0e-01071b8a64d6 |                      |                                                      | True                  | 2024-07-08T23:03:14.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::df5edf49-7cfc-41e7-99f2-ceec53b1b1ba | AdventureWorks2019     | WindowsCluster         | rp-winfcsql                    | WindowsCluster:::ecad58f9-cc49-41fd-8472-8565991366ba         | RP-SQLFC\MSSQLSERVER | MssqlInstance:::6171478b-cc44-423c-b455-f57c9eddf94d | False                 | 2024-05-18T23:00:19.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::e16b892b-2d7d-4fed-a5f4-30c3ed466b1d | AdventureWorks2019     | Host                   | rp-sql19wmv-1.perf.rubrik.com  | Host:::b2051b20-e933-42bb-bc48-e2d4bd055281                   | MSSQLSERVER          | MssqlInstance:::2ac37a47-c5ff-49a3-90d5-934077594527 | False                 | 2021-12-14T18:21:17.000Z | UNPROTECTED
MssqlDatabase:::02d9578a-8bfc-44a2-bc6c-6407ea598e05 | AdventureWorks2019test | Host                   | rp-sql19ags-1b.perf.rubrik.com | Host:::7755eb8d-ca0d-4aa8-988a-3772d83b3563                   | MSSQLSERVER          | MssqlInstance:::80745d85-b991-43b9-9f6f-c405229a26c8 | False                 | 2024-07-08T23:00:11.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::2c75d6de-28c1-4af6-b8fd-33993020c660 | AdventureWorksDW2019   | Host                   | rp-sql19s-001.perf.rubrik.com  | Host:::93c5aed0-2140-4c3a-a464-d57b8a8dcfbb                   | MSSQLSERVER          | MssqlInstance:::8c66675e-8c84-45b6-a136-73dce79d2372 | False                 | 2024-07-08T23:00:14.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::519c6063-7ce7-4c61-8e6d-83e4cd0fca6c | AdventureWorksDW2019   | MssqlAvailabilityGroup | test                           | MssqlAvailabilityGroup:::6cc2c30e-811f-420c-bb0e-01071b8a64d6 |                      |                                                      | True                  | 2024-07-08T23:03:14.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::84647971-d8ff-48f0-8882-e25fbc65ffba | AdventureWorksDW2019   | MssqlAvailabilityGroup | rp-sql19ags-g1                 | MssqlAvailabilityGroup:::7c9d4846-e840-49df-b1b1-3efbe87429b4 |                      |                                                      | True                  | 2024-07-08T23:03:22.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::f2ab7215-aecc-4931-a998-43d052429b1b | AdventureWorksDW2019   | Host                   | rp-sql19wmv-1.perf.rubrik.com  | Host:::b2051b20-e933-42bb-bc48-e2d4bd055281                   | MSSQLSERVER          | MssqlInstance:::2ac37a47-c5ff-49a3-90d5-934077594527 | False                 |                          | UNPROTECTED
MssqlDatabase:::1506e936-3971-47ab-ba88-96446931cbee | AdventureWorksLT2019   | Host                   | rp-sql19wmv-1.perf.rubrik.com  | Host:::b2051b20-e933-42bb-bc48-e2d4bd055281                   | MSSQLSERVER          | MssqlInstance:::2ac37a47-c5ff-49a3-90d5-934077594527 | False                 |                          | UNPROTECTED
MssqlDatabase:::a09ffab7-b9bd-45e2-bb10-3d878d16f6cc | AdventureWorksLT2019   | MssqlAvailabilityGroup | rp-sql19ags-g1                 | MssqlAvailabilityGroup:::7c9d4846-e840-49df-b1b1-3efbe87429b4 |                      |                                                      | True                  | 2024-07-08T23:03:22.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::ae95e9b8-75ee-460b-90b6-fd5deb9f92b4 | AdventureWorksLT2019   | Host                   | rp-sql19s-001.perf.rubrik.com  | Host:::93c5aed0-2140-4c3a-a464-d57b8a8dcfbb                   | MSSQLSERVER          | MssqlInstance:::8c66675e-8c84-45b6-a136-73dce79d2372 | False                 | 2024-07-08T23:00:14.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb
MssqlDatabase:::e6eb0c46-a8ac-4efa-9eba-2d004276a337 | AdventureWorksLT2019   | MssqlAvailabilityGroup | RR-SQL22-AG1                   | MssqlAvailabilityGroup:::9e94cf06-1c97-46e5-a595-dfffcba14aa1 |                      |                                                      | True                  | 2024-07-08T23:00:08.000Z | 506fcfba-10f3-4c2e-8181-0f877ee538cb

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

def print_table(headers, rows):
    # Determine the width of each column
    col_widths = [len(header) for header in headers]
    for row in rows:
        for i, value in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(value) if value is not None else ''))

    # Create a format string for each row
    row_format = ' | '.join([f'{{:<{width}}}' for width in col_widths])

    # Print the header
    print(row_format.format(*headers))
    print('-+-'.join(['-' * width for width in col_widths]))

    # Print the rows
    for row in rows:
        row = [str(value) if value is not None else '' for value in row]
        print(row_format.format(*row))

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Rubrik MSSQL Database Operations.',
        epilog='Example usage:\n'
               'python script_name.py --action backup --secret <SECRET> --userid <USERID> --rubrik <RUBRIK> --dbid <DBID> --slaid <SLAID> [--monitor <MONITOR>]\n'
               'python script_name.py --action search --secret <SECRET> --userid <USERID> --rubrik <RUBRIK> --searchname <SEARCHNAME>',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--action', required=True, choices=['backup', 'search'], help='Action to perform: "backup" or "search"')
    parser.add_argument('--secret', required=True, help='Service Account Secret')
    parser.add_argument('--userid', required=True, help='Service Account User ID')
    parser.add_argument('--rubrik', required=True, help='Hostname or IP address of the Rubrik cluster')
    parser.add_argument('--dbid', help='Database ID that you want to trigger on-demand backup on')
    parser.add_argument('--slaid', help='SLA ID you want to associate with the on-demand backup')
    parser.add_argument('--monitor', type=int, default=0, help='Set to non-zero if you want the script to monitor progress until the backup has finished')
    parser.add_argument('--searchname', help='Database name to search for')

    args = parser.parse_args()

    # RUBRIK VARIABLES
    SECRET = args.secret
    USERID = args.userid
    RUBRIK = args.rubrik
    DBID = args.dbid
    SLAID = args.slaid
    MONITOR = args.monitor
    SEARCHNAME = args.searchname
    LAUNCHTIME = datetime.now().strftime('%m%d%y_%H%M%S')

    # Service Account Configuration Session Setup
    auth_response = requests.post(
        f"https://{RUBRIK}/api/v1/service_account/session",
        headers={'Content-Type': 'application/json'},
        data=json.dumps({"serviceAccountId": USERID, "secret": SECRET}),
        verify=False
    )

    if auth_response.status_code != 200:
        print("Error: Failed to retrieve the authentication token.")
        exit(1)

    TOKEN = auth_response.json().get("token")
    if not TOKEN:
        print("Error: Failed to retrieve the authentication token.")
        exit(1)
    AUTH_HEADER = {"Authorization": f"Bearer {TOKEN}"}

    print("Shell:", "python3")
    print("Launch Time:", LAUNCHTIME)

    if args.action == "backup":
        # Check required variables for backup
        if not all([DBID, SLAID]):
            print("Error: --dbid and --slaid are required for the backup action.")
            exit(1)

        # Prepare the JSON payload for the on-demand backup
        payload = {"slaId": SLAID}

        # Check for any currently running backups
        response = requests.get(
            f"https://{RUBRIK}/api/v1/event/latest?event_status=Running&event_type=Backup&object_ids={DBID}",
            headers={**AUTH_HEADER, "accept": "application/json"},
            verify=False
        )

        # Check if the response contains an event with status "Running"
        if '"eventStatus":"Running"' in response.text:
            print("A Backup Is Already In Progress")
            exit(0)
        else:
            # Trigger on-demand backup
            result = requests.post(
                f"https://{RUBRIK}/api/v1/mssql/db/{DBID}/snapshot",
                headers={**AUTH_HEADER, 'Content-Type': 'application/json'},
                data=json.dumps(payload),
                verify=False
            )

            # Debugging output
            print("Debug: Response Status Code:", result.status_code)
            print("Debug: Response JSON:", result.json())

            # Check if the result contains the status field
            result_json = result.json()
            if result.status_code in [200, 202] and "status" in result_json:
                print("Backup triggered successfully. Monitoring:", MONITOR)
                href = next((link["href"] for link in result_json.get("links", []) if link["rel"] == "self"), None)

                if MONITOR and href:
                    rubrik_status = 0
                    while rubrik_status == 0:
                        # Query the URL for the current status of the on-demand backup
                        status_response = requests.get(
                            href,
                            headers={**AUTH_HEADER, 'Content-Type': 'application/json'},
                            verify=False
                        ).json()

                        # Check if any of the end states are found
                        rubrik_status = len([s for s in ['SUCCEED', 'SUCCESS', 'SUCCESSWITHWARNINGS', 'FAIL', 'CANCEL'] if s in status_response.get("status", "")])

                        print(status_response)
                        time.sleep(60)
                    print("Final Status:", status_response)
            else:
                print("Error: Failed to trigger the on-demand backup.")
                print("Response:", result.text)
                exit(1)

    elif args.action == "search":
        # Check required variables for search
        if not SEARCHNAME:
            print("Error: --searchname is required for the search action.")
            exit(1)

        # Perform the search
        search_url = f"https://{RUBRIK}/api/v1/mssql/db?is_live_mount=false&is_log_shipping_secondary=false&is_relic=false&limit=51&name={SEARCHNAME}&offset=0&primary_cluster_id=local&sort_by=name&sort_order=asc"
        response = requests.get(
            search_url,
            headers={**AUTH_HEADER, 'accept': 'application/json'},
            verify=False
        )

        if response.status_code != 200:
            print("Error: Failed to search the MSSQL database.")
            print("Response:", response.text)
            exit(1)

        # Collect the search results with required fields
        search_results = response.json().get('data', [])
        headers = ['id', 'name', 'rootType', 'rootName', 'rootId', 'instanceName', 'instanceId', 'isInAvailabilityGroup', 'lastSnapshotTime', 'effectiveSlaDomainId']
        rows = []
        for result in search_results:
            rows.append([
                result.get('id'),
                result.get('name'),
                result.get('rootProperties', {}).get('rootType'),
                result.get('rootProperties', {}).get('rootName'),
                result.get('rootProperties', {}).get('rootId'),
                result.get('instanceName'),
                result.get('instanceId'),
                result.get('isInAvailabilityGroup'),
                result.get('lastSnapshotTime'),
                result.get('effectiveSlaDomainId')
            ])
        
        # Print the results in a table format
        print_table(headers, rows)

    else:
        print("Invalid action specified. Use --action 'backup' or 'search'.")
        exit(1)

if __name__ == "__main__":
    main()
