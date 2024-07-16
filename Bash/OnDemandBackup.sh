#!/bin/bash
# Set this to /opt/freeware/bin/bash on AIX hosts as this is the typical location if bash is present. 
# Requires 'curl'
# Date: 2024/07/08

# CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# This script will take an on-demand backup of a fileset with a corresponding SLA.
# Create a custom role to limit the privileges of the script user.

### RUBRIK VARIABLES - BEGIN ###

# Define the Service Account Secret
SECRET=''
# Define the Service Account User ID
USERID=''
# Hostname or IP address of the Rubrik cluster
RUBRIK=''
# Fileset ID that you want to trigger on demand backup on
FILESETID=''
# SLA ID you want to associate with the on-demand backup
SLAID=''
# Set MONITOR to non-zero if you want the script to monitor progress until the backup has finished
MONITOR=0
# Script execution time
LAUNCHTIME=$(date +%m%d%y_%H%M%S)

### RUBRIK VARIABLES - END ###

# Check for required variables
if [ -z "$SECRET" ] || [ -z "$USERID" ] || [ -z "$RUBRIK" ] || [ -z "$FILESETID" ] || [ -z "$SLAID" ]; then
    echo "Error: One or more required variables are not set."
    exit 1
fi

# Service Account Configuration Session Setup
auth_response=$(curl -k --location "https://$RUBRIK/api/v1/service_account/session" \
  --header 'Content-Type: application/json' \
  --data "{\"serviceAccountId\": \"$USERID\", \"secret\": \"$SECRET\"}")

# Extract the token from the response
TOKEN=$(echo "$auth_response" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$TOKEN" ]; then
    echo "Error: Failed to retrieve the authentication token."
    exit 1
fi
AUTH_HEADER="Authorization: Bearer $TOKEN"

echo $SHELL
echo $LAUNCHTIME

# Prepare the JSON payload for the on-demand backup
JSON="{\"slaId\": \"$SLAID\"}"

# Check for any currently running backups
response=$(curl -k -X GET "https://$RUBRIK/api/v1/event/latest?event_status=Running&event_type=Backup&object_ids=$FILESETID" \
  -H "accept: application/json" -H "$AUTH_HEADER")

# Check if the response contains an event with status "Running"
if echo "$response" | grep -q '"eventStatus":"Running"'; then
    echo "A Backup Is Already In Progress"
    exit 0
else
    # Trigger on-demand backup using curl
    RESULT=$(curl -H "$AUTH_HEADER" -X POST -H 'Content-Type: application/json' -d "$JSON" \
      "https://$RUBRIK/api/v1/fileset/$FILESETID/snapshot" -k -1 -s)

    # Check if the result contains the href field
    HREF=$(echo $RESULT | sed -n 's/.*"href":"\([^"]*\)".*/\1/p')
    if [ -z "$HREF" ]; then
        echo "Error: Failed to trigger the on-demand backup."
        echo "Response: $RESULT"
        exit 1
    fi
    echo "Backup triggered successfully. Monitoring: $MONITOR"

    if [ $MONITOR -ne 0 ]; then
        # Monitor the backup status
        STATUS=""
        RUBRIKSTATUS=0

        while [ $RUBRIKSTATUS -eq 0 ]; do
            # Query the URL for the current status of the on-demand backup
            STATUS=$(curl -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "$HREF" -k -1 -s)

            # Check if any of the end states are found, if so, $RUBRIKSTATUS changes and loop exits
            RUBRIKSTATUS=$(echo $STATUS | grep -E 'SUCCEED|SUCCESS|SUCCESSWITHWARNINGS|FAIL|CANCEL' | wc -l)

            echo $STATUS
            sleep 60
        done
        echo "Final Status: $STATUS"
    fi
fi
