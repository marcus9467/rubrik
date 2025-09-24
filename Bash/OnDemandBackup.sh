#!/bin/bash
# Set this to /opt/freeware/bin/bash on AIX hosts as this is the typical location if bash is present.
# Requires 'curl' and 'jq'
# Date: 2024/07/08

# CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# This script will take an on-demand backup of a fileset with a corresponding SLA, and also provides APIs to collect SLA Domain and Fileset information.
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
if [ -z "$SECRET" ] || [ -z "$USERID" ] || [ -z "$RUBRIK" ]; then
    echo "Error: One or more required variables (SECRET, USERID, RUBRIK) are not set."
    exit 1
fi

# Check for jq
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' is not installed. Please install it to use this script."
    exit 1
fi

# Function to authenticate and get the token
authenticate() {
    auth_response=$(curl -k --silent --location "https://$RUBRIK/api/v1/service_account/session" \
      --header 'Content-Type: application/json' \
      --data "{\"serviceAccountId\": \"$USERID\", \"secret\": \"$SECRET\"}")

    TOKEN=$(echo "$auth_response" | jq -r '.token')
    if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
        echo "Error: Failed to retrieve the authentication token."
        exit 1
    fi
    AUTH_HEADER="Authorization: Bearer $TOKEN"
    AUTH_TIME=$(date +%s)
}

# Renew the token if it is older than 3 hours
renew_token_if_needed() {
    current_time=$(date +%s)
    elapsed_time=$((current_time - AUTH_TIME))
    if [ $elapsed_time -ge 10800 ]; then # 10800 seconds = 3 hours
        echo "Token expired, re-authenticating..."
        authenticate
    fi
}

# Function to get all SLA Domains (name and ID) using v2 API
get_all_sla_domains() {
    echo "Getting all SLA Domains (name and ID) using v2 API..."
    renew_token_if_needed
    # Use the v2 SLA Domain API endpoint
    sla_response=$(curl -k -s -X GET "https://$RUBRIK/api/v2/sla_domain" \
      -H "accept: application/json" -H "$AUTH_HEADER")

    # Use jq to filter for only the name and ID from the 'data' array
    if echo "$sla_response" | jq -e '.data' >/dev/null; then
        echo "$sla_response" | jq -r '.data[] | "SLA Name: \(.name), SLA ID: \(.id)"'
    else
        echo "Error: Failed to retrieve SLA domains."
        echo "Response: $sla_response"
    fi
}

# Function to get all Fileset IDs, locations, and inclusion rules
get_all_fileset_details() {
    echo "Getting all Fileset details (name, ID, host, includes)..."
    renew_token_if_needed
    filesets_response=$(curl -k -s -X GET "https://$RUBRIK/api/v1/fileset" \
      -H "accept: application/json" -H "$AUTH_HEADER")

    if echo "$filesets_response" | jq -e '.data' >/dev/null; then
        echo "$filesets_response" | jq -r '.data[] | "Fileset Name: \(.name)\nFileset ID: \(.id)\nHost Name: \(.hostName)\nIncludes: \(.includes | join(","))\n---"'
    else
        echo "Error: Failed to retrieve the list of filesets."
        echo "Response: $filesets_response"
    fi
}

# Function to trigger an on-demand backup
trigger_on_demand_backup() {
    if [ -z "$FILESETID" ] || [ -z "$SLAID" ]; then
        echo "Error: FILESETID and SLAID must be set for this command."
        exit 1
    fi

    # Prepare the JSON payload for the on-demand backup
    JSON="{\"slaId\": \"$SLAID\"}"

    # Check for any currently running backups
    renew_token_if_needed
    response=$(curl -k -s -X GET "https://$RUBRIK/api/v1/event/latest?event_status=Running&event_type=Backup&object_ids=$FILESETID" \
      -H "accept: application/json" -H "$AUTH_HEADER")

    # Check if the response contains an event with status "Running"
    if echo "$response" | grep -q '"eventStatus":"Running"'; then
        echo "A Backup Is Already In Progress"
        exit 0
    else
        # Trigger on-demand backup using curl
        renew_token_if_needed
        RESULT=$(curl -H "$AUTH_HEADER" -X POST -H 'Content-Type: application/json' -d "$JSON" \
          "https://$RUBRIK/api/v1/fileset/$FILESETID/snapshot" -k -s)

        # Check if the result contains the href field
        HREF=$(echo "$RESULT" | jq -r '.links[] | select(.rel=="self") | .href')
        if [ -z "$HREF" ] || [ "$HREF" == "null" ]; then
            echo "Error: Failed to trigger the on-demand backup."
            echo "Response: $RESULT"
            exit 1
        fi
        echo "Backup triggered successfully. Monitoring: $MONITOR"
	echo $RESULT

        if [ $MONITOR -ne 0 ]; then
            # Monitor the backup status
            STATUS=""
            RUBRIKSTATUS=0

            while [ $RUBRIKSTATUS -eq 0 ]; do
                # Query the URL for the current status of the on-demand backup
                renew_token_if_needed
                STATUS=$(curl -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "$HREF" -k -s)

                # Check if any of the end states are found, if so, $RUBRIKSTATUS changes and loop exits
                RUBRIKSTATUS=$(echo "$STATUS" | grep -E 'SUCCEED|SUCCESS|SUCCESSWITHWARNINGS|FAIL|CANCEL' | wc -l)

                echo "$STATUS"
                sleep 60
            done
            echo "Final Status: $STATUS"
        fi
    fi
}

# Initial authentication
authenticate

# Main script logic based on command line arguments
case "$1" in
    get-slas)
        get_all_sla_domains
        ;;
    get-filesets)
        get_all_fileset_details
        ;;
    trigger-backup)
        trigger_on_demand_backup
        ;;
    *)
        echo "Usage: $0 {get-slas | get-filesets | trigger-backup}"
        echo "For trigger-backup, ensure FILESETID and SLAID are set in the script variables."
        exit 1
        ;;
esac
