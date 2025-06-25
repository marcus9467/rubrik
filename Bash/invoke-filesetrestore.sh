#!/bin/bash
# Requires 'curl' and 'jq'
# https://build.rubrik.com
# Written by Steven Tong for community usage
# Date: 12/10/20, updated: 12/20/21 
# Refactored: 06/25/2025 by Marcus Henderson

# This script will restore a fileset to an alternate host (fileset export).
# A list of source and target directories must be provided.
# The script will list the available snapshots for the fileset so the user can select a point in time for restore.
# Once selected the script will kick off a restore job for each source-target directory pair.

# For authentication, use a Service Account ID and Secret.

# You can also run this script by passing in an argument that represents a UTC date:
# ./invoke-filesetrestore.sh --date 2021-12-01
# ./invoke-filesetrestore.sh --date 2021-12-01T06:00:00

# --- Script Configuration Variables ---
# Define the Service Account Secret
SECRET=''
# Define the Service Account User ID
USERID=''
# Hostname or IP address of the Rubrik cluster
RUBRIK=''
# SOURCE - Fileset ID that you want to restore from. This will be overwritten if SOURCE_HOSTNAME_INPUT is used.
FILESETID='Fileset:::ecd6533a-e110-4cfe-9d05-ce3ed5ac8ebb'
# NEW: SOURCE - Hostname that you want to restore from.
# If set, this will be used to look up the source host ID and then the FILESETID.
SOURCE_HOSTNAME_INPUT=''
# NEW: SOURCE - Fileset name on the source host.
# If SOURCE_HOSTNAME_INPUT is set and multiple filesets are found, this name will be used to filter.
SOURCE_FILESET_NAME_INPUT=''
# SOURCE - List of directories to restore, separated by a space in the array.
# All files and sub-directories under these directories will be selected for restore.
SOURCE_DIR=('/epic')
# TARGET - List of directories to restore to, must have same number of directories as $SOURCE_DIR.
# All files and sub-directories from each source directory will be restored to the corresponding target directory.
TARGET_DIR=('/restore')
# TARGET - Host ID that you want to restore to.
# If $TARGET_HOSTID is blank, the restore will be done to the same host it was backed up from
TARGET_HOSTID=''
# TARGET - Hostname that you want to restore to.
# If set, this will be used to look up TARGET_HOSTID. It takes precedence over TARGET_HOSTID if both are set.
TARGET_HOSTNAME_INPUT=''
# Set MONITOR to non-zero if you want the script to monitor progress until the backup has finished
MONITOR=0
# Optional: Path for log file
LOGPATH="" # E.g., "/var/log/rubrik_restore.log"

# --- Global Variables (Internal Use) ---
AUTH_HEADER=""
SNAPSHOT_DATE_ARG=""
TARGET_RESOLVED_HOSTNAME="" # This will store the actual hostname fetched from Rubrik, regardless of input method
SOURCE_RESOLVED_HOSTNAME="" # This will store the actual hostname fetched from Rubrik for the source
SOURCE_RESOLVED_FILESET_NAME="" # This will store the actual fileset name for the source
LAUNCHTIME=$(date +%m%d%y_%H%M%S)
TOKEN="" # Will store the retrieved bearer token
AUTH_TIME=0 # Timestamp when the token was last acquired

# --- Functions ---

# Function to log messages to stdout and optionally to a file
log_message() {
    local type="$1" # e.g., INFO, WARN, ERROR
    local message="$2"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$type] $message"
    if [ -n "$LOGPATH" ]; then
        echo "[$timestamp] [$type] $message" >> "$LOGPATH"
    fi
}

# Function to display usage information
usage() {
    echo "Usage: $0 [--date Watanabe-MM-DD[THH:MM:SS]]"
    echo ""
    echo "This script restores a Rubrik fileset to an alternate host or the same host."
    echo "Configuration variables must be set within the script."
    echo ""
    echo "Options:"
    echo "  --date Watanabe-MM-DD[THH:MM:SS]  Select the snapshot closest to the specified UTC date and time."
    echo "                                If not provided, the script will prompt for manual selection."
    echo "  --help                        Display this help message."
    exit 0
}

# Function to authenticate and get the token
authenticate() {
    log_message INFO "Attempting to authenticate using Service Account ID and Secret..."
    # Added -k back as requested
    local auth_response=$(curl -k --silent --location "https://$RUBRIK/api/v1/service_account/session" \
      --header 'Content-Type: application/json' \
      --data "{\"serviceAccountId\": \"$USERID\", \"secret\": \"$SECRET\"}")
    local CURL_STATUS=$?

    if [ "$CURL_STATUS" -ne 0 ]; then
        log_message ERROR "Curl failed during authentication: $CURL_STATUS. Check Rubrik connection or credentials. API response: $auth_response"
        exit 1
    fi

    # Check for API errors in the response
    if echo "$auth_response" | jq -e '.error // empty' > /dev/null; then
        log_message ERROR "Rubrik API error during authentication: $(echo "$auth_response" | jq -r '.error.message'). ABORTING."
        exit 1
    fi

    TOKEN=$(echo "$auth_response" | jq -r '.token // empty')
    if [ -z "$TOKEN" ]; then
        log_message ERROR "Failed to retrieve the authentication token. Response: $auth_response. ABORTING."
        exit 1
    fi
    AUTH_HEADER="Authorization: Bearer $TOKEN"
    AUTH_TIME=$(date +%s) # Record the time of successful authentication
    log_message INFO "Successfully authenticated and acquired token."
}

# Function to set authentication header (now calls authenticate)
set_auth_header() {
    # This function now simply calls the `authenticate` function.
    # The `AUTH_HEADER` and `TOKEN` global variables will be set by `authenticate`.
    # No explicit token/user_pass check here, as `authenticate` handles it.
    if [ -z "$USERID" ] || [ -z "$SECRET" ]; then
        log_message ERROR "Service Account USERID and SECRET must be configured."
        exit 1
    fi
    authenticate
}

# Function to resolve source host and fileset ID
resolve_source_fileset_id() {
    if [ -n "$SOURCE_HOSTNAME_INPUT" ]; then
        log_message INFO "Attempting to resolve source host ID from SOURCE_HOSTNAME_INPUT: $SOURCE_HOSTNAME_INPUT"
        local HOST_SEARCH_RESPONSE=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/host?name=$SOURCE_HOSTNAME_INPUT")
        local CURL_STATUS=$?

        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when searching for source host '$SOURCE_HOSTNAME_INPUT': $CURL_STATUS. Check Rubrik connection or AUTH_HEADER."
            exit 1
        fi
        if echo "$HOST_SEARCH_RESPONSE" | jq -e '.error // empty' > /dev/null; then
            log_message ERROR "Rubrik API error when searching for source host '$SOURCE_HOSTNAME_INPUT': $(echo "$HOST_SEARCH_RESPONSE" | jq -r '.error.message'). ABORTING."
            exit 1
        fi

        local SOURCE_HOST_ID=$(echo "$HOST_SEARCH_RESPONSE" | jq -r '.data[0].id // empty')
        SOURCE_RESOLVED_HOSTNAME=$(echo "$HOST_SEARCH_RESPONSE" | jq -r '.data[0].hostname // empty')

        if [ -z "$SOURCE_HOST_ID" ] || [ "$SOURCE_HOST_ID" = "null" ]; then
            log_message ERROR "Could not find source host with hostname '$SOURCE_HOSTNAME_INPUT'. API response: $HOST_SEARCH_RESPONSE. ABORTING."
            exit 1
        fi
        log_message INFO "Resolved SOURCE_HOSTNAME_INPUT '$SOURCE_HOSTNAME_INPUT' to host ID: $SOURCE_HOST_ID"

        log_message INFO "Searching for fileset on source host ID: $SOURCE_HOST_ID"
        local FILESET_SEARCH_RESPONSE=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/fileset?host_id=$SOURCE_HOST_ID")
        local CURL_STATUS=$?

        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when searching for filesets on host '$SOURCE_HOST_ID': $CURL_STATUS. ABORTING."
            exit 1
        fi
        if echo "$FILESET_SEARCH_RESPONSE" | jq -e '.error // empty' > /dev/null; then
            log_message ERROR "Rubrik API error when searching for filesets on host '$SOURCE_HOST_ID': $(echo "$FILESET_SEARCH_RESPONSE" | jq -r '.error.message'). ABORTING."
            exit 1
        fi

        local filesets_data=$(echo "$FILESET_SEARCH_RESPONSE" | jq -c '.data[]')
        local found_fileset_ids=()
        local found_fileset_names=()

        if [ -z "$filesets_data" ]; then
            log_message ERROR "No filesets found on source host ID: $SOURCE_HOST_ID. ABORTING."
            exit 1
        fi

        while read -r line; do
            local fileset_id=$(echo "$line" | jq -r '.id')
            local fileset_name=$(echo "$line" | jq -r '.name')
            found_fileset_ids+=("$fileset_id")
            found_fileset_names+=("$fileset_name")
        done <<< "$filesets_data"

        if [ -n "$SOURCE_FILESET_NAME_INPUT" ]; then
            log_message INFO "Filtering filesets by name: $SOURCE_FILESET_NAME_INPUT"
            local matched_fileset_id=""
            local matched_fileset_name=""
            for i in "${!found_fileset_names[@]}"; do
                if [[ "${found_fileset_names[$i]}" == "$SOURCE_FILESET_NAME_INPUT" ]]; then
                    if [ -n "$matched_fileset_id" ]; then
                        log_message ERROR "Multiple filesets found matching name '$SOURCE_FILESET_NAME_INPUT'. Please specify a unique name or use FILESETID directly. ABORTING."
                        exit 1
                    fi
                    matched_fileset_id="${found_fileset_ids[$i]}"
                    matched_fileset_name="${found_fileset_names[$i]}"
                fi
            done

            if [ -z "$matched_fileset_id" ]; then
                log_message ERROR "No fileset found with name '$SOURCE_FILESET_NAME_INPUT' on host '$SOURCE_HOSTNAME_INPUT'. ABORTING."
                exit 1
            fi
            FILESETID="$matched_fileset_id"
            SOURCE_RESOLVED_FILESET_NAME="$matched_fileset_name"
            log_message INFO "Resolved SOURCE_FILESET_NAME_INPUT '$SOURCE_FILESET_NAME_INPUT' to FILESETID: $FILESETID"
        elif [ ${#found_fileset_ids[@]} -eq 1 ]; then
            FILESETID="${found_fileset_ids[0]}"
            SOURCE_RESOLVED_FILESET_NAME="${found_fileset_names[0]}"
            log_message INFO "Found single fileset on host '$SOURCE_HOSTNAME_INPUT'. Using FILESETID: $FILESETID (Name: $SOURCE_RESOLVED_FILESET_NAME)."
        else
            log_message ERROR "Multiple filesets found on source host '$SOURCE_HOSTNAME_INPUT' and SOURCE_FILESET_NAME_INPUT was not specified. Found: ${found_fileset_names[*]} . Please specify SOURCE_FILESET_NAME_INPUT. ABORTING."
            exit 1
        fi
    else
        log_message INFO "Using pre-configured FILESETID: $FILESETID"
        # Attempt to get the name of the pre-configured fileset for display
        local FILESET_INFO_RESPONSE=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/fileset/$FILESETID")
        local FILES_CURL_STATUS=$?
        if [ "$FILES_CURL_STATUS" -eq 0 ] && ! echo "$FILESET_INFO_RESPONSE" | jq -e '.error // empty' > /dev/null; then
            SOURCE_RESOLVED_FILESET_NAME=$(echo "$FILESET_INFO_RESPONSE" | jq -r '.name // empty')
            SOURCE_RESOLVED_HOSTNAME=$(echo "$FILESET_INFO_RESPONSE" | jq -r '.hostName // empty')
            log_message INFO "Pre-configured FILESETID '$FILESETID' resolved to name: $SOURCE_RESOLVED_FILESET_NAME on host: $SOURCE_RESOLVED_HOSTNAME."
        else
             log_message WARN "Could not retrieve details for pre-configured FILESETID '$FILESETID'. Display might be incomplete. Error: $FILES_CURL_STATUS, API Response: $FILESET_INFO_RESPONSE"
        fi
    fi
}


# Function to validate Rubrik cluster and target host (if specified)
validate_environment() {
    if [ -z "$RUBRIK" ]; then
        log_message ERROR "RUBRIK hostname or IP address is not configured."
        exit 1
    fi

    if [ ${#SOURCE_DIR[@]} -ne ${#TARGET_DIR[@]} ]; then
        log_message ERROR "Number of SOURCE_DIR entries (${#SOURCE_DIR[@]}) does not equal TARGET_DIR entries (${#TARGET_DIR[@]}). ABORTING."
        exit 1
    fi

    # Determine TARGET_HOSTID based on TARGET_HOSTNAME_INPUT or existing TARGET_HOSTID
    if [ -n "$TARGET_HOSTNAME_INPUT" ]; then
        log_message INFO "Attempting to resolve TARGET_HOSTID from TARGET_HOSTNAME_INPUT: $TARGET_HOSTNAME_INPUT"
        # Use the API endpoint from the provided image
        local HOST_SEARCH_RESPONSE=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/host?name=$TARGET_HOSTNAME_INPUT")
        local CURL_STATUS=$?

        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when searching for host '$TARGET_HOSTNAME_INPUT': $CURL_STATUS. Check Rubrik connection or AUTH_HEADER."
            exit 1
        fi
        if echo "$HOST_SEARCH_RESPONSE" | jq -e '.error // empty' > /dev/null; then
            log_message ERROR "Rubrik API error when searching for host '$TARGET_HOSTNAME_INPUT': $(echo "$HOST_SEARCH_RESPONSE" | jq -r '.error.message'). ABORTING."
            exit 1
        fi

        # Extract the ID from the 'data' array. Assumes the first match is sufficient.
        TARGET_HOSTID=$(echo "$HOST_SEARCH_RESPONSE" | jq -r '.data[0].id // empty')
        TARGET_RESOLVED_HOSTNAME=$(echo "$HOST_SEARCH_RESPONSE" | jq -r '.data[0].hostname // empty')

        if [ -z "$TARGET_HOSTID" ] || [ "$TARGET_HOSTID" = "null" ]; then
            log_message ERROR "Could not find a host with hostname '$TARGET_HOSTNAME_INPUT'. API response: $HOST_SEARCH_RESPONSE. ABORTING."
            exit 1
        fi
        log_message INFO "Resolved TARGET_HOSTNAME_INPUT '$TARGET_HOSTNAME_INPUT' to TARGET_HOSTID: $TARGET_HOSTID"
        log_message INFO "Resolved hostname for TARGET_HOSTID: $TARGET_RESOLVED_HOSTNAME"
    elif [ -n "$TARGET_HOSTID" ]; then
        log_message INFO "Using pre-configured TARGET_HOSTID: $TARGET_HOSTID"
        # Validate the pre-configured TARGET_HOSTID and get its name
        local HOSTINFO=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/host/$TARGET_HOSTID")
        local CURL_STATUS=$?
        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when checking pre-configured target host ID: $CURL_STATUS. Check Rubrik connection or AUTH_HEADER."
            exit 1
        fi

        TARGET_RESOLVED_HOSTNAME=$(echo "$HOSTINFO" | jq -r '.name // empty')
        if [ -z "$TARGET_RESOLVED_HOSTNAME" ] || [ "$TARGET_RESOLVED_HOSTNAME" = "null" ]; then
            log_message ERROR "Pre-configured TARGET_HOSTID '$TARGET_HOSTID' not found or invalid. API response: $HOSTINFO. ABORTING."
            exit 1
        fi
        log_message INFO "Pre-configured TARGET_HOSTID '$TARGET_HOSTID' resolved to hostname: $TARGET_RESOLVED_HOSTNAME"
    else
        log_message INFO "Neither TARGET_HOSTNAME_INPUT nor TARGET_HOSTID configured. Restoring to the original host."
    fi
}

# Function to get snapshots for the fileset
get_fileset_snapshots() {
    log_message INFO "Retrieving snapshot information for fileset ID: $FILESETID"
    # Added -k back as requested
    FILESETINFO=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "https://$RUBRIK/api/v1/fileset/$FILESETID")
    CURL_STATUS=$?
    if [ "$CURL_STATUS" -ne 0 ]; then
        log_message ERROR "Curl failed when getting fileset info: $CURL_STATUS. Check Rubrik connection or FILESETID."
        exit 1
    fi

    # Check for API errors or empty response
    if echo "$FILESETINFO" | jq -e '.error // empty' > /dev/null; then
        log_message ERROR "Rubrik API error when getting fileset info: $(echo "$FILESETINFO" | jq -r '.error.message'). ABORTING."
        exit 1
    fi

    # Initiate arrays
    SNAPSHOTIDS=()
    SNAPSHOTDATES=()

    # Populate arrays with snapshot IDs and dates
    SNAPSHOTINFO=$(echo "$FILESETINFO" | jq -c '.snapshots[]')
    if [ -z "$SNAPSHOTINFO" ]; then
        log_message ERROR "No snapshots found for fileset ID: $FILESETID. ABORTING."
        exit 1
    fi

    while read -r line; do
        SNAPSHOTIDS+=($(echo "$line" | jq -r '.id'))
        SNAPSHOTDATES+=($(echo "$line" | jq -r '.date'))
    done <<< "$SNAPSHOTINFO"

    if [ ${#SNAPSHOTIDS[@]} -eq 0 ]; then
        log_message ERROR "No snapshots found for fileset ID: $FILESETID after parsing. ABORTING."
        exit 1
    fi
}

# Function to select the snapshot based on argument or user input
select_snapshot() {
    local USERSNAPSHOT_INDEX=-1

    if [ -n "$SNAPSHOT_DATE_ARG" ]; then
        log_message INFO "Searching for snapshot closest to date: $SNAPSHOT_DATE_ARG"
        local INPUT_EPOCH=$(date -d "$SNAPSHOT_DATE_ARG" +"%s" 2>/dev/null)
        if [ $? -ne 0 ]; then
            log_message ERROR "Invalid date format provided: $SNAPSHOT_DATE_ARG. Use Watanabe-MM-DD or Watanabe-MM-DDTHH:MM:SS."
            exit 1
        fi

        local EPOCH_DIFF=9999999999 # Large initial difference
        local j=0
        while [ $j -lt ${#SNAPSHOTDATES[@]} ]; do
            local J_EPOCH=$(date -d "${SNAPSHOTDATES[$j]}" +"%s" 2>/dev/null)
            if [ $? -ne 0 ]; then
                log_message WARN "Could not parse snapshot date: ${SNAPSHOTDATES[$j]}. Skipping."
                ((j++))
                continue
            fi

            local CURRENT_DIFF=$((INPUT_EPOCH - J_EPOCH))
            local ABS_CURRENT_DIFF=${CURRENT_DIFF#-} # Get absolute value

            if [ "$ABS_CURRENT_DIFF" -lt "$EPOCH_DIFF" ]; then
                USERSNAPSHOT_INDEX=$j
                EPOCH_DIFF="$ABS_CURRENT_DIFF"
            fi
            ((j++))
        done

        if [ "$USERSNAPSHOT_INDEX" -eq -1 ]; then
            log_message ERROR "Could not find a suitable snapshot for the given date: $SNAPSHOT_DATE_ARG. ABORTING."
            exit 1
        fi
        log_message INFO "Selected snapshot index $USERSNAPSHOT_INDEX: ${SNAPSHOTDATES[$USERSNAPSHOT_INDEX]} (closest to $SNAPSHOT_DATE_ARG)."
    else
        echo -e "\n## Snapshot Dates (UTC)"
        echo "-- --------------------"
        local i=0
        while [ $i -lt ${#SNAPSHOTDATES[@]} ]; do
            printf '%-3s %-20s\n' "$i" "${SNAPSHOTDATES[$i]}"
            ((i++))
        done

        while true; do
            read -rp "Enter snapshot # to recover from: " USERSNAPSHOT_INPUT
            if [[ "$USERSNAPSHOT_INPUT" =~ ^[0-9]+$ ]] && \
               (( USERSNAPSHOT_INPUT >= 0 )) && \
               (( USERSNAPSHOT_INPUT < ${#SNAPSHOTDATES[@]} )); then
                USERSNAPSHOT_INDEX=$USERSNAPSHOT_INPUT
                break
            else
                echo "Selection outside acceptable range or not a number, try again."
            fi
        done
        log_message INFO "User selected snapshot index $USERSNAPSHOT_INDEX: ${SNAPSHOTDATES[$USERSNAPSHOT_INDEX]}."
    fi

    SNAPSHOTIDTORESTORE=${SNAPSHOTIDS[$USERSNAPSHOT_INDEX]}
    SNAPSHOTDATETORESTORE=${SNAPSHOTDATES[$USERSNAPSHOT_INDEX]}
}

# Function to build and initiate the restore request
initiate_restore() {
    log_message INFO "Building restore JSON payload."

    local restore_configs=()
    local SOURCE_DIR_COUNT=${#SOURCE_DIR[@]}
    local i=0

    while [ $i -lt $SOURCE_DIR_COUNT ]; do
        local current_source_dir="${SOURCE_DIR[$i]}"
        local current_target_dir="${TARGET_DIR[$i]}"

        log_message INFO "Browsing source directory: $current_source_dir for snapshot $SNAPSHOTIDTORESTORE"
        # Added -k back as requested
        local DIRINFO=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' \
            "https://$RUBRIK/api/internal/browse?limit=500&snapshot_id=$SNAPSHOTIDTORESTORE&path=$current_source_dir")
        local CURL_STATUS=$?

        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when browsing directory '$current_source_dir': $CURL_STATUS. ABORTING."
            exit 1
        fi
        if echo "$DIRINFO" | jq -e '.error // empty' > /dev/null; then
            log_message ERROR "Rubrik API error when browsing directory '$current_source_dir': $(echo "$DIRINFO" | jq -r '.error.message'). ABORTING."
            exit 1
        fi

        # Extract filenames and construct restore config items using jq
        local path_list=$(echo "$DIRINFO" | jq -c '.data[] | { path: (.path + "/" + .filename), restorePath: "'"$current_target_dir"'" }')

        if [ -z "$path_list" ]; then
            log_message WARN "No files or sub-directories found under '$current_source_dir' in snapshot '$SNAPSHOTIDTORESTORE'. Skipping this source directory."
        else
            while IFS= read -r item; do
                restore_configs+=("$item")
            done <<< "$path_list"
        fi
        ((i++))
    done

    if [ ${#restore_configs[@]} -eq 0 ]; then
        log_message ERROR "No valid paths found to restore. ABORTING."
        exit 1
    fi

    # Construct the final JSON payload using jq's array and object creation capabilities
    # Join array elements with comma
    local RESTORE_JSON_ARRAY=$(IFS=,; echo "${restore_configs[*]}")
    local RESTORE_JSON=$(jq -n --argjson config_array "[$RESTORE_JSON_ARRAY]" '{ restoreConfig: $config_array, ignoreErrors: false }')

    log_message INFO "Restore request payload:"
    echo "$RESTORE_JSON" | jq .

    log_message INFO "Invoking restore task for fileset ID: $FILESETID, snapshot ID: $SNAPSHOTIDTORESTORE"
    # Added -k back as requested
    RESULT=$(curl -k -s -X POST -H "$AUTH_HEADER" -H 'Content-Type: application/json' \
        -d "$RESTORE_JSON" "https://$RUBRIK/api/internal/fileset/snapshot/$SNAPSHOTIDTORESTORE/restore_files")
    local CURL_STATUS=$?

    if [ "$CURL_STATUS" -ne 0 ]; then
        log_message ERROR "Curl failed when initiating restore: $CURL_STATUS. API response: $RESULT. ABORTING."
        exit 1
    fi
    if echo "$RESULT" | jq -e '.error // empty' > /dev/null; then
        log_message ERROR "Rubrik API error when initiating restore: $(echo "$RESULT" | jq -r '.error.message'). ABORTING."
        exit 1
    fi

    log_message INFO "Restore job initiated. API response: $RESULT"

    # Extract job URL for monitoring
    local HREF=$(echo "$RESULT" | jq -r '.links[] | select(.rel == "self") | .href // empty')
    # The API for monitoring should be v1, not internal
    HREF=$(echo "$HREF" | sed 's/internal/v1/g')

    if [ -z "$HREF" ] || [ "$HREF" = "null" ]; then
        log_message ERROR "Could not extract job status URL from API response. Cannot monitor job. ABORTING."
        exit 1
    fi

    echo "$HREF"
}

# Function to monitor the restore job status
monitor_job() {
    local job_url="$1"
    log_message INFO "Monitoring restore job status at: $job_url"

    local STATUS_MESSAGE=""
    local RUBRIKSTATUS_CODE=0 # 0 for in-progress, 1 for finished

    while [ "$RUBRIKSTATUS_CODE" -eq 0 ]; do
        # Added -k back as requested
        local STATUS_RESPONSE=$(curl -k -s -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "$job_url")
        local CURL_STATUS=$?

        if [ "$CURL_STATUS" -ne 0 ]; then
            log_message ERROR "Curl failed when monitoring job status: $CURL_STATUS. ABORTING MONITORING."
            return 1 # Exit monitoring, not the whole script
        fi
        if echo "$STATUS_RESPONSE" | jq -e '.error // empty' > /dev/null; then
            log_message ERROR "Rubrik API error when monitoring job: $(echo "$STATUS_RESPONSE" | jq -r '.error.message'). ABORTING MONITORING."
            return 1 # Exit monitoring
        fi

        STATUS_MESSAGE=$(echo "$STATUS_RESPONSE" | jq -r '.status // "UNKNOWN"')
        log_message INFO "Current job status: $STATUS_MESSAGE"

        case "$STATUS_MESSAGE" in
            SUCCEED|SUCCESS|SUCCESSWITHWARNINGS)
                RUBRIKSTATUS_CODE=1
                log_message INFO "Restore job completed successfully with status: $STATUS_MESSAGE."
                ;;
            FAIL|CANCELLED|CANCELED)
                RUBRIKSTATUS_CODE=1
                log_message ERROR "Restore job failed or was cancelled with status: $STATUS_MESSAGE."
                ;;
            *)
                log_message INFO "Job still in progress ($STATUS_MESSAGE). Waiting 60 seconds..."
                sleep 60
                ;;
        esac
    done
    echo "$STATUS_RESPONSE" | jq . # Print final status
    return 0
}

# --- Main Script Execution ---

main() {
    # Set strict mode for robustness
    set -euo pipefail

    # Parse command-line arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --date)
                SNAPSHOT_DATE_ARG="$2"
                shift 2
                ;;
            --help)
                usage
                ;;
            *)
                log_message ERROR "Unknown argument: $1"
                usage
                ;;
        esac
    done

    log_message INFO "Script started at $LAUNCHTIME."
    if [ -n "$LOGPATH" ]; then
        log_message INFO "Logging to: $LOGPATH"
        # Ensure log file exists and is writable
        mkdir -p "$(dirname "$LOGPATH")" || log_message WARN "Could not create log directory."
        touch "$LOGPATH" || log_message WARN "Could not create log file: $LOGPATH. Logging only to stdout."
    fi

    # Check for 'jq' utility
    command -v jq >/dev/null 2>&1 || { log_message ERROR "Script requires the utility 'jq'. Aborting."; exit 1; }

    # Authenticate with Rubrik
    set_auth_header

    # Validate environment and configuration
    validate_environment

    # Resolve the source fileset ID (after authentication and general validation)
    resolve_source_fileset_id

    # Get snapshot information
    get_fileset_snapshots

    # Select snapshot
    select_snapshot

    # Display confirmation details
    echo -e "\n--- Restore Details ---"
    echo "Restoring from fileset: ${SOURCE_RESOLVED_FILESET_NAME:-$FILESETID} (ID: $FILESETID)"
    echo "On source host: ${SOURCE_RESOLVED_HOSTNAME:-Unknown}"
    echo "Snapshot: ${SNAPSHOTDATETORESTORE} UTC (ID: $SNAPSHOTIDTORESTORE)"
    if [ -n "$TARGET_HOSTID" ]; then
        echo "Target host: ${TARGET_RESOLVED_HOSTNAME:-$TARGET_HOSTID} (ID: $TARGET_HOSTID)"
    else
        echo "Restoring to original host."
    fi
    echo "Source Directories to Target Directories:"
    echo "---------------------------------------"
    for i in "${!SOURCE_DIR[@]}"; do
        echo "${SOURCE_DIR[$i]} to ${TARGET_DIR[$i]}"
    done
    echo "---------------------------------------"

    # User confirmation
    read -rp "Type 'y' to proceed with the restore: " USERPROCEED
    if [[ "$USERPROCEED" != 'y' ]]; then
        log_message INFO "User aborted script."
        exit 2
    fi

    # Initiate restore and get job URL
    JOB_URL=$(initiate_restore)

    # Monitor job if configured
    if [ "$MONITOR" -ne 0 ]; then
        monitor_job "$JOB_URL"
    else
        log_message INFO "Monitoring is disabled. Job initiated. Check Rubrik UI for status."
        log_message INFO "Job URL: $JOB_URL"
    fi

    log_message INFO "Script finished successfully."
}

# Call the main function
main "$@"
