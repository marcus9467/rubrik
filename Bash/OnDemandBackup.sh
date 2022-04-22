#!/bin/sh

#The goals of this script are to:
#Provide the ability to take on-demand snapshots from the client side. 
#Provide the ability to update fileset templates from the client side.
#Provide an exit code to indicate the success or failure of the on-demand backup. 

#Syntax
#./OnDemandBackup.sh backupinclusion.txt

#Provides Rubrik with the updated inclusions contained in the file backupinclusion.txt and then issues a backup request for that fileset
#For use with month end backups. 

#./OnDemandBackup.sh

#Issues an ondemand backup using the pre-defined fileset. For use with daily backups.

#CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

FILESETID='FilesetId Here'
FILESETTEMPLATEID='FilesetTemplate Here'
AUTH_HEADER='Authorization:Bearer <token here>'
FILESETINCLUSION=$(grep backup $1 | sed 'N;s/\n/,/')
FILESETINCLUSIONCOUNT=$(echo $FILESETINCLUSION | wc -l)
RUBRIK='Rubrik IP Here'
SLAId='SLA ID Here'

if [ $FILESETINCLUSIONCOUNT -gt 0 ]; then
    curl -k -X PATCH "https://$RUBRIK/api/internal/fileset_template/bulk" -H "accept: application/json" -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "[{\"allowBackupNetworkMounts\":false,\"allowBackupHiddenFoldersInNetworkMounts\":false,\"includes\":[\"$FILESETINCLUSION\"],\"operatingSystemType\":\"UnixLike\",\"id\":\"$FILESETTEMPLATEID\"}]"
fi

RESULT=$(curl -k -X POST "https://$RUBRIK/api/v1/fileset/$FILESETID/snapshot" -H "accept: application/json" -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "{ \"slaId\": \"$SLAId\"}")
# Reset $STATUS in case it contains other values
  STATUS=""

  # Pull out the URL that we use to query status
  HREF=$(echo $RESULT | sed -e 's/.*href\"\:\"\(.*\)\"\,.*/\1/')
  RUBRIKSTATUS=0

#Monitor backup and poll the status every 60 seconds 
  while [ $RUBRIKSTATUS -eq 0 ]
  do
    # Query the URL for the current status of the on demand backup
    STATUS=$(curl -H "$AUTH_HEADER" -X GET -H 'Content-Type: application/json' "$HREF" -k -1 -s)

    # Check if any of the end states are found, if so, $RUBRIKSTATUS changes and loop exits
    RUBRIKSTATUS=$(echo $STATUS | grep 'SUCCEED\|SUCCESS\|SUCCESSWITHWARNINGS\|FAIL\|CANCEL' -c)
    RUBRIKSUCCESS=$(echo $STATUS | grep 'SUCCEED\|SUCCESS\|SUCCESSWITHWARNINGS' -c)
    RUBRIKFAILURE=$(echo $STATUS | grep 'FAIL\|CANCEL' -c)

    echo $STATUS
    sleep 60
  done

#Assign 0 error code in the event of Backup Success 
if [ $RUBRIKSUCCESS -eq 1 ]; then
    echo $STATUS
    echo "The Backup has succeeded!"
    exit 0
fi

#Assign 1 exit code in the event of Backup Failure
if [ $RUBRIKFAILURE -eq 1 ]; then
    echo $STATUS
    echo "The Backup has failed!"
    exit 1
fi