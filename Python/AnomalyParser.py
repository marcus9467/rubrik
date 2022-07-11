#! /usr/bin/env python3


"""
Script to parse high confidence ransomware alerts from Polaris directly. In the event an anomaly is detected a syslog message is sent to the specified syslog server. 

Example:
python3 AnomalyParser.py --keyfile SampleKeyFile.json --syslogServer syslogserver.rubrik.com --port 514 

Starts monitoring the specified Polaris instance for anomaly events and sends them to the syslog server specified over port 514. 

Example:
python3 AnomalyParser.py --keyfile SampleKeyFile.json --syslogServer syslogserver.rubrik.com --port 514 --test

Sends a test message to the specified syslog server over port 514 to validate communication.

CODE HERE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import argparse
import datetime
import json
import logging
import logging.handlers as handlers
import os
import pprint
import sys
import time
import requests
import socket
requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)



def parseArguments():
    parser = argparse.ArgumentParser(description='Parse Radar alerts from Polaris and send to syslog')
    parser.add_argument('--syslogServer', dest='syslogServer', help='specify the syslog server')
    parser.add_argument('-k', '--keyfile', dest='json_keyfile', help="Polaris JSON Keyfile", default=None)
    parser.add_argument('--test', help='Test connection to Syslog Server', action="store_true")
    parser.add_argument('--protocol', help='specify tcp or udp', dest='protocol')
    parser.add_argument('-p', '--port', dest='syslogPort', type=int, help="Defines the port to use with the syslog server", default=None)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    class SfTcpSyslogHandler(handlers.SysLogHandler):
        """
    This class override the python SyslogHandler emit function.
    It is needed to deal with appending of the nul character to the end of the message when using TCP.
    Please see: https://stackoverflow.com/questions/40041697/pythons-sysloghandler-and-tcp/40152493#40152493
    """
    def __init__(self, message_separator_character, address=('localhost', handlers.SYSLOG_UDP_PORT),
                 facility=handlers.SysLogHandler.LOG_USER,
                 socktype=None):
        """
        The user of this class must specify the value for the messages separator.
        :param message_separator_character: The value to separate between messages.
                                            The recommended value is the "nul character": "\000".
        :param address: Same as in the super class.
        :param facility: Same as in the super class.
        :param socktype: Same as in the super class.
        """
        super(SfTcpSyslogHandler, self).__init__(address=address, facility=facility, socktype=socktype)

        self.message_separator_character = message_separator_character

    def emit(self, record):
        """
        SFTCP addition:
        To let the user to choose which message_separator_character to use, we override the emit function.
        ####
        Emit a record.

        The record is formatted, and then sent to the syslog server. If
        exception information is present, it is NOT sent to the server.
        """
        try:
            msg = self.format(record) + self.message_separator_character
            if self.ident:
                msg = self.ident + msg

            # We need to convert record level to lowercase, maybe this will
            # change in the future.
            prio = '<%d>' % self.encodePriority(self.facility,
                                                self.mapPriority(record.levelname))
            prio = prio.encode('utf-8')
            # Message is a string. Convert to bytes as required by RFC 5424
            msg = msg.encode('utf-8')
            msg = prio + msg
            if self.unixsocket:
                try:
                    self.socket.send(msg)
                except OSError:
                    self.socket.close()
                    self._connect_unixsocket(self.address)
                    self.socket.send(msg)
            elif self.socktype == socket.SOCK_DGRAM:
                self.socket.sendto(msg, self.address)
            else:
                self.socket.sendall(msg)
        except Exception:
            self.handleError(record)

    args = parseArguments()
    syslogServer = args.syslogServer
    json_keyfile = args.json_keyfile
    syslogPort = args.syslogPort
    protocol = args.protocol
    Test = args.test
    
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
    #Setup syslog forwarding  
    socket_type = socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM
    logger = logging.getLogger('AnomalyParser')
    logger.setLevel(logging.DEBUG)
    syslog = logging.handlers.SysLogHandler(address=(syslogServer, syslogPort), socktype=socket_type)
    formatter = logging.Formatter('%(asctime)s rbk-log: %(levelname)s[%(name)s] %(message)s', datefmt= '%b %d %H:%M:%S')
    logging.handlers.SysLogHandler.append_nul = False
    syslog.setLevel(logging.INFO)
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)

    def TestConnection():
          logger.info("This is a test for AnomalyParser")
    if Test == True:
          print("Sending Test message to", syslogServer)
          TestConnection()
          sys.exit()
          
    while True:
        #Check for time since token generation
        current_time = datetime.datetime.utcnow()
        TimeDelta = (current_time - token_time).total_seconds()

        #if it has been more than 4 hours, reauth
        if TimeDelta > 14400:
              try:
                print("It has been", TimeDelta, "seconds since last authentication. Reauthenticating now.")
                #Setup token auth
                syslogServer = args.syslogServer
                json_keyfile = args.json_keyfile
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
              except Exception:
                print("An Auth error has occurred. Logging to AnomalyParserException.log")
                file1 = open("AnomalyParserException.log", "w")
                L = ["json_file \n", json_file, "json_key \n", json_key, "session_url \n", session_url, "response_json \n", response_json]
                file1.writelines(L) 
                file1.close()
              token_time = datetime.datetime.utcnow()
        
        #Poll Polaris for events in the last day. 
        end_time = datetime.datetime.utcnow().isoformat
        start_time = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()

        #Setup query and filters  
        variables = {}
        filters = {}
        filters['objectType'] = []
        filters['lastActivityStatus'] = []
        filters['lastActivityType'] = ["Anomaly"]
        filters['severity'] = []
        cluster = {}
        cluster['id'] = []
        filters['cluster'] = cluster
        filters['lastUpdatedGt'] = start_time
        filters['objectName'] = ""
        variables['filters'] = filters
  
        query = """query EventSeriesListQuery($after: String, $filters: ActivitySeriesFilterInput, $first: Int, $sortBy: ActivitySeriesSortByEnum, $sortOrder: SortOrderEnum) {
          activitySeriesConnection(after: $after, first: $first, filters: $filters, sortBy: $sortBy, sortOrder: $sortOrder) {
            edges {
              cursor
              node {
                ...EventSeriesFragment
                cluster {
                  id
                  name
                }
                activityConnection(first: 1) {
                  nodes {
                    id
                    message
                  }
                }
              }
            }
            pageInfo {
              endCursor
              hasNextPage
              hasPreviousPage
            }
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
        }"""
  
        JSON_BODY = {
            "query": query,
            "variables": variables
        }

        #Issue query
        PolarisQuery = requests.post(PolarisUri, json=JSON_BODY, headers=PolarisHeaders)
  
        #Parse events list for just Anamolies 
        Events = PolarisQuery.json()['data']['activitySeriesConnection']['edges']
  
        if bool(Events) == False:
            LastCheckInTime = datetime.datetime.utcnow()
            print("No new events detected. Last Check at", LastCheckInTime)
            time.sleep(300)


        if bool(Events) == True:            
            #Extract information about the anamoly
            try: 
              LastCheckInTime
            except NameError:
              FirstScan = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%f')
              LastCheckInTime = FirstScan

            for x in Events:
              lastUpdated = x['node']['lastUpdated']
              LastRunTime = datetime.datetime.strptime(lastUpdated, '%Y-%m-%dT%H:%M:%S.%fZ') 
              print()
              if LastRunTime > LastCheckInTime:
                print("New events detected. Displaying results below:")
                print("")
                pp.pprint(x['node'])
                logger.error(x['node'])
              else:
                #Just here for debugging purposes
                print("No new events detected. Last Check at", LastCheckInTime)
        
            #Set LastCheckInTime at the end of the loop.
            LastCheckInTime = datetime.datetime.utcnow()
            time.sleep(300)
