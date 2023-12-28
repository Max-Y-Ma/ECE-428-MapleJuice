import socket
import sdfs
import time
import json
import random
import datetime
import logging
import argparse
import threading

# Define a list of host names that represent nodes in the distributed system.
# These host names are associated with specific machines in the network.
# The 'Introducer' variable points to a specific host in the system that may serve as an introducer node.
HOST_NAME_LIST = [
    "fa23-cs425-2301.cs.illinois.edu",
    "fa23-cs425-2302.cs.illinois.edu",
    "fa23-cs425-2303.cs.illinois.edu",
    "fa23-cs425-2304.cs.illinois.edu",
    "fa23-cs425-2305.cs.illinois.edu",
    "fa23-cs425-2306.cs.illinois.edu",
    "fa23-cs425-2307.cs.illinois.edu",
    "fa23-cs425-2308.cs.illinois.edu",
    "fa23-cs425-2309.cs.illinois.edu",
    "fa23-cs425-2310.cs.illinois.edu",
]

# 'Introducor' specifies the introducer node's hostname, which plays a crucial role in system coordination.
Introducor = "fa23-cs425-2310.cs.illinois.edu"

# 'DEFAULT_PORT_NUM' defines the default port number used for communication within the system.
DEFAULT_PORT_NUM = 12360

# Configure logging for the script. This sets up a logging system that records debug information
# to the 'output.log' file, including timestamps and log levels.
'''
logging.basicConfig(level=logging.DEBUG,
                    filename='output.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(levelname)s - %(message)s')
gossip_log = logging.getLogger(__name__)
'''

# The `Server` class represents a node in a distributed system.
class Server:
    def __init__(self, args, sdfs_server, maplejuice_server):
        # Initialize the server with various attributes.
        self.sdfs = sdfs_server
        self.maplejuice_server = maplejuice_server
        self.ip = socket.gethostname()
        self.port = DEFAULT_PORT_NUM
        self.heartbeat = 0
        self.timejoin = int(time.time())
        self.id = f"{self.ip}:{self.port}:{self.timejoin}"
        self.addr = (self.ip, self.port)
        self.MembershipList = {
                f"{ip}:{port}:{self.timejoin}": {
                "id": f"{ip}:{port}:{self.timejoin}",
                "addr": (ip, port),
                "heartbeat": 0,
                "incarnation":0,
                "status": "Alive",
                "time": time.time(),
            }
            for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in [self.ip, Introducor]]
        }
        # List to track failed members.
        self.failMemberList = {}
        # Thresholds for various time-based criteria.
        self.failure_time_threshold = 7
        self.cleanup_time_threshold = 7
        self.suspect_time_threshold = 7
        self.protocol_period = args.protocol_period
        # Number of times to send messages.
        self.n_send = 3
        self.status = "Alive"
        # Probability of dropping a message.
        self.drop_rate = args.drop_rate
        # Incarnation number for handling suspicion.
        self.incarnation = 0
        # Thread-safe lock for synchronization.
        self.rlock = threading.RLock()
        # Flag to enable or disable message sending for leaving group and enable and disable suspicion mechanisism
        self.enable_sending = True
        self.gossipS = False
    
    """
    Utility functions for the Gossip server class
    """

    def printID(self):
        # Method to print the unique ID of the server.
        with self.rlock:
            print(self.id)

    def check_leader(self, leader_hostname):
        for member_hostname in self.MembershipList.keys():
            if leader_hostname in member_hostname:
                return True

        return False

    def json(self):
        # Method to generate a JSON representation of the server's membership information.
        with self.rlock:
            if self.gossipS:
            # If using GossipS protocol, include additional information like status and incarnation.
                return {
                    m['id']:{
                        'id': m['id'],
                        'addr': m['addr'],
                        'heartbeat': m['heartbeat'] ,
                        'status': m['status'],
                        'incarnation': m['incarnation']
                    }
                    for m in self.MembershipList.values()
                }
            else:
            # If not using GossipS protocol, include basic information like ID, address, and heartbeat.
                return {
                    m['id']:{
                        'id': m['id'],
                        'addr': m['addr'],
                        'heartbeat': m['heartbeat'] ,
                    }
                    for m in self.MembershipList.values()
                }

    def printMembershipList(self):
        # Method to print the membership list to the log file and return it as a string.
        with self.rlock:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_message = f"{timestamp} ==============================================================\n"
            log_message += f" {str(self.failMemberList)}\n"
            for member_id, member_info in self.MembershipList.items():
                log_message += f"ID: {member_info['id']}, Heartbeat: {member_info['heartbeat']}, Status: {member_info['status']}, Incarnation:{member_info['incarnation']}, Time: {member_info['time']}\n"
            with open('output.log', 'a') as log_file:
                log_file.write(log_message)
            return(log_message)
        
    def getMembershipList(self):
        return self.MembershipList
        
    def join(self):
        self.enable_sending = True
        print("Starting to send messages.")
        self.MembershipList = {
            f"{ip}:{port}:{self.timejoin}": {
            "id": f"{ip}:{port}:{self.timejoin}",
            "addr": (ip, port),
            "heartbeat": 0,
            "status": "Alive",
            "incarnation": 0,
            "time": time.time(),
            }
            for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in [self.ip, Introducor]]
        }  

    def leave(self):
        self.enable_sending = False
        print("Leaving the group.")

    def list_mem(self):
        print(self.printMembershipList())

    def list_self(self):
        self.printID()

    """
    Helper functions for the Gossip server class
    """

    def updateMembershipList(self, membershipList):
        # Method to update the membership list of the server with received information.
        with self.rlock:
            # Iterate through the received membership list.
            for member_id, member_info in membershipList.items():
                if member_info['heartbeat'] == 0:
                    # Skip members with heartbeat equal to 0, to clear out the initial introducer with 0 heartbeat.
                    continue
                if member_id in self.failMemberList:
                    # Skip members that are already in the failed members list.
                    continue

                member_info.setdefault("status", "Alive")
                member_info.setdefault("incarnation", 0)
                #if the server receive the suspect message about itself, overwrite the message with great incarnation number:
                if member_id == self.id:
                    if member_info["status"] == "Suspect":
                         if self.incarnation < member_info["incarnation"]:
                            self.incarnation = member_info["incarnation"] + 1
                # Check if the member is already in the MembershipList
                if member_id in self.MembershipList:
                    current_heartbeat = self.MembershipList[member_id]["heartbeat"]
                    # Incarnation overwrite heartbeat
                    if member_info["incarnation"] > self.MembershipList[member_id]["incarnation"]:
                        self.MembershipList[member_id] = member_info
                        self.MembershipList[member_id]["time"] = time.time()
                        #if suspect print out
                        if self.MembershipList[member_id]["status"] == "Suspect":
                            # gossip_log.info("[SUS]    - {}".format(member_id))
                            log_message = f"Incaroverwrite: ID: {member_id}, Status: {self.MembershipList[member_id]['status']}, Time: {self.MembershipList[member_id]['time']}\n"
                            print(log_message)
                    # Update only if the received heartbeat is greater and both at the same incarnation
                    elif member_info["heartbeat"] > current_heartbeat and member_info["incarnation"] == self.MembershipList[member_id]["incarnation"]:
                        self.MembershipList[member_id] = member_info
                        self.MembershipList[member_id]["time"] = time.time()
                else:
                    # If the member is not in the MembershipList, add it
                    self.MembershipList[member_id] = member_info
                    self.MembershipList[member_id]["time"] = time.time()
                    # If suspect print out 
                    if self.MembershipList[member_id]["status"] == "Suspect":
                        # gossip_log.info("[SUS]    - {}".format(member_id))
                        log_message = f"Newmem        : ID: {member_id}, Status: {self.MembershipList[member_id]['status']}, Time: {self.MembershipList[member_id]['time']}\n"
                        print(log_message)
                    # gossip_log.info("[JOIN]   - {}".format(member_id))

    def detectSuspectAndFailMember(self):
        # Method to detect and handle suspected and failed members in the membership list for the gossip S protocol.
        with self.rlock:
            now = int(time.time())
            # Calculate the threshold time
            failure_threshold_time = now - self.failure_time_threshold
            suspect_threshold_time = now - self.suspect_time_threshold
            # Collect members to remove
            suspect_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < failure_threshold_time and member_info["status"] != "Suspect"]
            for member_id in suspect_members_detected:
                self.MembershipList[member_id]["status"] = "Suspect"
                self.MembershipList[member_id]["incarnation"] += 1
                self.MembershipList[member_id]["time"] = now
                # gossip_log.info("[SUS]    - {}".format(member_id))
                log_message = f"Detected      : ID: {member_id}, Status: {self.MembershipList[member_id]['status']}, Time: {self.MembershipList[member_id]['time']}\n"
                print(log_message)
            fail_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < suspect_threshold_time and member_id not in self.failMemberList and member_info['status'] == "Suspect"]
            for member_id in fail_members_detected:
                self.failMemberList[member_id] = now
                del self.MembershipList[member_id]
                # gossip_log.info("[DELETE] - {}".format(member_id))

    def detectFailMember(self):
        # Method to detect and handle failed members in the membership list for the gossip protocol.
        with self.rlock:
            now = int(time.time())
            # Calculate the threshold time
            threshold_time = now - self.failure_time_threshold
            # Collect members to remove
            fail_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < threshold_time and member_id not in self.failMemberList]
            for member_id in fail_members_detected:
                self.sdfs.add_failure(member_id)
                self.failMemberList[member_id] = now
                del self.MembershipList[member_id]
                # gossip_log.info("[DELETE] - {}".format(member_id))

    def removeFailMember(self):
        # Remove the members from the failMembershipList
        with self.rlock:
            now = int(time.time())
            threshold_time = now - self.cleanup_time_threshold
            fail_members_to_remove = [member_id for member_id, fail_time in self.failMemberList.items() if fail_time < threshold_time]
            for member_id in fail_members_to_remove:
                del self.failMemberList[member_id]

    def chooseMemberToSend(self):
        # Method to randomly choose members from the membership list to send messages to.
        with self.rlock:
            candidates = list(self.MembershipList.keys())
            random.shuffle(candidates)  # Shuffle the list in-place
            return candidates[:self.n_send]

    def update_heartbeat(self):
        # Method to update the server's heartbeat and refresh its status in the membership list.
        with self.rlock:
            self.heartbeat += 1
            self.MembershipList[self.id]["status"] = "Alive"
            self.MembershipList[self.id]["heartbeat"] = self.heartbeat
            self.MembershipList[self.id]["time"] = time.time()
            self.MembershipList[self.id]["incarnation"] = self.incarnation
            if self.gossipS:
                self.detectSuspectAndFailMember()
            else:
                self.detectFailMember()
            self.removeFailMember()
            # self.printMembershipList()

    """
    Thread functions for the Gossip server class
    """
    def receive(self):
        """
        A server's receiver is respnsible to receive all gossip UDP message:
        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                try:
                    # UDP receiver
                    data, server = s.recvfrom(4096)
                    # * if receives data
                    if data:
                        if random.random() < self.drop_rate:
                            continue
                        else:
                            msgs = json.loads(data.decode('utf-8'))
                            # * print out the info of the received message
                            self.updateMembershipList(msgs) 
                except Exception as e:
                    print(e)

    def user_input(self):
        """
        Toggle the sending process on or off.
        :param enable_sending: True to enable sending, False to disable sending.
        """
        while True:
            user_input = input("Enter 'join' to start sending, 'leave' to leave the group, 'enable suspicion' for GOSSIP+S mode and 'disable suspicion' for GOSSIP mode, 'list_mem' to list members and 'list_self' to list self info:\n")
            if user_input == 'join':
                self.enable_sending = True
                print("Starting to send messages.")
                self.MembershipList = {
                f"{ip}:{port}:{self.timejoin}": {
                "id": f"{ip}:{port}:{self.timejoin}",
                "addr": (ip, port),
                "heartbeat": 0,
                "status": "Alive",
                "incarnation": 0,
                "time": time.time(),
                }
                for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in [self.ip, Introducor]]
            }    
            elif user_input == 'leave':
                self.enable_sending = False
                print("Leaving the group.")
            elif user_input == 'enable suspicion':
                self.gossipS = True
                print("Starting gossip S.")
            elif user_input == 'disable suspicion':
                self.gossipS = False
                print("Stopping gossip S.")
            elif user_input == 'list_mem':
                print(self.printMembershipList())
            elif user_input == 'list_self':
                self.printID()
            else:
                print("Invalid input.")

    def send(self):
        """
        A UDP sender for a node. It sends json message to random N nodes periodically
        and maintain time table for handling timeout issue.
        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                try:
                    if self.enable_sending:  # Check if sending is enabled
                        self.update_heartbeat()
                        peers = self.chooseMemberToSend()
                        for peer in peers:
                            send_msg = self.json()
                            s.sendto(json.dumps(send_msg).encode('utf-8'), tuple(self.MembershipList[peer]['addr']))
                    time.sleep(self.protocol_period)          
                except Exception as e:
                    print(e)

    def run(self, user_input_thread = None):
        """
        Run a server as a node in group.
        There are totally two parallel processes for a single node:
        - receiver: receive all UDP message
        - sender: send gossip message periodically

        :return: None
        """
        
        receiver_thread = threading.Thread(target=self.receive)
        receiver_thread.daemon = True
        receiver_thread.start()

        # Start a sender thread
        sender_thread = threading.Thread(target=self.send)
        sender_thread.daemon = True
        sender_thread.start()

        # Start a to update enable sending
        if (user_input_thread == None):
            user_thread = threading.Thread(target=self.user_input)
        else:
            user_thread = threading.Thread(target=user_input_thread)
        user_thread.daemon = True
        user_thread.start()

        user_thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=float, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float, help='The message drop rate', default=0)
    args = parser.parse_args()
    
    server = Server(args)
    server.run()
