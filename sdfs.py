import os
import sys
import time
import signal
import random
import socket
import gossip
import shutil
import fileops
import logging
import argparse
import threading
import multiprocessing
import ftplib
from ftplib import FTP
import pyftpdlib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# User input command line interface
input_str = "\nThe following are available commands:\n \
    'join' | 'leave' | 'list_mem' | 'list_self'\n \
    'put <localfilename> <sdfsfilename>'\n \
    'get <sdfsfilename> <localfilename>'\n \
    'delete <sdfsfilename>'\n \
    'ls <sdfsfilename>'\n \
    'store'\n\n>>> "

# The port the FTP server will listen on
FTP_PORT = 18251

# FTP login information
FTP_USER = "user"
FTP_PASSWORD = "password"

# The directory the FTP user will have full read/write access to.
FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"

# Configure logging for the script. This sets up a logging system that records debug information
# to the 'output.log' file, including timestamps and log levels.
logging.basicConfig(level=logging.DEBUG,
                    filename='output.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(levelname)s - %(message)s')
sdfs_log = logging.getLogger(__name__)

# Defines the hostname of the local machine
localhostname = socket.gethostname()

# LEADER_PORT defines the port that the leader server is listening on
LEADER_PORT = 18250

# COMMS_PORT defines the port that nodes listen to for multireads
COMMS_PORT = 19000

# Seed random number generator
random.seed(int(time.time()))

class SDFS:
    def __init__(self):
        """ 
        This hashtable holds metadata (replications, sdfs files, etc.) information for each server
        in the filesystem. This data structure should only be initialized for the leader.

        Key: file_name

        Value: Dictionary of metadata information
            key : valuelocal_hashfs
            server_hostname : "172.192.1.1"
        """
        self.leader_hashfs = {}

        """
        This hashtable holds cached metadata ({file_name: local, sdfs, size, file_path, etc.}) information
        for the files stored on the current server in the filesystem. This data structure is updated upon 
        fileops commands from the leader. It can also be updated with local files given user input. 

        Key: file_name

        Value: Dictionary of metadata information
            type : value
        """
        self.local_hashfs = {}

        """
        Leader and client variables
        """
        # Objects
        self.gossip_server = None
        self.maplejuice_server = None

        # Thread management
        self.ftp_process = None
        self.sdfs_thread = None
        self.comms_thread = None
        self.sdfs_leader_thread = None
        self.sdfs_stop_flag = threading.Event()
        self.leader_hostname = None
        self.enable_leader_election = True
        self.leader_election_status = False
        self.voting = True
        self.vote_map = {}

        # Replication
        self.num_replicas = 4

        # R/W Synchronization
        self.consecutive_reads = 0
        self.consecutive_writes = 0
        self.consecutive_rw_lock = threading.Lock()
        self.read_count = 0
        self.write_count = 0
        self.rw_lock = threading.Lock()

        # A queue which holds failed nodes 
        self.failure_queue = []

        # Index all local files
        self.index_local_file(FTP_DIRECTORY)

        # Run FTP daemon thread
        self.ftp_process = multiprocessing.Process(target=self.run_sdfs_ftp)
        self.ftp_process.daemon = True
        self.ftp_process.start()

    """
    Utility functions for SDFS class
    """

    # Return leader election status
    def get_leader_election_status(self):
        return self.leader_election_status

    # Return current leader hostname
    def get_leader_hostname(self):
        return self.leader_hostname
    
    # Return membershiplist of nodes
    def get_membershiplist(self):
        return self.gossip_server.getMembershipList()

    # Adds a node id to the failure queue
    def add_failure(self, node_id):
        self.failure_queue.append(node_id)

    # Reads the current files stored at the directory_path
    def index_local_file(self, directory_path):
        # Clear local dictionary
        self.local_hashfs = {}

        # Iterate over the files in the directory
        try:

            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)

                # Remove previous SDFS Files on startup
                if (self.sdfs_thread == None):
                    if '.' == filename[0]:
                        os.remove(file_path)
                        sdfs_log.info(f"[INFO]  -  Old SDFS File '{file_path}' has been deleted.")
                        continue
                
                # Check if the item is a file not a directory
                if os.path.isfile(file_path):
                    # Get metadata for each file and store in local dictionary
                    file_size = os.path.getsize(file_path)
                    creation_time = time.ctime(os.path.getctime(file_path))
                    last_access_time = time.ctime(os.path.getatime(file_path))
                    self.local_hashfs[filename] = {
                        "file_path": file_path,
                        "file_size": file_size,
                        "creation_time": creation_time,
                        "last_access_time": last_access_time
                    }
                else:
                    # Recursively traverse directory
                    for filename in os.listdir(file_path):
                        dir_file_path = os.path.join(file_path, filename)

                        # Remove previous SDFS Files on startup
                        if (self.sdfs_thread == None):
                            if '.' == filename[0]:
                                os.remove(dir_file_path)
                                sdfs_log.info(f"[INFO]  -  Old SDFS File '{dir_file_path}' has been deleted.")

                        # Check if the item is a file not a directory
                        if os.path.isfile(dir_file_path):
                            # Get metadata for each file and store in local dictionary
                            file_size = os.path.getsize(dir_file_path)
                            creation_time = time.ctime(os.path.getctime(dir_file_path))
                            last_access_time = time.ctime(os.path.getatime(dir_file_path))
                            self.local_hashfs[filename] = {
                                "file_path": dir_file_path,
                                "file_size": file_size,
                                "creation_time": creation_time,
                                "last_access_time": last_access_time
                            }

        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error index local file: {e}")

    # Reads the current sdfs file stored at the leader and collects metadata information
    def index_sdfs_file(self, file_name):
        # Check if the item is a file not a directory
        file_path = os.path.join(FTP_DIRECTORY, file_name)
        if os.path.isfile(file_path):
            # Get metadata for each file and store in leader dictionary
            file_size = os.path.getsize(file_path)
            creation_time = time.ctime(os.path.getctime(file_path))
            last_access_time = time.ctime(os.path.getatime(file_path))
            machine_list = self.gen_machine_list(self.num_replicas)
            self.leader_hashfs[file_name] = {
                "file_path": file_path,
                "file_size": file_size,
                "creation_time": creation_time,
                "last_access_time": last_access_time,
                "machine_list": machine_list,
                "write_lock": threading.Lock(),
                "read_lock": threading.Semaphore(2)
            }

    def gen_machine_list(self, Nmachines):
        """
        Randomly generates a list of machines for replicas to be send to. The machines
        must be valid within the membership list.
        """
        
        # Grab membership list from gossip-based module
        MembershipList = self.gossip_server.getMembershipList()

        # Create possible machine list
        id_list = list(MembershipList.keys())
        machine_list = [(id.split(':'))[0] for id in id_list]
        if len(machine_list) > Nmachines:
            machine_list = random.sample(machine_list, Nmachines)

        sdfs_log.info(f"Machine List:{machine_list}")

        return machine_list
    
    def ftp_replicate(self, sdfs_file, machine_list):
        """
        This function is responsible for replicating files across the given machine_list. 
        It will open up FTP write connections to the machine_list and send the leader's local sdfs_file.
        Finally, it will delete the sdfs_file from the leader's local server.
        """

        # Send sdfs_file to all replica machines in machine_list
        sdfs_log.info("[INFO]  -  Replicating Client file in SDFS")
        print("[INFO]  -  Replicating Client file in SDFS")
        for machine_name in machine_list:
            if (machine_name == self.leader_hostname):
                continue
            self.ftp_write_file(machine_name, sdfs_file, sdfs_file)

        # Delete local sdfs_file on leader
        if self.leader_hostname not in machine_list:
            self.ftp_delete_file(localhostname, sdfs_file)

    def ftp_delete_replicas(self, sdfs_file, machine_list):
        """ 
        Responsible for deleting all replica files and removing
        the sdfs filename from the distributed file system.
        """

        # Delete sdfs_file across all replica machines in machine_list
        sdfs_log.info("[INFO]  -  Deleting sdfs file from replica machines")
        print("[INFO]  -  Deleting sdfs file from replica machines")
        for machine_name in machine_list:
            self.ftp_delete_file(machine_name, sdfs_file)

        # Remove entry from leader hash table
        self.leader_hashfs.pop(sdfs_file)

    def ftp_rereplicate(self, sdfs_file, failed_node_id):
        """
        Re-replicate files when a machine has failed. 
        """

        # Calculate start time and network bandwidth 
        start_time = time.perf_counter()

        # Check if sdfs file is stored, "cached", at the leader
        sdfs_log.info("[INFO]  -  Deleting original replicated SDFS files")
        got_file = False
        if self.leader_hostname in self.leader_hashfs[sdfs_file]["machine_list"]:
            got_file = True

        try:
            # Delete old replicas from the current machine_list [Bandwidth is neglible]
            for machine_name in self.leader_hashfs[sdfs_file]["machine_list"]:
                if machine_name != failed_node_id and machine_name != self.leader_hostname:
                    if not got_file:
                        got_file = True
                        self.ftp_read_file(machine_name, sdfs_file, sdfs_file)
                    else:
                        self.ftp_delete_file(machine_name, sdfs_file)

            # Remove entry from leader hash table
            self.leader_hashfs.pop(sdfs_file)

        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error while rereplicating : {e}")
            print(f"Error while rereplicating : {e}")

        # Replicate on a new list of machines [Bandwidth is filesize * number of replicas]
        try:
            sdfs_log.info("[INFO]  -  Replicating SDFS files to new machines")
            self.index_sdfs_file(sdfs_file)
            self.ftp_replicate(sdfs_file, self.leader_hashfs[sdfs_file]["machine_list"])

            # Log time and bandwidth
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            data_Kbytes = self.leader_hashfs[sdfs_file]["file_size"] * len(self.leader_hashfs[sdfs_file]["machine_list"]) / 1000.0
            sdfs_log.info(f"[Re-Replication]  -  Replicate Time:{elapsed_time}s | Bandwidth:{data_Kbytes / elapsed_time} KB/s")
        
        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error2 while rereplicating : {e}")
            print(f"Error2 while rereplicating : {e}")

    # Custom signal handler to gracefully shutdown the FTP server
    def signal_handler(self, signum, frame):
        # Gracefully terminate FTP server
        sdfs_log.info("[INFO]  -  Gracefully terminating FTP server!")
        print("Gracefully terminating FTP server!")
        self.ftp_process.terminate()
        self.ftp_process.join()

        sys.exit(0)

    def print_local_files(self):
        print("========================")
        for file in self.local_hashfs.keys():
            print("File Name: {}".format(file))
            for metadata_key, value in self.local_hashfs[file].items():
                print("{}: {}".format(metadata_key, value))
            print("========================")

    def print_leader_files(self):
        print("========================")
        for file in self.leader_hashfs.keys():
            print("File Name: {}".format(file))
            for metadata_key, value in self.leader_hashfs[file].items():
                print("{}: {}".format(metadata_key, value))
            print("========================")

    def file_location_json(self, file):
        try:
            file_json = {
                f"{machine} " 
                for machine in self.leader_hashfs[file]["machine_list"]
            }
        except Exception as e:
            file_json = f"[ERROR]  -  Invalid file: {e}"
        finally:
            return f"{file_json}"
        
    def valid_sdfs_filename(self, sdfs_filename):
        return sdfs_filename in self.leader_hashfs

    # Kills the local server thread
    def kill_server_thread(self):
        self.sdfs_stop_flag.set()

    def get_sdfs_file(self, sdfs_filename):
        """
        Retrieve SDFS file locally to leader machine's server. This file
        can be located locally on the leader's machine or from a replica machine
        """

        # Check replicas for file, exit upon successful read
        sdfs_log.info("[INFO]  -  Grabbing sdfs file from replica machines")
        print("Grabbing sdfs file from replica machines")

        # Check if replica is cached at the leader machine
        if localhostname not in self.leader_hashfs[sdfs_filename]["machine_list"]:
            for machine_name in self.leader_hashfs[sdfs_filename]["machine_list"]:
                if (self.ftp_read_file(machine_name, sdfs_filename, sdfs_filename) == 0):
                    break

    """
    Helper functions for Fileops and SDFS class
    """

    def ftp_write_file(self, hostname, local_filename, sdfs_filename):
        """
        Writes a localfile to the hostname FTP server as the sdfsfile
        """

        try:
            # Establish a connection to the FTP server
            ftp = FTP()
            ftp.connect(socket.gethostbyname(hostname), FTP_PORT)
            ftp.login(FTP_USER, FTP_PASSWORD)

            sdfs_log.info(f"[INFO]  -  Writing {FTP_DIRECTORY}/{local_filename} to {FTP_DIRECTORY}/{sdfs_filename}")

            # Open and write remote file on the FTP server
            if os.path.isfile(f"{FTP_DIRECTORY}/{local_filename}"):
                with open(f"{FTP_DIRECTORY}/{local_filename}", 'rb') as file:
                    ftp.storbinary(f'STOR {sdfs_filename}', file)
            else:
                raise Exception("Invalid local file")

        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error while writing ftp file: {e}")
            print(f"Error while writing ftp file: {e}")
        finally:
            # Close the FTP connection
            ftp.quit()

    def ftp_read_file(self, hostname, local_filename, sdfs_filename):
        """
        Reads a sdfsfile from given hostname FTP server and store as the localfile
        """

        try:
            # Establish a connection to the FTP server
            ftp = FTP()
            ftp.connect(socket.gethostbyname(hostname), FTP_PORT)
            ftp.login(FTP_USER, FTP_PASSWORD)

            # Open and write to local file after reading from FTP server
            with open(f"{FTP_DIRECTORY}/{local_filename}", 'wb') as local_file:
                def retr_callback(data):
                    local_file.write(data)

                ftp.retrbinary(f'RETR {sdfs_filename}', retr_callback)

        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error reading writing ftp file: {e}")
            print(f"Error while reading ftp file: {e} {sdfs_filename}")
            # Close the FTP connection
            ftp.quit()
            return -1
        
        # Close the FTP connection
        ftp.quit()
        return 0

    def ftp_delete_file(self, hostname, sdfs_filename):
        """
        Deletes sdfsfile on given hostname FTP server
        """

        try:
            # Establish a connection to the FTP server
            ftp = FTP()
            ftp.connect(socket.gethostbyname(hostname), FTP_PORT)
            ftp.login(FTP_USER, FTP_PASSWORD)

            # Delete file on FTP server
            ftp.delete(sdfs_filename)

        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error deleting writing ftp file: {e}")
            print(f"Error while reading ftp file: {e}")
        finally:
            # Close the FTP connection
            ftp.close()
            return 0

    def elect_leader(self):
        """
        Implement leader election on start up, blocking until an appropriate
        node is selected as the new leader. Record the leader and start the SDFS.
        This method can be called again if the leader node has failed.  
        """

        sdfs_log.info("[STATUS]  -  Leader Election!")
        print("Leader Election!")

        # Allow membership lists to settle
        time.sleep(5)

        # Reset incarnation number
        incarnation_number = 0

        # Voting loop, which is broken upon an election message
        while self.voting:
            # Grab membership list from gossip-based module
            MembershipList = self.gossip_server.getMembershipList()
            if (len(MembershipList) <= 1):
                continue

            # Create possible machine list
            id_list = list(MembershipList.keys())
            machine_list = [(id.split(':'))[0] for id in id_list]

            # Send vote and incarnation number to quorum of nodes
            machines = random.sample(machine_list, (len(MembershipList) // 2) + 1)
            for machine in machines:
                if machine == localhostname:
                    continue

                # Open up a client connection to cast vote with incarnation number
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((socket.gethostbyname(f"{machine}"), COMMS_PORT))

                # Send VOTE message
                message = f"VOTE {localhostname} {incarnation_number}"
                client_socket.send(message.encode())

                client_socket.close()

            # Increment incarnation number to the next round
            incarnation_number += 1
            time.sleep(1)

        # Allow leadership to settle down
        print("Leader election is wrapping up...")
        time.sleep(10)

        # Run base SDFS Server
        self.sdfs_thread = threading.Thread(target=self.sdfs_server)
        self.sdfs_thread.daemon = True
        self.sdfs_thread.start()

        # Run the Leader server if the current node is a leader
        if localhostname == self.leader_hostname:
            self.sdfs_leader_thread = threading.Thread(target=self.sdfs_leader)
            self.sdfs_leader_thread.daemon = True
            self.sdfs_leader_thread.start()

        # Update leader election status
        self.leader_election_status = True

    def extern_sdfs_user_input(self):
        """
        User input function for other objects to use for interfacing with SDFS
        """

        try:
            # Grab User Input
            user_input = input(input_str)

            # Support client fileops queries
            fops_args = user_input.split(' ')
            if (len(fops_args) < 1):
                sdfs_log.info("[ERR]  -  'fops' Invalid Input!")
                print("'fops' Invalid Input!")
                return
            
            # Commands to support gossip membership list
            if user_input == 'join':
                self.gossip_server.join()

            elif user_input == 'leave':
                self.gossip_server.leave()

            elif user_input == 'list_mem':
                self.gossip_server.list_mem()

            elif user_input == 'list_self':
                self.gossip_server.list_self()

            elif user_input == 'list_leader':
                print(self.leader_hostname)

            elif user_input == 'check_leader':
                if (self.gossip_server.check_leader(self.leader_hostname)):
                    sdfs_log.info("Leader Alive!")
                    print("Leader Alive!")
                else:
                    sdfs_log.info("No Leader!")
                    print("No Leader!")

            # Commands to support file operations
            elif fops_args[0] == 'put':
                if (len(fops_args) != 3):
                    sdfs_log.info("[ERR]  -  'put' Invalid Input!")
                    print("'put' Invalid Input!")
                    return

                fops_thread = threading.Thread(target=fileops.put_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT))
                fops_thread.start()

            elif fops_args[0] == 'get':
                if (len(fops_args) != 3):
                    sdfs_log.info("[ERR]  -  'get' Invalid Input!")
                    print("'get' Invalid Input!")
                    return

                fops_thread = threading.Thread(target=fileops.get_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT))
                fops_thread.start()

            elif fops_args[0] == 'multiget':
                if (len(fops_args) <= 3):
                    sdfs_log.info("[ERR]  -  'get' Invalid Input!")
                    print("'get' Invalid Input!")
                    return

                machine_list_idx = fops_args[3:]
                fops_thread = threading.Thread(target=fileops.multiget_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT, machine_list_idx))
                fops_thread.start()

            elif fops_args[0] == 'delete':
                if (len(fops_args) != 2):
                    sdfs_log.info("[ERR]  -  'delete' Invalid Input!")
                    print("'delete' Invalid Input!")
                    return

                fops_thread = threading.Thread(target=fileops.delete_file(self, fops_args[1], self.leader_hostname, LEADER_PORT))
                fops_thread.start()

            elif fops_args[0] == 'ls':
                if (len(fops_args) != 2):
                    sdfs_log.info("[ERR]  -  'ls' Invalid Input!")
                    print("'ls' Invalid Input!")
                    return

                fops_thread = threading.Thread(target=fileops.list_VMs(fops_args[1], self.leader_hostname, LEADER_PORT))
                fops_thread.start()
                
            elif user_input == 'store':
                self.print_local_files()

            elif user_input == 'leader_store':
                self.print_leader_files()

            else:
                sdfs_log.info("[ERR]  -  Generic Invalid Input!")
                print("Generic Invalid Input!")
        
        except Exception as e:
            sdfs_log.info(f"[INFO]  -  Error in user input: {e}")
            print(f"Error in user input: {e}")

    def add_remote_sdfsfile(self, local_filename, sdfs_filename, hostname):
        try:
        
            # Append contents to current SDFS file
            if self.valid_sdfs_filename(sdfs_filename):
                # Fetch current SDFS file
                if not os.path.isfile(f"{FTP_DIRECTORY}/{sdfs_filename}"):
                    self.get_sdfs_file(sdfs_filename)

                # Fetch new SDFS file from given hostname
                self.ftp_read_file(hostname, f"._{sdfs_filename}", local_filename)

                # Append contents to current SDFS file
                with open(f"{FTP_DIRECTORY}/{sdfs_filename}", 'a') as outfile:
                    with open(f"{FTP_DIRECTORY}/._{sdfs_filename}", 'r') as appendfile:
                        outfile.write(appendfile.read())

                # Rereplicate appended SDFS file
                for machine_name in self.leader_hashfs[sdfs_filename]["machine_list"]:
                    if machine_name != self.leader_hostname:
                        self.ftp_delete_file(machine_name, sdfs_filename)
                self.ftp_replicate(sdfs_filename, self.leader_hashfs[sdfs_filename]["machine_list"])

                # Remove old SDFS file
                os.remove(f"{FTP_DIRECTORY}/._{sdfs_filename}")

            # Append contents to current local file
            elif os.path.isfile(f"{FTP_DIRECTORY}/{sdfs_filename}"):

                # Fetch new SDFS file from given hostname
                self.ftp_read_file(hostname, f"._{sdfs_filename}", local_filename)

                self.index_sdfs_file(sdfs_filename)

                # Append contents to current SDFS file
                with open(f"{FTP_DIRECTORY}/{sdfs_filename}", 'a') as outfile:
                    with open(f"{FTP_DIRECTORY}/._{sdfs_filename}", 'r') as appendfile:
                        outfile.write(appendfile.read())

                self.ftp_replicate(sdfs_filename, self.leader_hashfs[sdfs_filename]["machine_list"])

                # Remove old SDFS file
                os.remove(f"{FTP_DIRECTORY}/._{sdfs_filename}")
                    
            else:
                # Fetch remote file
                if not os.path.isfile(f"{FTP_DIRECTORY}/{sdfs_filename}"):
                    self.ftp_read_file(hostname, sdfs_filename, local_filename)

                self.index_sdfs_file(sdfs_filename)

                # Replicate file for SDFS robustness
                self.ftp_replicate(sdfs_filename, self.leader_hashfs[sdfs_filename]["machine_list"])

        except Exception as e:
            sdfs_log.info(f"[ERROR]  -  add_remote_sdfsfile(): {e}")
            print(f"[ERROR]  -  add_remote_sdfsfile(): {e}")

    """
    Thread functions for SDFS class.
    """
    def sdfs_user_input(self):
        """
        User input thread for each client of the SDFS. This thread executes user input commands.
        Technically, it depends on gossip server calling run() from gossip.py. 
        """

        # Calls the leader election algorithm
        self.elect_leader()

        while True:
            try:
                # Grab User Input
                user_input = input(input_str)

                # Support client fileops queries
                fops_args = user_input.split(' ')
                if (len(fops_args) < 1):
                    sdfs_log.info("[ERR]  -  'fops' Invalid Input!")
                    print("'fops' Invalid Input!")
                    continue
                
                # Commands to support gossip membership list
                if user_input == 'join':
                    self.gossip_server.join()

                elif user_input == 'leave':
                    self.gossip_server.leave()

                elif user_input == 'list_mem':
                    self.gossip_server.list_mem()

                elif user_input == 'list_self':
                    self.gossip_server.list_self()

                elif user_input == 'list_leader':
                    print(self.leader_hostname)

                elif user_input == 'check_leader':
                    if (self.gossip_server.check_leader(self.leader_hostname)):
                        sdfs_log.info("Leader Alive!")
                        print("Leader Alive!")
                    else:
                        sdfs_log.info("No Leader!")
                        print("No Leader!")

                # Commands to support file operations
                elif fops_args[0] == 'put':
                    if (len(fops_args) != 3):
                        sdfs_log.info("[ERR]  -  'put' Invalid Input!")
                        print("'put' Invalid Input!")
                        continue

                    fops_thread = threading.Thread(target=fileops.put_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT))
                    fops_thread.start()

                elif fops_args[0] == 'get':
                    if (len(fops_args) != 3):
                        sdfs_log.info("[ERR]  -  'get' Invalid Input!")
                        print("'get' Invalid Input!")
                        continue

                    fops_thread = threading.Thread(target=fileops.get_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT))
                    fops_thread.start()

                elif fops_args[0] == 'multiget':
                    if (len(fops_args) <= 3):
                        sdfs_log.info("[ERR]  -  'get' Invalid Input!")
                        print("'get' Invalid Input!")
                        continue

                    machine_list_idx = fops_args[3:]
                    fops_thread = threading.Thread(target=fileops.multiget_file(self, fops_args[1], fops_args[2], self.leader_hostname, LEADER_PORT, machine_list_idx))
                    fops_thread.start()

                elif fops_args[0] == 'delete':
                    if (len(fops_args) != 2):
                        sdfs_log.info("[ERR]  -  'delete' Invalid Input!")
                        print("'delete' Invalid Input!")
                        continue

                    fops_thread = threading.Thread(target=fileops.delete_file(self, fops_args[1], self.leader_hostname, LEADER_PORT))
                    fops_thread.start()

                elif fops_args[0] == 'ls':
                    if (len(fops_args) != 2):
                        sdfs_log.info("[ERR]  -  'ls' Invalid Input!")
                        print("'ls' Invalid Input!")
                        continue

                    fops_thread = threading.Thread(target=fileops.list_VMs(fops_args[1], self.leader_hostname, LEADER_PORT))
                    fops_thread.start()
                    
                elif user_input == 'store':
                    self.print_local_files()

                elif user_input == 'leader_store':
                    self.print_leader_files()

                else:
                    sdfs_log.info("[ERR]  -  Generic Invalid Input!")
                    print("Generic Invalid Input!")
            
            except Exception as e:
                sdfs_log.info(f"[INFO]  -  Error in SDFS user input: {e}")
                print(f"Error in SDFS user input: {e}")

    def sdfs_server(self):
        """
        Server running on each node in the SDFS, which listens to incoming messages
        from the leader node. This server should fulfill replication requests, service
        reads and writes, store data in required structures, etc.
        """

        print("SDFS Server")
        while not self.sdfs_stop_flag.is_set():
            # Index all local files
            self.index_local_file(FTP_DIRECTORY)

            # Check node failures
            if len(self.failure_queue) != 0 and localhostname == self.leader_hostname:
                failed_node = self.failure_queue.pop(0)
                sdfs_log.info(f"[ERROR]  -  Node '{failed_node}' has failed")

                # Re replicate all the nodes files 
                failed_id = failed_node.split(':')[0]
                for sdfs_filename, metadata_dict in self.leader_hashfs.items():
                    if failed_id in metadata_dict["machine_list"]:
                        # If failed node had a replica, re replacate sdfs_file
                        self.ftp_rereplicate(sdfs_filename, failed_id)
                        sdfs_log.info(f"[INFO]  -  Rereplicated sdfs_file {sdfs_filename}")

                self.maplejuice_server.maple_engine.add_failure(failed_node)
                self.maplejuice_server.juice_engine.add_failure(failed_node)

            # Log Consecutive R/W
            if self.consecutive_reads >= 4 and self.write_count > 0:
                sdfs_log.info(f"[ERROR]  -  Consecutive Reads @ {self.consecutive_reads}")

            if self.consecutive_writes >= 4 and self.read_count > 0:
                sdfs_log.info(f"[ERROR]  -  Consecutive Writes @ {self.consecutive_writes}")

            time.sleep(1)

    def comms_server(self):
        """
        Server running on the leader node in the SDFS, which fulfills fileop requests from the client,
        stores metadata for each file in the SDFS, enforces R/W Synchronization
        """

        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, COMMS_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        sdfs_log.info(f"[INFO]  -  Comms server is listening on {localhostname}:{COMMS_PORT}")

        try:

            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                sdfs_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle data from the client
                    data = client_socket.recv(1024)

                    # Process the received data
                    client_msg = data.decode('utf-8')
                    sdfs_log.info(f"[INFO]  -  Received data {client_msg}")

                    # Separate comand arguments
                    client_args = client_msg.split(' ')

                    # Check for MULTIREAD
                    if "MULTIREAD" in client_msg:
                        if (len(client_args) != 3):
                            sdfs_log.info("[ERR]  -  'get' Invalid Input!")
                            print("'get' Invalid Input!")
                            continue

                        fops_thread = threading.Thread(target=fileops.get_file(self, client_args[1], client_args[2], self.leader_hostname, LEADER_PORT))
                        fops_thread.start()

                    # Process VOTE messages
                    if "VOTE" in client_msg:
                        hostname = client_args[1]
                        incarnation = int(client_args[2])
                        sdfs_log.info(f"[VOTE]  -  {hostname} {incarnation}")

                        # Relay current leader if one has already been elected
                        if self.leader_hostname is not None:
                            # Open up a client connection to each machine
                            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            client_socket.connect((socket.gethostbyname(f"{hostname}"), COMMS_PORT))

                            # Send Elect Message
                            message = f"ELECT {self.leader_hostname}"
                            client_socket.send(message.encode())

                            client_socket.close()

                            continue

                        # Track votes using incarnation number
                        if self.vote_map.get(incarnation) is None:
                            self.vote_map[incarnation] = 1
                        else:
                            self.vote_map[incarnation] += 1

                        # Multicast ELECT message if choosen as the leader by a quorum of nodes
                        MembershipList = self.gossip_server.getMembershipList()
                        if self.vote_map[incarnation] > (len(MembershipList) // 2):
                            # Create possible machine list
                            id_list = list(MembershipList.keys())
                            machine_list = [(id.split(':'))[0] for id in id_list]

                            for machine_name in machine_list:
                                # Open up a client connection to each machine
                                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                client_socket.connect((socket.gethostbyname(f"{machine_name}"), COMMS_PORT))

                                # Send Elect Message
                                message = f"ELECT {localhostname}"
                                client_socket.send(message.encode())

                                client_socket.close()

                    # Process ELECT messages by setting leader, ties are broken with lowest ID
                    if "ELECT" in client_msg:
                        elected_hostname = client_args[1]
                        sdfs_log.info(f"[ELECT]  -  {elected_hostname}")

                        # Disable voting in elect_leader
                        self.voting = False

                        # Record Leaderg
                        if self.leader_hostname == None: 
                            # self.leader_hostname = elected_hostname
                            self.leader_hostname = f"fa23-cs425-2310.cs.illinois.edu"
                            print(f"Elected Leader: {self.leader_hostname}")
                        else:
                            if elected_hostname < self.leader_hostname:
                                # self.leader_hostname = elected_hostname
                                self.leader_hostname = f"fa23-cs425-2310.cs.illinois.edu"
                                print(f"Elected Leader: {self.leader_hostname}")

                except Exception as e:
                    sdfs_log.info(f"[INFO]  -  Error while handling data: {e}")
                    print(f"Error while handling data: {e}")

                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            sdfs_log.info(f"[ERR]  -  Comms {e}")
            print(f"[ERR]  -  Comms {e}")

        finally:
            # Close the server socket
            server_socket.close()

    def sdfs_leader(self):
        """
        Server running on the leader node in the SDFS, which fulfills fileop requests from the client,
        stores metadata for each file in the SDFS, enforces R/W Synchronization
        """

        print("Leader Server")

        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, LEADER_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        sdfs_log.info(f"[INFO]  -  Leader server is listening on {localhostname}:{LEADER_PORT}")
        print(f"Leader server is listening on {localhostname}:{LEADER_PORT}")
        try:

            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                sdfs_log.info(f"[INFO]  -  Connection from {client_address}")
                print(f"Connection from {client_address}")

                try:
                    # Handle data from the client
                    data = client_socket.recv(1024)

                    # Data handler thread entrypoint
                    handler_thread = threading.Thread(target=self.leader_handler(client_socket, data))
                    handler_thread.start()

                except Exception as e:
                    sdfs_log.info(f"[INFO]  -  Error while handling data: {e}")
                    print(f"Error while handling data: {e}")

                finally:
                    # Close the client socket
                    client_socket.close()

                time.sleep(1)

        except Exception as e:
            sdfs_log.info("[ERR]  -  {}".format(e))
            print(f"Error: {e}")

        finally:
            # Close the server socket
            server_socket.close()

    def leader_handler(self, client_socket, data):
        """
        This function runs within a threaded context. Refer to sdfs_leader() for the 
        execution context. This handles all the read, write, and delete request for the leader.
        """

        # Process the received data
        client_msg = data.decode('utf-8')
        sdfs_log.info(f"[INFO]  -  Received data {client_msg}")
        print(f"Received data {client_msg}")

        # Separate comand arguments
        client_args = client_msg.split(' ')
        response = ""

        # Respond to PUT message, send ACK to client and wait for a SEND command
        if "PUT" in client_msg:
            # if self.valid_sdfs_filename(client_args[2]):

                # Critical Section: Acquire write lock (1 machine can write file simultaneously)
                # with self.leader_hashfs[client_args[2]]["write_lock"]:
                #     sdfs_log.info(f"[INFO]  -  Acquire Write Lock for {client_args[2]}")

                #     # Prevent reads by acquiring both read semaphores
                #     self.leader_hashfs[ client_args[2]]["read_lock"].acquire()
                #     self.leader_hashfs[client_args[2]]["read_lock"].acquire()

                #     # Log Updating
                #     print(f"Client is updating {client_args[2]}")
                #     sdfs_log.info(f"[INFO]  - Client is updating {client_args[2]}")

                #     # Release read semaphores
                #     self.leader_hashfs[client_args[2]]["read_lock"].release()
                #     self.leader_hashfs[client_args[2]]["read_lock"].release()

            response = f"ACK"

        # Respond to SEND message, replicate given file to all replica machines
        elif "SEND" in client_msg:
            # Calculate start time and network bandwidth 
            start_time = time.perf_counter()
            
            with self.rw_lock:
                self.write_count += 1

            # Index new sdfsfile
            if not self.valid_sdfs_filename(client_args[2]):
                self.index_sdfs_file(client_args[2])

            # Replicate file for SDFS robustness
            self.ftp_replicate(client_args[2], self.leader_hashfs[client_args[2]]["machine_list"])

            with self.rw_lock:
                self.write_count -= 1

            # Log time and bandwidth
            # Worst Case: Leader replicates all files to num_replicas
            data_Kbytes = (self.leader_hashfs[client_args[2]]["file_size"] / 1000.0)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            sdfs_log.info(f"[Put Leader]  -  Write Time:{elapsed_time}s | Bandwidth:{data_Kbytes / elapsed_time} KB/s")

        # Grab sdfs filename and send to client FTP server as localfile name. 
        elif "GET" in client_msg:
            with self.rw_lock:
                self.read_count += 1

            if not self.valid_sdfs_filename(client_args[1]):
                response = f"INVALID SDFS FILE"
            else:
                # Prevent consecutive reads 
                if self.consecutive_reads >= 4 and self.write_count > 0:
                    sdfs_log.info(f"[INFO]  -  Reads yielding to writes")
                    while(self.consecutive_reads >= 4):
                        time.sleep(0.5)

                # Cached local file and write it to the client
                self.get_sdfs_file(client_args[1])
                self.ftp_write_file(client_args[3], client_args[1], client_args[2])

                response = f"ACK"

            with self.rw_lock:
                self.read_count -= 1
        
        # Delete sdfs filename across all replica machines
        elif "DELETE" in client_msg:
            if not self.valid_sdfs_filename(client_args[1]):
                response = f"INVALID SDFS FILE"
            else:

                # Delete sdfs file from the distributed filesystem and remove replicas
                self.ftp_delete_replicas(client_args[1], self.leader_hashfs[client_args[1]]["machine_list"])

                response = f"ACK"
        
        # Respond to LS message
        elif "LS" in client_msg:
            response = self.file_location_json(client_args[1])

        # Respond to the client
        client_socket.send(response.encode('utf-8'))
    
    def run_sdfs_ftp(self):
        """
        Runs background thread for FTP server at every node.
        """

        # Define a new user having full r/w permissions.
        authorizer = DummyAuthorizer()
        authorizer.add_user(FTP_USER, FTP_PASSWORD, FTP_DIRECTORY, perm='elradfmw')

        handler = FTPHandler
        handler.authorizer = authorizer

        # Configure server
        address = (socket.gethostbyname(localhostname), FTP_PORT)
        server = FTPServer(address, handler)
        server.max_cons = 256
        server.max_cons_per_ip = 5
        sdfs_log.info(f"[INFO]  -  FTP server is listening on {localhostname}:{FTP_PORT}")
        print(f"FTP server is listening on {localhostname}:{FTP_PORT}")
        server.serve_forever()

    def run(self, maplejuice = None, external = None, args = None, user_input = None):
        """
        Run server or leader thread based on leader election.
        """


        # Run comms server
        self.comms_thread = threading.Thread(target=self.comms_server)
        self.comms_thread.daemon = True
        self.comms_thread.start()

        # Register the signal handler for the interrupt signal (SIGINT)
        signal.signal(signal.SIGINT, self.signal_handler)

        # Gossip Membership list processes
        self.maplejuice_server = maplejuice
        if external != None and user_input != None:
            self.gossip_server = gossip.Server(args, self, maplejuice)
            self.gossip_server.run(user_input)


if __name__ == "__main__":
    # Grab user-defined arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=float, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float, help='The message drop rate', default=0)
    args = parser.parse_args()

    # Start SDFS and Gossip Membership list processes
    sdfs_server = SDFS()
    sdfs_server.run(external=True, args=args, user_input=sdfs_server.sdfs_user_input)
