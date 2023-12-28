import os
import ast
import sys
import time
import math
import socket
import random
import fileops
import logging
import argparse
import threading
import subprocess
from sdfs import SDFS, LEADER_PORT
from queue import Queue

# Tutorial for setting up hadoop cluster
# https://www.linode.com/docs/guides/how-to-install-and-set-up-hadoop-cluster/

# HDFS command manual
# https://www.alluxio.io/learn/hdfs/basic-file-operations-commands/#:~:text=ls%3A,listing%20of%20directories%20and%20files.

# User input command line interface
input_str = "\nThe following are available commands:\n \
    maple <maple_exe> <num_maples>\n \
    <sdfs_intermediate_filename_prefix> <sdfs_src_directory>\n\n \
    juice <juice_exe> <num_juices>\n \
    <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>\n \
    delete_input={0, 1}\n\n\
    filter <regex> <num_tasks> <prefix> <src_dir> <dest_filename>\n\
    demo <X> <num_tasks> <prefix> <src_dir> <dest_filename>\n\
    join <num_tasks> <prefix> <dest_filename> <D1_dir> <field1> <D2_dir> <field2>\n\n\
    'jobs': print in queue\n\
    'list_maple': print maple partitions\n\
    'list_keys': print key dictionary\n\
    'list_outfile': print outfile dict\n\
    'sdfs': sdfs input mode\n\n>>> "

# Logging for maplejuice program
logging.basicConfig(level=logging.DEBUG,
                    filename='output.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(levelname)s - %(message)s')
maplejuice_log = logging.getLogger(__name__)

# Client server ports for maple/juice engines
MAPLE_CLIENT_PORT = 18123
MAPLE_SCHEDULER_PORT = 18124
JUICE_CLIENT_PORT = 18125
JUICE_SCHEDULER_PORT = 18126
MAPLE_JUICE_LEADER_PORT = 18127
localhostname = socket.gethostname()

# Directory with all maple and juice executables
FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"
EXE_DIRECTORY = "bin"
COMBINED_FILENAME = "fa23_combined"

# MapleJuice object
maplejuice_server = None
sdfs_server = None

"""
Manages user input and runs resource manager, which sends
jobs to the MapleEngine and JuiceEngine respectively. Jobs
submitted by users are sent to the leader server and queued
in FIFO order. 
"""
class MapleJuice:
    def __init__(self):
        # Job Queueue
        self.job_queue = Queue()

        # Engine Objects
        self.maple_engine = MapleEngine()
        self.maple_engine.run()

        self.juice_engine = JuiceEngine()
        self.juice_engine.run()

        # SQL variables
        self.regex = None
        self.xargs = None

        # Threading Handles
        self.leader_server_thread = None
        self.resource_manager_thread = None
        self.maple_scheduler_server_thread = None
        self.juice_scheduler_server_thread = None

    """
    Utility Functions for MapleJuice
    """
    def print_job_queue(self):
        if sdfs_server.get_leader_hostname() == localhostname:
            for job in list(self.job_queue.queue):
                print(f"{job}")
        else:
            print("Invalid Operation: Non-Leader Node")

    def get_key_dictionary(self):
        return self.maple_engine.key_dictionary
    
    def get_regex(self):
        return self.regex
    
    def get_xargs(self):
        return self.xargs

    """
    Threading Functions for MapleJuice
    """
    def maplejuice_user_input(self):
        """
        User input thread for each node running MapleJuice. This thread executes user input jobs
        for maple and juice jobs. It supports two user input windows, one for MapleJuice and one
        for SDFS/Gossip backend. 
        """

        # Calls the leader election algorithm
        sdfs_server.elect_leader()

        sdfs_input_mode = False
        while True:

            try:
                # Grab User Input
                if sdfs_input_mode:
                    sdfs_server.extern_sdfs_user_input()
                else:
                    user_input = input(input_str)

                # Service non-MapleJuice Job User Input
                if user_input == 'sdfs':
                    sdfs_input_mode = False if sdfs_input_mode else True
                    continue
                elif user_input == 'jobs':
                    self.print_job_queue()
                    continue
                elif user_input == 'list_maple':
                    self.maple_engine.print_task_partition()
                    continue
                elif user_input == 'list_keys':
                    self.maple_engine.print_key_dictionary()
                    continue
                elif user_input == 'list_outfile':
                    self.juice_engine.print_outfile_dictionary()
                    continue

                # Parse maple and juice jobs
                job_args = user_input.split(' ')
                if (len(job_args) < 1):
                    maplejuice_log.info("[ERR]  -  Invalid Job!")
                    print("Invalid Job!")
                    continue

                # Send job to MapleJuice leader server
                handler_thread = threading.Thread(target=self.send_job(job_args))
                handler_thread.daemon = True
                handler_thread.start()

            except Exception as e:
                maplejuice_log.info(f"[INFO]  -  Error in MapleJuice user input: {e}")
                print(f"Error in MapleJuice user input: {e}")

    def send_job(self, job):
        """
        Send user-input job to the MapleJuice leader server. This function is 
        called from maplejuice_user_input(), which is run at every client. 
        """
        maplejuice_log.info(f"[INFO]  -  Sending Job {job}")
        print(f"[INFO]  -  Sending Job {job}")

        # Parse all filenames in src_directory
        message = ""
        if "maple" in job[0]:
            # Grab source directory filenames
            sdfs_src_directory = job[4]
            sdfs_src_files = []
            for filename in os.listdir(f"{FTP_DIRECTORY}/{sdfs_src_directory}"):
                sdfs_src_files.append(filename)

            # Format message
            message = f"{job}${localhostname}${sdfs_src_files}"

        elif "juice" in job[0]:
            # Format message
            message = f"{job}${localhostname}"

        elif "filter" in job[0]:
            # Grab source directory filenames
            sdfs_src_directory = job[4]
            sdfs_src_files = []
            for filename in os.listdir(f"{FTP_DIRECTORY}/{sdfs_src_directory}"):
                sdfs_src_files.append(filename)
            # Format message
            message = f"{job}${localhostname}${sdfs_src_files}"

        elif "demo" in job[0]:
            # Grab source directory filenames
            sdfs_src_directory = job[4]
            sdfs_src_files = []
            for filename in os.listdir(f"{FTP_DIRECTORY}/{sdfs_src_directory}"):
                sdfs_src_files.append(filename)
            # Format message
            message = f"{job}${localhostname}${sdfs_src_files}"

        elif "join" in job[0]:
            # Grab D1 source directory filenames
            d1_src_directory = job[4]
            d1_src_files = []
            for filename in os.listdir(f"{FTP_DIRECTORY}/{d1_src_directory}"):
                d1_src_files.append(filename)

            # Grab D1 field index
            d1_field_index = 0
            d1_field = job[5]
            with open(f"{FTP_DIRECTORY}/{d1_src_directory}/{d1_src_files[0]}", 'r') as d1file:
                # Grab first key line
                header = d1file.readline().split(',')
                print(header)

                for field in header:
                    if d1_field != field:
                        d1_field_index += 1
                    else:
                        break

            # Grab D2 source directory filenames
            d2_src_directory = job[6]
            d2_src_files = []
            for filename in os.listdir(f"{FTP_DIRECTORY}/{d2_src_directory}"):
                d2_src_files.append(filename)

            # Grab D2 field index
            d2_field_index = 0
            d2_field = job[7]
            with open(f"{FTP_DIRECTORY}/{d2_src_directory}/{d2_src_files[0]}", 'r') as d2file:
                # Grab first key line
                header = d2file.readline().split(',')

                for field in header:
                    if d2_field != field:
                        d2_field_index += 1
                    else:
                        break

            # Format message
            message = f"{job}${localhostname}${d1_src_files}${d1_field_index}${d2_src_files}${d2_field_index}"

        else:
            raise ValueError("Invalid job!")

        # Open up a client connection to the leader to send in the sdfs_filename
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((sdfs_server.get_leader_hostname(), MAPLE_JUICE_LEADER_PORT))

        # Format job into message for leader server, '$' delimiter
        client_socket.send(message.encode())

        # Close the client connection
        client_socket.close()

    def leader_server(self):
        # Wait for leader election results
        while not sdfs_server.get_leader_election_status():
            time.sleep(0.1)

        # Check if elected as leader server
        if sdfs_server.get_leader_hostname() != localhostname:
            return

        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, MAPLE_JUICE_LEADER_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        maplejuice_log.info(f"[INFO]  -  MapleJuice leader server is listening on {localhostname}:{MAPLE_JUICE_LEADER_PORT}")
        print(f"MapleJuice leader server is listening on {localhostname}:{MAPLE_JUICE_LEADER_PORT}")

        try:
            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                maplejuice_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle incoming juice task
                    job = client_socket.recv(1024).decode('utf-8')

                    # Enqueue Job
                    self.job_queue.put(job)

                except Exception as e:
                    maplejuice_log.info(f"[INFO]  -  Error in MapleJuice Task: {e}")
                    print(f"Error in MapleJuice Task: {e}")
            
                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            maplejuice_log.info("[ERROR]  -  {}".format(e))
            print(f"Error: {e}")

    def resource_manager(self):
        """ Processes enqueued jobs and invoke respective schedulers """

        # Wait for leader election results
        while not sdfs_server.get_leader_election_status():
            time.sleep(0.1)

        # Check if elected as leader server
        if sdfs_server.get_leader_hostname() != localhostname:
            return
        
        # Start Maple and Juice Engines scheduling servers
        self.maple_scheduler_server_thread = threading.Thread(target=self.maple_engine.maple_scheduler_server)
        self.maple_scheduler_server_thread.daemon = True
        self.maple_scheduler_server_thread.start()

        self.juice_scheduler_server_thread = threading.Thread(target=self.juice_engine.juice_scheduler_server)
        self.juice_scheduler_server_thread.daemon = True
        self.juice_scheduler_server_thread.start()

        maplejuice_log.info(f"[INFO]  -  Running MapleJuice Resource Manager")

        # Process jobs in the queue in FIFO order
        while True:
            if not self.job_queue.empty():
                # Calculate start time and network bandwidth 
                start_time = time.perf_counter()
     
                # Parse args using '$' delimiter
                job_args = f"{self.job_queue.get()}".split("$")
                job = ast.literal_eval(job_args[0])
                client_hostname = job_args[1]

                # Invoke Maple Engine's Scheduler or Juice Engine's Scheduler
                if job[0] == "maple":
                    src_files = ast.literal_eval(job_args[2])
                    scheduler_thread = threading.Thread(target=self.maple_engine.maple_scheduler(job, client_hostname, src_files))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for maple job to finish
                    scheduler_thread.join()

                elif job[0] == "juice":
                    scheduler_thread = threading.Thread(target=self.juice_engine.juice_scheduler(job, client_hostname))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                elif job[0] == "filter":
                    # Execute Maple Filter Phase
                    self.regex = job[1]
                    job[1] = f"mfilter_exe.py"
                    src_files = ast.literal_eval(job_args[2])
                    scheduler_thread = threading.Thread(target=self.maple_engine.maple_scheduler(job, client_hostname, src_files))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for maple job to finish
                    scheduler_thread.join()

                    # Execute Juice Filter Phase
                    job[1] = f"jfilter_exe.py"
                    job[4] = job[5]
                    job[5] = f"1"
                    scheduler_thread = threading.Thread(target=self.juice_engine.juice_scheduler(job, client_hostname))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                elif job[0] == "demo":
                    # Execute Maple Filter Phase
                    self.xargs = job[1]
                    job[1] = f"mdemo_exe.py"
                    src_files = ast.literal_eval(job_args[2])
                    scheduler_thread = threading.Thread(target=self.maple_engine.maple_scheduler(job, client_hostname, src_files))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for maple job to finish
                    scheduler_thread.join()

                    # Execute Juice Filter Phase
                    job[1] = f"jdemo_exe.py"
                    job[4] = job[5]
                    job[5] = f"1"
                    scheduler_thread = threading.Thread(target=self.juice_engine.juice_scheduler(job, client_hostname))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                elif job[0] == "join":
                    d1_src_files = ast.literal_eval(job_args[2])
                    d1_field_index = job_args[3]
                    d2_src_files = ast.literal_eval(job_args[4])
                    d2_field_index = job_args[5]

                    # Execute Maple Join Phase 1
                    maple1_job = [job[0], f"mjoin_exe.py", job[1], job[2], job[4], d1_field_index]
                    scheduler_thread = threading.Thread(target=self.maple_engine.maple_scheduler(maple1_job, client_hostname, d1_src_files))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                    # Execute Maple Join Phase 2
                    maple2_job = [job[0], f"mjoin_exe.py", job[1], job[2], job[6], d2_field_index]
                    scheduler_thread = threading.Thread(target=self.maple_engine.maple_scheduler(maple2_job, client_hostname, d2_src_files))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                    # Execute Juice Join Phase 1
                    juice1_job = [job[0], f"jjoin_exe.py", job[1], job[2], job[3], f"1"]
                    scheduler_thread = threading.Thread(target=self.juice_engine.juice_scheduler(juice1_job, client_hostname))
                    scheduler_thread.daemon = True
                    scheduler_thread.start()

                    # Wait for juice job to finish
                    scheduler_thread.join()

                else:
                    print("Invalid Command: 'maple' or 'juice'")

                # Log time and bandwidth
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                maplejuice_log.info(f"[MapleJuice Time]  -  Execution Time:{elapsed_time}s")
                    
            else:
                # Frequency of new jobs is low, wait 1 second
                time.sleep(1)

    def run(self):
        """
        Runs processes and threads needed for MapleJuice framework.
        """

        # Start Leader Server
        self.leader_server_thread = threading.Thread(target=self.leader_server)
        self.leader_server_thread.daemon = True
        self.leader_server_thread.start()

        # Start Resource Manager
        self.resource_manager_thread = threading.Thread(target=self.resource_manager)
        self.resource_manager_thread.daemon = True
        self.resource_manager_thread.start()

"""
MapleEngine handles all job scheduling and task execution related to Maple commands
"""
class MapleEngine:
    def __init__(self):
        # Threading Handles
        self.maple_client_thread = None
        
        # Barrier Counters
        self.task_counter = 0
        self.leader_working_flag = 0

        # Failed Queues
        self.failed_task_queue = Queue()

        """
        Partition dictionary used to reschedule maple tasks upon failures

        Key: Node_ID
        Example: "fa23-cs425-2301.cs.illinois.edu"

        Values: List of shard files
        Example:
            ["shard01_filename_prefix", "shard02_filename_prefix"]
        """
        self.task_partition = {}

        """
        Key dictionary used to track all the keys for a particular sdfs_prefix

        Key: sdfs_prefix
        Example: "wordcount"

        Value: keys
        Example: ["The", "Hello", "World"]
        """
        self.key_dictionary = {}

    """
    Utility Functions for MapleEngine
    """
    def print_key_dictionary(self):
        """ Print key_dictionary data structure """
        print("========================")
        for prefix in self.key_dictionary.keys():
            print(f"SDFS Prefix: {prefix}\nKeys: {self.key_dictionary[prefix]}")
            print("========================")
   
    def print_task_partition(self):
        """ Print task_partition data structure """
        print("========================")
        for node in self.task_partition.keys():
            print(f"Node Name: {node}\nShards: {self.task_partition[node]}")
            print("========================")

    def add_failure(self, node_id):
        """ Add failed node to queue """
        self.failed_task_queue.put(node_id)

    """
    Threading Functions for MapleEngine
    """
    def maple_scheduler(self, job, client_hostname, src_files):
        """
        Responsible for scheduling and partitions tasks for a given maple job
        """
        maplejuice_log.info(f"[INFO]  -  Running Maple Scheduler {job}")
        print(f"[INFO]  -  Running Maple Scheduler {job}")

        # Parse arguments
        maple_exe = job[1]
        num_maple_tasks = int(job[2])
        sdfs_prefix = job[3]
        sdfs_src_directory = job[4]

        # Add "maple_exe" to SDFS
        if not sdfs_server.valid_sdfs_filename(f".{maple_exe}"):
            sdfs_server.add_remote_sdfsfile(f"{EXE_DIRECTORY}/{maple_exe}", f".{maple_exe}", client_hostname)

        # Retrieve input files
        for filename in src_files:
            sdfs_server.ftp_read_file(client_hostname, filename, f"{sdfs_src_directory}/{filename}")

        # Group input files together into a combined file
        with open(f"{FTP_DIRECTORY}/{COMBINED_FILENAME}", 'w') as outfile:
            for filename in src_files:
                with open(f"{FTP_DIRECTORY}/{filename}", 'r') as infile:
                    outfile.write(infile.read() + "\n")

        # Partition tasks amongst machines and shard input files
        total_lines = 0
        with open(f"{FTP_DIRECTORY}/{COMBINED_FILENAME}", 'rb') as infile:
            # Iterate through each line in the file
            for line in infile:
                total_lines += 1

        shard_size = math.ceil(total_lines / num_maple_tasks)
        MembershipList = sdfs_server.get_membershiplist()
        id_list = list(MembershipList.keys())
        membership_list = [(id.split(':'))[0] for id in id_list]
        membership_list_length = len(membership_list)

        with open(f"{FTP_DIRECTORY}/{COMBINED_FILENAME}", 'rb') as infile:

            for m in range(num_maple_tasks):
                # Hash partition with machine
                machine_idx = m % membership_list_length

                # Create shard file
                shard_name = f"shard{m}_{sdfs_prefix}"
                
                with open(f"{FTP_DIRECTORY}/{shard_name}", 'wb') as shard_file:
                    # Write shard_size lines to shard file
                    for lines in range(shard_size):
                        line = infile.readline()

                        # Check for end of line
                        if not line:
                            break

                        shard_file.write(line)

                # Store shard in SDFS
                sdfs_server.add_remote_sdfsfile(f"{shard_name}", f".{shard_name}", localhostname)

                # Store partition information for failure recovery
                if membership_list[machine_idx] not in self.task_partition:
                    self.task_partition[membership_list[machine_idx]] = [shard_name]
                else:
                    self.task_partition[membership_list[machine_idx]].append(shard_name)

        # Send run commands for maple tasks
        for member_hostname in membership_list:
            if member_hostname in self.task_partition:
                # Open up a client connection to the leader to send in the sdfs_filename
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((member_hostname, MAPLE_CLIENT_PORT))

                # Reply task execution status
                if job[0] == f"filter":
                    message = f"FILTER${maple_exe}${sdfs_prefix}${self.task_partition[member_hostname]}${maplejuice_server.get_regex()}"
                elif job[0] == f"demo":
                    message = f"DEMO${maple_exe}${sdfs_prefix}${self.task_partition[member_hostname]}${maplejuice_server.get_xargs()}"
                elif job[0] == f"join":
                    message = f"JOIN${maple_exe}${sdfs_prefix}${self.task_partition[member_hostname]}${job[5]}"
                else:
                    message = f"MAPLE${maple_exe}${sdfs_prefix}${self.task_partition[member_hostname]}"

                client_socket.send(message.encode())

                # Close the client connection
                client_socket.close()

        # Barrier on "DONE" commands and handle failures
        while self.task_counter < len(self.task_partition):
            if self.failed_task_queue.empty():
                time.sleep(1)
            else:
                # Run failed task on machine that has finished
                failed_hostname = (self.failed_task_queue.get().split(':'))[0]

                failed_task_thread = threading.Thread(target=self.maple_fail_scheduler(job[0], job[5], maple_exe, sdfs_prefix, failed_hostname))
                failed_task_thread.daemon = True
                failed_task_thread.start()

        maplejuice_log.info(f"[INFO]  -  Finished Maple Job {job}")
        print(f"[INFO]  -  Finished Maple Job {job}")

        # Sort keys
        self.key_dictionary[sdfs_prefix] = sorted(self.key_dictionary[sdfs_prefix])

        # Scheduler teardown
        self.task_counter = 0
        self.task_partition = {}

        # Delete remaining shards
        for m in range(num_maple_tasks):
            fileops.delete_file(self, f"shard{m}_{sdfs_prefix}", sdfs_server.get_leader_hostname(), LEADER_PORT)

        # Remove shard files
        for m in range(num_maple_tasks):
            os.remove(f"{FTP_DIRECTORY}/shard{m}_{sdfs_prefix}")

        # Remove input files
        for filename in src_files:
            os.remove(f"{FTP_DIRECTORY}/{filename}")

        # Remove combined input files
        os.remove(f"{FTP_DIRECTORY}/{COMBINED_FILENAME}")

    def maple_fail_scheduler(self, job_0, job_5, maple_exe, sdfs_prefix, failed_hostname):
        # Wait Barrier
        while self.leader_working_flag:
            print("Waiting")
            time.sleep(1)

        # Set Barrier
        self.leader_working_flag = True

        # Open up a client connection to the leader to send in the sdfs_filename
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((sdfs_server.get_leader_hostname(), MAPLE_CLIENT_PORT))

        # Reply task execution status
        if job_0 == f"filter":
            message = f"FILTER${maple_exe}${sdfs_prefix}${self.task_partition[failed_hostname]}${maplejuice_server.get_regex()}"
        elif job_0 == f"demo":
            message = f"DEMO${maple_exe}${sdfs_prefix}${self.task_partition[failed_hostname]}${maplejuice_server.get_xargs()}"
        elif job_0 == f"join":
            message = f"JOIN${maple_exe}${sdfs_prefix}${self.task_partition[failed_hostname]}${job_5}"
        else:
            message = f"MAPLE${maple_exe}${sdfs_prefix}${self.task_partition[failed_hostname]}"

        client_socket.send(message.encode())

        # Close the client connection
        client_socket.close()

    def maple_scheduler_server(self):
        """
        Listen to maple task messages
        """

        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, MAPLE_SCHEDULER_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        maplejuice_log.info(f"[INFO]  -  Maple scheduler server is listening on {localhostname}:{MAPLE_SCHEDULER_PORT}")

        try:
            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                maplejuice_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle incoming maple task messages
                    message = client_socket.recv(65536).decode('utf-8').split('$')

                    if "DONE" in message:
                        # Parse arguments
                        keys = ast.literal_eval(message[1])
                        sdfs_prefix = message[2]
                        task_hostname = message[3]

                        if len(keys) >= 1 and keys[0] != "": 
                            # Store generated keys in self.key_dictionary
                            if sdfs_prefix in self.key_dictionary:
                                self.key_dictionary[sdfs_prefix] = list(set(self.key_dictionary[sdfs_prefix] + keys))
                            else:
                                self.key_dictionary[sdfs_prefix] = keys

                            # Append or store given key files in SDFS
                            for key in keys:
                                sdfs_server.add_remote_sdfsfile(f"{EXE_DIRECTORY}/.{sdfs_prefix}_{key}", f".{sdfs_prefix}_{key}", task_hostname)

                        self.task_counter += 1

                        # Leader Barrier
                        if task_hostname == sdfs_server.get_leader_hostname():
                            self.leader_working_flag = False

                        print("Done!")

                    elif "FAIL" in message:
                        failed_machine_name = message[1]
                        self.add_failure(failed_machine_name)

                except Exception as e:
                    maplejuice_log.info(f"[INFO]  -  Error in Maple Task: {e}")
                    print(f"Error in Maple Task: {e}")
            
                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            maplejuice_log.info("[ERROR]  -  {}".format(e))
            print(f"Error: {e}")

    def maple_client(self):
        """
        Listens for incoming maple tasks to be executed
        """
        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, MAPLE_CLIENT_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        maplejuice_log.info(f"[INFO]  -  Maple client server is listening on {localhostname}:{MAPLE_CLIENT_PORT}")

        try:
            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                maplejuice_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle incoming juice task
                    task = client_socket.recv(1024).decode('utf-8')

                    # Execute Job
                    handler_thread = threading.Thread(target=self.execute_task(task))
                    handler_thread.daemon = True
                    handler_thread.start()

                except Exception as e:
                    maplejuice_log.info(f"[INFO]  -  Error in Maple Task: {e}")
                    print(f"Error in Maple Task: {e}")
            
                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            maplejuice_log.info("[ERROR]  -  {}".format(e))
            print(f"Error: {e}")

    def execute_task(self, task):
        """
        Process the current maple task
        """
        maplejuice_log.info(f"[INFO]  -  Executing Maple Task: {task}")
        # print(f"[INFO]  -  Executing Maple Task: {task}")

        # Clear EXE_DIRECTORY of previous files
        for filename in os.listdir(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}"):
            if filename[0] == ".":
                os.remove(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/{filename}");      

        # Set Barrier
        self.leader_working_flag = True     

        # Execute given maple task
        task_args = f"{task}".split('$')
        message = ""
        keys = []
        if "MAPLE" in task_args[0] or "FILTER" in task_args[0] or "JOIN" in task_args[0] or "DEMO" in task_args[0]:
            maple_exe = task_args[1]
            sdfs_prefix = task_args[2]
            shard_files = ast.literal_eval(task_args[3])
            
            # Retrieve executable file
            while fileops.get_file(sdfs_server, maple_exe, f"_{maple_exe}", sdfs_server.get_leader_hostname(), LEADER_PORT) == -1:
                time.sleep(2)

            # Retrieve shards
            for shard in shard_files:
                while fileops.get_file(sdfs_server, shard, f"_{shard}", sdfs_server.get_leader_hostname(), LEADER_PORT) == -1:
                    time.sleep(2)

            # Maple executable takes 1 input file and outputs a series of KV pairs
            for shard in shard_files:
                try:
                    # Calculate start time
                    start_time = time.perf_counter()
                    
                    # Run maple executable
                    if "FILTER" in task_args[0]:
                        result = subprocess.run(['python3', f"{FTP_DIRECTORY}/_{maple_exe}", f"{FTP_DIRECTORY}/_{shard}", sdfs_prefix, task_args[4]], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                    elif "DEMO" in task_args[0]:
                        result = subprocess.run(['python3', f"{FTP_DIRECTORY}/_{maple_exe}", f"{FTP_DIRECTORY}/_{shard}", sdfs_prefix, task_args[4]], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                    elif "JOIN" in task_args[0]:
                        result = subprocess.run(['python3', f"{FTP_DIRECTORY}/_{maple_exe}", f"{FTP_DIRECTORY}/_{shard}", sdfs_prefix, task_args[4]], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                    else:
                        result = subprocess.run(['python3', f"{FTP_DIRECTORY}/_{maple_exe}", f"{FTP_DIRECTORY}/_{shard}", sdfs_prefix, task_args[4]], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

                    # Log execution time
                    end_time = time.perf_counter()
                    elapsed_time = end_time - start_time
                    maplejuice_log.info(f"[Maple Exe]  -  Execution Time:{elapsed_time}s")
                    
                    # Process output information
                    status = result.returncode
                    if status != 0:
                        message = "FAIL"
                        break

                    output_keys = []
                    if result.stdout != None:
                        output_keys = result.stdout.split(' ')
                    
                    # Append keys to message
                    for key in output_keys:
                        keys.append(key)

                except subprocess.CalledProcessError as e:
                    print(f"Error executing _{maple_exe}: {e}")

            # Task teardown

            # Remove executable
            os.remove(f"{FTP_DIRECTORY}/_{maple_exe}")

            # Remove shard files
            for shard in shard_files:
                os.remove(f"{FTP_DIRECTORY}/_{shard}")
        else:
            message = "FAIL"
            
        # Format Message
        if "FAIL" in message:
            message = f"FAIL${localhostname}"
        else:
            message = f"DONE${keys}${sdfs_prefix}${localhostname}"

        # Open up a client connection to the leader to send in the sdfs_filename
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((sdfs_server.get_leader_hostname(), MAPLE_SCHEDULER_PORT))

        # Reply task execution status
        client_socket.send(message.encode())

        # Close the client connection
        client_socket.close()

    def run(self):
        """
        Runs processes and threads needed for MapleEngine.
        """

        # Run client server
        self.maple_client_thread = threading.Thread(target=self.maple_client)
        self.maple_client_thread.daemon = True
        self.maple_client_thread.start()

"""
JuiceEngine handles all job scheduling and task execution related to Juice commands
"""
class JuiceEngine:
    def __init__(self):
        # Threading Handles
        self.juice_client_thread = None

        # Counter
        self.task_counter = 0

        # Failed Queues
        self.failed_task_queue = Queue()

        """
        Partition dictionary used to reschedule juice tasks upon failures

        Key: Node_ID
        Example: 
            "fa23-cs425-2301.cs.illinois.edu"

        Values: List of keys
        Example:
            ["key1", "key2"]
        """
        self.task_partition = {}

        """
        Juice output file dictionary used to keep track of local output
        files for a specific sdfs_prefix job

        Key: sdfs_prefix
        Example: "wordcount"

        Values: List of local output file names
        Example:
            ["sdfs_dest_filename_hostname", "sdfs_dest_filename_hostname"]
        """
        self.outfile_dict = {}

    """
    Utility Functions for MapleEngine
    """

    def add_failure(self, node_id):
        """ Add failed node to queue """
        self.failed_task_queue.put(node_id)

    def print_outfile_dictionary(self):
        """ Print outfile_dictionary data structure """
        print("========================")
        for prefix in self.outfile_dict.keys():
            print(f"SDFS Prefix: {prefix}\nKeys: {self.outfile_dict[prefix]}")
            print("========================")

    """
    Threading Functions for JuiceEngine
    """
    def juice_scheduler(self, job, client_hostname):
        """
        Responsible for scheduling and partitions tasks for a given juice job
        """
        maplejuice_log.info(f"[INFO]  -  Running Juice Scheduler {job}")
        print(f"[INFO]  -  Running Juice Scheduler {job}")

        # Parse arguments
        juice_exe = job[1]
        num_juice_tasks = int(job[2])
        sdfs_prefix = job[3]
        sdfs_dest_filename = job[4]
        delete = job[5]

        # Add "juice_exe" to SDFS
        sdfs_server.add_remote_sdfsfile(f"{EXE_DIRECTORY}/{juice_exe}", f".{juice_exe}", client_hostname)

        # Check key dictionary for sdfs_prefix
        key_dictionary = maplejuice_server.get_key_dictionary()
        if sdfs_prefix not in key_dictionary:
            maplejuice_log.info(f"[ERROR]  -  Invalid Juice Command: Prefix Error")
            print("[ERROR]  -  Invalid Juice Command: Prefix Error")
            return

        # Partition key files evenly among juice tasks across machines
        key_list = key_dictionary[sdfs_prefix]
        key_list_length = len(key_list)
        partition_size = key_list_length // num_juice_tasks

        MembershipList = sdfs_server.get_membershiplist()
        id_list = list(MembershipList.keys())
        membership_list = [(id.split(':'))[0] for id in id_list]
        membership_list_length = len(membership_list)

        for j in range(num_juice_tasks):
            # Hash partition with machine
            machine_idx = j % membership_list_length

            start = j * partition_size
            end = (j + 1) * partition_size
            keys = key_list[start:end] if (j < num_juice_tasks - 1) else key_list[start:]

            # Store partition information for failure recovery
            if membership_list[machine_idx] not in self.task_partition:
                self.task_partition[membership_list[machine_idx]] = keys
            else:
                self.task_partition[membership_list[machine_idx]] += keys

        # Send run commands for juice tasks
        for member_hostname in membership_list:
            if member_hostname in self.task_partition:
                # Open up a client connection to the leader to send in the sdfs_filename
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((member_hostname, JUICE_CLIENT_PORT))

                # Reply task execution status
                message = f"JUICE${juice_exe}${sdfs_prefix}${self.task_partition[member_hostname]}${sdfs_dest_filename}"
                client_socket.send(message.encode())

                # Close the client connection
                client_socket.close()

        # Barrier on "DONE" commands and handle failures
        while self.task_counter < len(self.task_partition):
            if self.failed_task_queue.empty():
                time.sleep(1)
            else:
                """Check for failures and such..."""

        maplejuice_log.info(f"[INFO]  -  Finished Juice Job {job}")
        print(f"[INFO]  -  Finished Juice Job {job}")

        # Group all sdfs_dest_filename together
        with open(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/.{sdfs_dest_filename}", 'w') as combined_outfile:
            data = ""
            for outfile_name in self.outfile_dict[sdfs_prefix]:
                with open(f"{FTP_DIRECTORY}/{outfile_name}", 'r') as infile:
                    data += infile.read()

            # Sort and write data to combined output file
            sorted_text = '\n'.join(sorted(data.splitlines()))
            combined_outfile.write(sorted_text + '\n')

        # Add Combined output to SDFS
        sdfs_server.add_remote_sdfsfile(f"{EXE_DIRECTORY}/.{sdfs_dest_filename}", f".{sdfs_dest_filename}", localhostname)

        # Delete intermediate files if required
        if delete == "1":
            for key in key_dictionary[sdfs_prefix]:
                sdfs_filename = f".{sdfs_prefix}_{key}"
                sdfs_server.ftp_delete_replicas(sdfs_filename, sdfs_server.leader_hashfs[sdfs_filename]["machine_list"])

        # Scheduler teardown
        self.task_counter = 0
        self.task_partition = {}

        print("Done!")

    def juice_scheduler_server(self):
        """
        Listen to juice task messages
        """

        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, JUICE_SCHEDULER_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        maplejuice_log.info(f"[INFO]  -  Juice scheduler server is listening on {localhostname}:{JUICE_SCHEDULER_PORT}")

        try:
            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                maplejuice_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle incoming juice task messages
                    message = client_socket.recv(65536).decode('utf-8').split('$')

                    if "DONE" in message:
                        # Parse arguments
                        sdfs_prefix = message[1]
                        sdfs_dest_filename = message[2]
                        task_hostname = message[3]

                        # Retrieve output file locally
                        outfile_name = f".{sdfs_dest_filename}_{task_hostname}"
                        sdfs_server.ftp_read_file(task_hostname, outfile_name, f"{EXE_DIRECTORY}/.{sdfs_dest_filename}")

                        # Add to dictionary
                        if sdfs_prefix in self.outfile_dict:
                            self.outfile_dict[sdfs_prefix] += [outfile_name]
                        else:
                            self.outfile_dict[sdfs_prefix] = [outfile_name]
                    
                        self.task_counter += 1

                    elif "FAIL" in message:
                        failed_machine_name = message[1]
                        self.add_failure(failed_machine_name)


                except Exception as e:
                    maplejuice_log.info(f"[INFO]  -  Error in Juice Task: {e}")
                    print(f"Error in Juice Task: {e}")
            
                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            maplejuice_log.info("[ERROR]  -  {}".format(e))
            print(f"Error: {e}")

    def juice_client(self):
        """
        Listens for incoming juice tasks to be executed
        """
        # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the address and port
        server_socket.bind((localhostname, JUICE_CLIENT_PORT))

        # Listen for incoming connections
        server_socket.listen(10)
        maplejuice_log.info(f"[INFO]  -  Juice client server is listening on {localhostname}:{JUICE_CLIENT_PORT}")

        try:
            while True:
                # Accept a new connection
                client_socket, client_address = server_socket.accept()
                maplejuice_log.info(f"[INFO]  -  Connection from {client_address}")

                try:
                    # Handle incoming juice task
                    task = client_socket.recv(65536).decode('utf-8')

                    # Execute Job
                    handler_thread = threading.Thread(target=self.execute_task(task))
                    handler_thread.daemon = True
                    handler_thread.start()

                except Exception as e:
                    maplejuice_log.info(f"[INFO]  -  Error in Juice Task: {e}")
                    print(f"Error in Juice Task: {e}")
            
                finally:
                    # Close the client socket
                    client_socket.close()

        except Exception as e:
            maplejuice_log.info("[ERROR]  -  {}".format(e))
            print(f"Error: {e}")

    def execute_task(self, task):
        """
        Process the current juice task
        """
        maplejuice_log.info(f"[INFO]  -  Executing Juice Task: {task}")
        # print(f"[INFO]  -  Executing Juice Task: {task}")

        # Execute given juice task
        task_args = f"{task}".split('$')
        message = ""
        if "JUICE" in task_args[0]:
            juice_exe = task_args[1]
            sdfs_prefix = task_args[2]
            keys = ast.literal_eval(task_args[3])
            sdfs_dest_filename = task_args[4]

            # Retrieve executable file
            fops_thread = threading.Thread(target=fileops.get_file(sdfs_server, juice_exe, f"_{juice_exe}", sdfs_server.get_leader_hostname(), LEADER_PORT))
            fops_thread.start()
            fops_thread.join()

            # Retrieve key files
            for key in keys:
                if not os.path.isfile(f"{FTP_DIRECTORY}/.{sdfs_prefix}_{key}"):
                    fops_thread = threading.Thread(target=fileops.get_file(sdfs_server, f"{sdfs_prefix}_{key}", f"_{sdfs_prefix}_{key}", sdfs_server.get_leader_hostname(), LEADER_PORT))
                    fops_thread.start()
                    fops_thread.join()

            # Juice executable takes [keys, sdfs_prefix, sdfs_dest_filename]
            
            # Calculate start time
            start_time = time.perf_counter()

            # Run juice executable
            result = subprocess.run(['python3', f"{FTP_DIRECTORY}/_{juice_exe}", f"{keys}", sdfs_prefix, sdfs_dest_filename], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

            # Log execution time
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            maplejuice_log.info(f"[Juice Exe]  -  Execution Time:{elapsed_time}s")

            # Process output information
            status = result.returncode
            if status != 0:
                message = "FAIL"

            # Task teardown

            # Remove executable
            os.remove(f"{FTP_DIRECTORY}/_{juice_exe}")

            # Remove key files
            for key in keys:
                if not os.path.isfile(f"{FTP_DIRECTORY}/.{sdfs_prefix}_{key}"):
                    os.remove(f"{FTP_DIRECTORY}/_{sdfs_prefix}_{key}")
        else:
            message = "FAIL"

        # Format Message
        if "FAIL" in message:
            message = f"FAIL${localhostname}"
        else:
            message = f"DONE${sdfs_prefix}${sdfs_dest_filename}${localhostname}"

        # Open up a client connection to the leader to send in the sdfs_filename
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((sdfs_server.get_leader_hostname(), JUICE_SCHEDULER_PORT))

        # Reply task execution status
        client_socket.send(message.encode())

        # Close the client connection
        client_socket.close()

    def run(self):
        """
        Runs processes and threads needed for JuiceEngine.
        """

        # Run client server
        self.juice_client_thread = threading.Thread(target=self.juice_client)
        self.juice_client_thread.daemon = True
        self.juice_client_thread.start()


if __name__ == "__main__":
    # Grab user-defined arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=float, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float, help='The message drop rate', default=0)
    args = parser.parse_args()

    # Start MapleJuice background processes
    sdfs_server = SDFS()
    maplejuice_server = MapleJuice()
    maplejuice_server.run()

    # Start SDFS and Gossip Membership list background processes
    sdfs_server.run(external=True, args=args, user_input=maplejuice_server.maplejuice_user_input, maplejuice=maplejuice_server)
