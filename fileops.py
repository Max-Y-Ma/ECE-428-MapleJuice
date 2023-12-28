import os
import time
import socket
import sdfs
import gossip

"""
Fileops each node will need to be able to perform. 
These functions are run in a threading context.
"""

# Hostname of current machine
localhostname = socket.gethostname()

# Data directory for all FTP commands
FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"

# COMMS_PORT defines the port that nodes listen to for multireads
COMMS_PORT = 19000

# Declare function for put localfilename sdfsfilename: add a file to sdfs
def put_file(_sdfs, local_filename, sdfs_filename, hostname, port):

    # Calculate start time and network bandwidth 
    start_time = time.perf_counter()

    # Worst Case: Client gives file to leader (+1), who replicates it num_replicas times
    data_Kbytes = (os.path.getsize(f"{FTP_DIRECTORY}/{local_filename}") / 1000.0)

    print("Running put_file...")

    # Set required sdfs_filename format f".{sdfs_filename}"
    sdfs_filename = f".{sdfs_filename}"

    # Open up a client connection to the leader to send in the sdfs_filename
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))

    # Indicate that we are adding a file to sdfs
    message = f"PUT {local_filename} {sdfs_filename}"
    client_socket.send(message.encode())

    # Recieve ACK addresses
    ack = client_socket.recv(1024)
    ack_string = ack.decode()
    print(ack_string)
    if "ACK" in ack_string:
        # Send File to FTP
        print("[INFO]  -  Sending file to leader!")
        _sdfs.ftp_write_file(hostname, local_filename, sdfs_filename)

    # Confirm to leader that file was sent and should be replicated
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))
    message = f"SEND {local_filename} {sdfs_filename}"
    client_socket.send(message.encode())

    # close the client connection
    client_socket.close()

    # Log time and bandwidth
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    sdfs.sdfs_log.info(f"[Put Client]  -  Write Time:{elapsed_time}s | Bandwidth:{data_Kbytes / elapsed_time} KB/s")

# Declare function for get sdfsfilename localfilename: get a file from sdfs
def get_file(_sdfs, sdfs_filename, local_filename, hostname, port):
    # Calculate start time and network bandwidth 
    start_time = time.perf_counter()

    print("Running get_file...")

    # Set required sdfs_filename format f".{sdfs_filename}"
    sdfs_filename = f".{sdfs_filename}"

    # Open up a client connection to the leader to send in the sdfs_filename
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))

    # indicate that we want to get a file from sdfs
    message = f"GET {sdfs_filename} {local_filename} {localhostname}"
    client_socket.send(message.encode())

    # Recieve ACK addresses
    ack = client_socket.recv(1024)
    ack_string = ack.decode()
    if "ACK" in ack_string:
        print("[INFO]  -  Recieved file from leader!")
    else:
        return -1

    # close the client connection
    client_socket.close()

    # Log time and bandwidth
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    # Worst Case: Leader reads from a replica (+1), then sends it to the client (+1)
    data_Kbytes = (os.path.getsize(f"{FTP_DIRECTORY}/{local_filename}") / 1000.0) * (2)
    sdfs.sdfs_log.info(f"[Get Client]  -  Read Time:{elapsed_time}s | Bandwidth:{data_Kbytes / elapsed_time} KB/s")

# Declare function for initiate a multiread 
def multiget_file(_sdfs, sdfs_filename, local_filename, hostname, port, machine_list_idx):

    print("Running multiget_file...")

    # Send multiget to all machines in machine_list
    for mi in machine_list_idx:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((socket.gethostbyname(gossip.HOST_NAME_LIST[int(mi)]), COMMS_PORT))

        # Send multiread message
        message = f"MULTIREAD {sdfs_filename} {local_filename}"
        client_socket.send(message.encode())

        # close the client connection
        client_socket.close()

    # Set required sdfs_filename format f".{sdfs_filename}"
    sdfs_filename = f".{sdfs_filename}"

    # Open up a client connection to the leader to send in the sdfs_filename
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))

    # indicate that we want to get a file from sdfs
    message = f"GET {sdfs_filename} {local_filename} {localhostname}"
    client_socket.send(message.encode())

    # Recieve ACK addresses
    ack = client_socket.recv(1024)
    ack_string = ack.decode()
    print(ack_string)
    if "ACK" in ack_string:
        print("[INFO]  -  Recieved file from leader!")
    else:
        """ Maybe handle Error Case """

    # close the client connection
    client_socket.close()

# Declare function for delete sdfsfilename: delete file from sdfs directory?
def delete_file(_sdfs, sdfs_filename, hostname, port):
    print("Running delete_file...")

    # Set required sdfs_filename format f".{sdfs_filename}"
    sdfs_filename = f".{sdfs_filename}"

    # Open up a client connection to the leader to send in the sdfs_filename
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))

    # indicate that we want to delete a file from sdfs
    message = f"DELETE {sdfs_filename}"
    client_socket.send(message.encode())

     # Recieve ACK addresses
    ack = client_socket.recv(1024)
    ack_string = ack.decode()
    print(ack_string)
    if "ACK" in ack_string:
        print("[INFO]  -  File was deleted from SDFS!")
    else:
        """ Maybe handle Error Case """

    # close the client connection
    client_socket.close()

# Declare function for ls sdfsfilename: list all VM addresses where this file 
# is currently being stored
def list_VMs(sdfs_filename, hostname, port):
    print("Running list_VMs...")

    # Set required sdfs_filename format f".{sdfs_filename}"
    sdfs_filename = f".{sdfs_filename}"

    # Open up a client connection to the leader to send in the sdfs_filename
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((socket.gethostbyname(hostname), port))

    # Request a list of vms with the sdfs filename
    message = f"LS {sdfs_filename}"
    client_socket.send(message.encode())

    # Recieve VM addresses locations
    file_locations = client_socket.recv(1024)
    file_locations_string = file_locations.decode()
    print(file_locations_string)

    # close the client connection
    client_socket.close()