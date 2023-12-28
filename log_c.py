#importing modules
import sys 
import time
import socket
import threading

# All server names in system
server_names = ["fa23-cs425-2301.cs.illinois.edu",
                "fa23-cs425-2302.cs.illinois.edu",
                "fa23-cs425-2303.cs.illinois.edu",
                "fa23-cs425-2304.cs.illinois.edu",
                "fa23-cs425-2305.cs.illinois.edu",
                "fa23-cs425-2306.cs.illinois.edu",
                "fa23-cs425-2307.cs.illinois.edu",
                "fa23-cs425-2308.cs.illinois.edu",
                "fa23-cs425-2309.cs.illinois.edu",
                "fa23-cs425-2310.cs.illinois.edu",]

# Grab translated ip addresses
ip_addrs = [socket.gethostbyname(n) for n in server_names]
port = 18000

# List of active threads
threads = []

print_lock = threading.Lock()

def wait_for_response(client_socket, index):
    """
    Thread function that waits for response from servers.
    Print response to the standard output

    Params:
        client_socket : Client socket descriptor 
        index : VM Number
    """

    # Wait for server response
    max_response_size = 2**30
    response = client_socket.recv(max_response_size)

    with print_lock:
        print(f"################ VM {index} ################")
        print(response.decode())
        print(f"################ VM {index} ################")

    client_socket.close()

def send_requests(target_list, command):
    """
    Sends requests to servers from given target_list

    Params:
        target_list : List of servers to query
        command : Command to send to each server
    
    Return: N/A
    """

    if target_list == "all":
        target_list = "0 1 2 3 4 5 6 7 8 9"

    # Open TCP connection to each server
    for index in [int(x) for x in target_list.split()]:
        # Setup client connection
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_socket.connect((ip_addrs[index], port))
        client_socket.send(command.encode())

        # Schedule waiting thread
        reponse_thread = threading.Thread(target=wait_for_response, args=(client_socket, index))
        reponse_thread.start()
        threads.append(reponse_thread)

    # Wait for all threads to finish
    for t in threads:
        t.join()

if __name__ == "__main__":
    # Record the start time
    start_time = time.perf_counter()

    # Check arguments
    if len(sys.argv) < 2:
        print("Usage: python3 log_client.py [target_list] [grep <flags> <file_path>]")
        print("Example: python3 log_c.py '1 4 6 7 8 9 10' 'grep -c /logs/vm.log'")
        print("Example: python3 log_c.py 'all' 'grep -c /logs/vm.log'")
        sys.exit(1)

    # Send command to servers
    server_list = sys.argv[1]
    command = sys.argv[2]
    send_requests(server_list, command)

    # Record the end time
    end_time = time.perf_counter()

    # Calculate and print the elapsed time
    print(f"\nElapsed time: {end_time - start_time} seconds")