import os
import socket
import subprocess
import threading

def init_server():
    """
    Initializes the server for distributed grep.
    Opens a socket on the host machine on port 18000.

    Params: N/A

    Returns:
        server_socket : Server socket descriptor
    """

    # Initializing server TCP socket
    server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    # Allow reusing the same address
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Grab hostname from file and 
    home_dir = os.environ.get('HOME')
    file_path = f"{home_dir}/hostname.txt"
    with open(file_path, "r") as file:
        host_name = file.read().rstrip('\n')

    # Resolve to ip address
    ip_addr = socket.gethostbyname(host_name)

    # Bind server socket to host machine with port 18000
    port = 18000
    server_socket.bind((ip_addr, port))

    # Set maximum incoming connects to 10
    server_socket.listen(10)
    print(f"Server Listening at {ip_addr} on port {port}")

    return server_socket

def process_request(client_socket):   
    """
    Thread function to handle client request and response.

    Parameters:
        client_socket : Client socket file descriptor

    Returns: N/A
    """   

    # Grab message from client socket 
    buffer_length = 1024
    client_msg = client_socket.recv(buffer_length).decode()

    # Execute grep command from the client
    process = subprocess.Popen(client_msg, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    grep_result = stdout.decode()

    # Send response message
    client_socket.send(grep_result.encode())
    client_socket.close()


if __name__ == "__main__":
    # Initialize server
    server_socket = init_server()

    # Main thread
    while True:
        # Wait for client connection
        client_socket, client_address = server_socket.accept()
        print(f"Connection established from {client_address}")

        # Start separate message processing thread 
        process_thread = threading.Thread(target=process_request, args=(client_socket,))
        process_thread.start()