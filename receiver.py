import socket

sample_sdfs = ['file2', 'file3']

host = '127.0.0.1'  # Use localhost
port = 18000

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen(1)

print(f"Server is listening on {host}:{port}")

for _ in range(5):
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")

    received_data = client_socket.recv(1024)
    received_string = received_data.decode()

    if "PUT" in received_string:
        print("SDFS before PUT: " + str(sample_sdfs))
        filename = received_string[4:]
        sample_sdfs.append(filename)
        print("Added " + filename + " to SDFS list")
        print("SDFS after PUT: " + str(sample_sdfs))

    elif "GET" in received_string:
        filename = received_string[4:]
        if filename in sample_sdfs:
            print("File " + filename + " found in SDFS list")
        else:
            print("File " + filename + " not found in SDFS list")
    
    elif "DELETE" in received_string:
        print("SDFS before DELETE: " + str(sample_sdfs))
        filename = received_string[7:]
        if filename in sample_sdfs:
            print("File " + filename + " found in SDFS list. Deleting...")
            sample_sdfs.remove(filename)
        else:
            print("File " + filename + " not found in SDFS list. Cannot delete.")
        print("SDFS after DELETE: " + str(sample_sdfs))
    
    else:
        print("Not implemented yet. Come back later!")

    # print(f"Received: {received_string}")

    client_socket.close()
server_socket.close()
