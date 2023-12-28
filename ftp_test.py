import os
import socket
import pyftpdlib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# The port the FTP server will listen on
FTP_PORT = 18251

# FTP login information
FTP_USER = "user"
FTP_PASSWORD = "password"

# The directory the FTP user will have full read/write access to.
FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"

# Defines the hostname of the local machine
localhostname = socket.gethostname()

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
print("Running FTP Server!")
server.serve_forever()
