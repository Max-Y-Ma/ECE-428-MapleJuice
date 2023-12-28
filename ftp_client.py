import os
import socket
import ftplib
from ftplib import FTP

# The port the FTP server will listen on
FTP_PORT = 18251

# FTP login information
FTP_USER = "user"
FTP_PASSWORD = "password"

# The directory the FTP user will have full read/write access to.
FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"

# FTP class object
ftp = FTP()
ftp.connect(socket.gethostbyname("fa23-cs425-2310.cs.illinois.edu"), FTP_PORT)
ftp.login(FTP_USER, FTP_PASSWORD)

# Open and send the file:
with open("/home/maxma2/Documents/Coursework/cs425/cs425_mp3_team23/data/local.txt", 'rb') as file:
    ftp.storbinary(f'STOR newfilename', file)

with open("/home/maxma2/Documents/Coursework/cs425/cs425_mp3_team23/data/10KiB.txt", 'rb') as file:
    ftp.storbinary(f'STOR newfilename', file)

with open("/home/maxma2/Documents/Coursework/cs425/cs425_mp3_team23/data/local.txt", 'rb') as file:
    ftp.storbinary(f'STOR newfilename2', file)

# Close the FTP connection
ftp.close()