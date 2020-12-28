###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and Luis Fernando Javier Velazquez Sosa
# Description:
# 	data node server for the DFS
#

from Packet import *
import sys
import socket
import socketserver
import uuid
import os.path

debug = 0


def usage():
    print("""Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0])
    sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
    """Creates a connection with the metadata server and
	   register as data node
	"""

    # Establish connection
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((meta_ip, meta_port))
    # Fill code

    try:
        response = "NAK"
        sp = Packet()
        while response == "NAK":
            sp.BuildRegPacket(data_ip, data_port)
            sock.sendall(sp.getEncodedPacket().encode())
            response = sock.recv(1024)

            if response == "DUP":
                print("Duplicate Registration")

            if response == "NAK":
                print("Registratation ERROR")

    finally:
        sock.close()


class DataNodeTCPHandler(socketserver.BaseRequestHandler):

    def handle_put(self, p):

        """Receives a block of data from a copy client, and
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""

        fname, fsize = p.getFileInfo()
        if debug:
            print("File name:", fname, "and file size", fsize)

        self.request.send("OK")

        # Generates an unique block id.
        blockid = str(uuid.uuid1())

        # Open the file for the new data block.
        file = open(fname + blockid, "rb")

        # Receive the data block.
        bsize = self.request.recv(1024)
        if debug:
            print(bsize)
        self.request.send("OK")

        # Convert the socket response in integers
        block_size = int(bsize)
        if debug:
            print(bsize)
        data = ""

        # Send data in 1024 size parts
        while (len(data) < block_size):
            r = self.request.recv(1024)
            data += r
            self.request.send("OK")

        # Save received data to file
        file.write(data)


        # Send the block id back
        print("Block id:", blockid)

        self.request.sendall(blockid)
        self.request.close()

    # Fill code

    def handle_get(self, p):

        # Get the block id from the packet
        blockid = p.getBlockID()
        fname = p.getFileName()
        # Read the file with the block id data
        file = open(DATA_PATH + blockid, 'rb')
        data = file.read(1024)
        size = os.path.getsize(fname)

        # Send it back to the copy client.
        while (size > 0):
            if debug:
                print("bytes left to send are:", size)
            self.request.sendall(data)
            size = size - 1024
            data = file.read(1024)
        file.close()
        self.request.close()

    # Fill code

    def handle(self):
        msg = self.request.recv(1024)
        print(msg, type(msg))

        p = Packet()
        p.DecodePacket(msg)

        cmd = p.getCommand()
        if cmd == "put":
            self.handle_put(p)

        elif cmd == "get":
            self.handle_get(p)


if __name__ == "__main__":

    META_PORT = 8000
    if len(sys.argv) < 4:
        usage()

    try:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
        DATA_PATH = sys.argv[3]

        if len(sys.argv) > 4:
            META_PORT = int(sys.argv[4])

        if not os.path.isdir(DATA_PATH):
            print("Error: Data path %s is not a directory." % DATA_PATH)
            usage()
    except:
        usage()

    register("localhost", META_PORT, HOST, PORT)
    server = socketserver.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()