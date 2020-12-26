###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *
#Esto es para cuando vaya a debuggear pueda luego apagar los print y el codigo quede intacto :)
debug = 1



def usage():
	print ("""Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0]))
	sys.exit(0)
#_______________________________________________________________________________________________________________________
def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server
	sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	sock.connect(address)
	if debug:
		print (address)

	# Fill code
	# Read file
	file = open(fname, "rb")
	data = file.read(1024)
	#using the file name finds the file size in bytes and return a int value
	fileSize = os.path.getsize(fname)
	if debug:
		print(data)
		print(fileSize)

	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server
	p = Packet()
	p.BuildPutPacket(fname,fileSize)
	sock.sendall(p.getEncodedPacket())

	# If no error or file exists
	#recives the data
	#r is going to be a byte type b'str' and we want the str so
	#we need to turn byte to str and the str.split"'"[1] to get the str
	r = str(sock.recv(1024)).split("'")[1]
	if debug:
		print(r)
	if r == "DUP":
		print("Duplicated File")
		return
	# Get the list of data nodes.
	else:
		p.DecodePacket(r)
		dNode = p.getDataNodes()


	# Divide the file in blocks
		dNodeSize = len(dNode)
		blockSize = int(fileSize/dNodeSize)
		blockList = []
#***********************************************************************************************************************
		for i in range(0, fileSize, blockSize):
			if (i / blockSize) + 1 == dNodeSize:
				blockList.append(data[i:])
				break

			else:
				blockList.append(data[i:i + blockSize])
	sock.close()
#***********************************************************************************************************************
	for i in dNode:
		# Connecto to the data node
		sockdn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sockdn.connect((i[0], i[1]))

		# Create a packet object to put data
		p.BuildPutPacket(fname, fileSize)
		sockdn.sendall(p.getEncodedPacket())
		r = sockdn.recv(1024)

		# Take the data block that will be send to data node
		bdata = blockList.pop(0)

		if r == "OK":
			size = len(bdata)

			sockdn.sendall(bytes(size))
			r = sockdn.recv(1024)

			# Send the data block into 1024 size parts
			while len(data) > 0:
				chunk = data[0:1024]
				data = data[1024:]

				sockdn.sendall(chunk)
				r = sockdn.recv(1024)

			# Adding the chunk id to the data nodes list
			sockdn.sendall(bytes("OK"))
			r = sockdn.recv(1024)
			i.append(r)

		sockdn.close()

	# Connect to the meta data
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(address)

	# Create a packet object to register data blocks into meta data
	p.BuildDataBlockPacket(fname, dNode)

	sock.sendall(p.getEncodedPacket())
	sock.close()


#***********************************************************************************************************************
# Send the blocks to the data servers

	# Fill code	

	# Notify the metadata server where the blocks are saved.

	# Fill code

	file.close()

def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

	# Contact the metadata server to ask for information of fname
	sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	sock.connect(address)
	# Fill code
#***********************************************************************************************************************
	p = Packet()
	p.BuildGetPacket(fname)
	sock.sendall(p.getEncodedPacket())


	# If there is no error response, retreive the data blocks
	r = sock.recv(1024)
	p.DecodePacket(r)
	dnList = p.getDataNodes()


	# Create file to store data from blocks
	f = open(path, 'wb')


	# Get data blocks from data servers
	for dnode in dnList:
		# Contact the data node
		sockdn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sockdn.connect((dnode[0], dnode[1]))


		# Create a packet object to get data from data node
		p.BuildGetDataBlockPacket(dnode[2])
		sockdn.sendall(p.getEncodedPacket())

		# Get the data size of the data that will be receive
		dsize = sockdn.recv(1024)
		dsize = int(dsize)

		sockdn.sendall(bytes("OK"))


		# Get data in 1024 size parts
		data = ""
		while(len(data) < dsize):
			r = str(sockdn.recv(1024)).split("'")[1]
			data = data + r
			sockdn.sendall(bytes("OK"))

		# Write data to file
		f.write(data)
		sockdn.close()


	f.close()
#***********************************************************************************************************************
	# If there is no error response Retreive the data blocks

	# Fill code

    # Save the file
	
	# Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print ("Error: path %s is a directory.  Please name the file." % to_path)
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print("Error: path %s is a directory.  Please name the file." % from_path)
			usage()

		copyToDFS((ip, port), to_path, from_path)


