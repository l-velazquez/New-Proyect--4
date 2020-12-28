Assignment 04: Distributed File Systems
Programmer: Luis Fernando Javier Velázquez Sosa

University of Puerto Rico at Rio Piedras
Department of Computer Science
CCOM 4017: Operating Systems
Profesor: Jose R. Ortix Ubarri


    Discription:
    These programs are to learn how to implement the main components of a file system by using the distributed file
    system (DFS). This program will use the programming language Python.

    Metadata server: will function as an inodes repository
    Data server: will be the disk space for file data blocks
    list client: list the files available in the DSF
    copy client: will copy files from and to the DSF

    How to run a functional Distributed File System:

    I. Create the Data Base for the data of the DFS
        1. In the terminal* go to were all the DFS files are
        2. Write:
                    python createdb.py
    THE SERVERS
        II. Run metadata server
            1. Now having created the data base open another terminal window
            2.If not in the DFS directory, enter the directory again in the terminal
            3. If you are already in the directory write this:
                python meta-data.py <port, default=8000>
                (if no port is chosen the default is 8000)
        III. Run data node server
            1. Open another terminal window
            2. If not in the DFS directoty, enter the directory again in the terminal
            3.If you are already in the directory write this:
                python data-node.py <server address> <port> <data path> <metadata port,default=8000>

            server adress: is the metadata server address
            port: is the data-node port number
            data path: is a path to a directory to store the data blocks
            metadata port: is the optional metadataport if it was run  in a different port other than the default port

     THE CLIENTS
       IV. The list client
            1. Open another terminal window
            2. If not in the DFS directory, enter the directory again in the terminal
            3. If you are already in the directory write this:
                python ls.py <server>:<port, default=8000>

                server: is the metadata server IP
                port: metadata server port, if the default port is the indicated the default port is 8000
                The ":" character is necessary

       V. The copy client
            1.If it your first time running the program, we need to copy files
              to the empty by opening another terminal window
            2.If not in the DFS directory, enter the directory again in the terminal
            3.If you are already in the directory write this:
                python copy.py <source file> <server>:<port>:<dfs file path>

                source file: is the file name
                server: is the metadata server IP address
                port: is the metadata server port
                dfs file path: is the path were you copied file will be located

            If you have files in the DFS, you can copy them to your computer by:
            1. Open another terminal window
            2. If not in the DFS directory, enter to the DFS directory
            3. If you are already in the directory write this:
                python copy.py <server>:<port>:<dfs file path> <destination file>

                server: is the localhost
                port: is the metadata server port
                dfs file path: is were the file in the DFS is located
                destination file: is the path were the file from the DFS will be located in your computer


    *Any time terminal is mentioned this means command prompt for windows users

    Resources used:
    What is python slicing?
    https://rb.gy/egikb6
        This reference was used to e able to see the use of slicing and it being used to be able to send multiple
        elements of the array instead of doing a for loop going through each element.


    Update: I did some changes and used some of the information in
    this website https://stackoverflow.com/questions/33003498/typeerror-a-bytes-like-object-is-required-not-str
    to solve the problem

    Socket Tutorial
    https://youtu.be/Lbfe3-v7yE0
    This was to help me visualize and understand it better to know what was going on, it
    was more like a review.

    Socket Server
    https://docs.python.org/3.8/library/socketserver.html?highlight=socketserver#module-socketserver
    To understand the way it functions I read parts of the documentations.

   Fixing the has_key problem in the Packet.py
    https://stackoverflow.com/questions/13976060/parsing-json-in-python-how-do-i-get-has-key-working-again-after-a-change-of-f
    https://portingguide.readthedocs.io/en/latest/dicts.html