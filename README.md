# MP4: MapleJuice + SQL
MP4 is a parallel programming paradigm called MapleJuice, similar to that of Google's MapReduce. MapleJuice supports
executing user created Maple (Map) and Juice (Reduce) programs. The framework handles the resource sharing and scheduling
of tasks. A SQL Filter and Join program is layered on top of MapleJuice to illustrate its functionality. 

## System Architecture
- MP4 uses all previous MPs in its backend failure detection and DFS components 
- Each node in the cluster can recieve user input jobs, which then get sent to a centralized leader
- Incoming jobs are queued for execution at the centralized leader
- The centralized leader contains a resource manager that schedules the respective maple or juice jobs
  - The scheduler also partitions the input data/files, handles failure re-execution, and output collection
- Barriers are placed between independent maple and juice jobs
- Output files and intermediate files from each maple and juice stage are stored in SDFS

## Programtic Design
- The program implementation is broken into three classes:
  - `MapleJuice` : Handles client job input and queuing
  - `MapleEngine` : Handles maple task partitioning and scheduling
  - `JuiceEngine` : Handles juice task partitioning and scheduling
- Barriers are implemented with counts for the number of completed tasks
  - Output files are replicated in the SDFS at the end of a completed task
- Socket Programming and threading was heavily used to organize the code
- SQL Queries were implemented as internal functions
  - Respective maple and juice jobs were executed internally in threads
- The Maple and Juice programs were defined in seperate python files
  - Program files, stored in `data/bin`, are stored in SDFS and run in each client task

## MapReduce Programs
- This repository also stores the MapReduce Programs used in the Hadoop Cluster
  - View `programs/...` for more details
- The programs were written in Java and contain MapReduce framework code for running YARN jobs

## Usage
Directly run `python3 maplejuice.py` on each VM in the cluster

## Commands
- `maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>` : Maple Program
- `juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> <delete_input={0, 1}>` : Juice Program
- `filter <regex> <num_tasks> <prefix> <src_dir> <dest_filename>` : SQL Filter
- `demo <X> <num_tasks> <prefix> <src_dir> <dest_filename>` : Demo Program
- `join <num_tasks> <prefix> <dest_filename> <D1_dir> <field1> <D2_dir> <field2>` : SQL Join
- `jobs`: print in queue 
- `list_maple`: print maple partitions 
- `list_keys`: print key dictionary 
- `list_outfile`: print outfile dict 
- `sdfs`: sdfs input mode 

# MP3: Simple Distributed File System
MP3 is a distributed file system that supports simple replication, failure detection, and bandwidth in the hundreds of MB/s. It uses MP2 to support
the failure detection and MP1 for debugging. SDFS also support RW synchronization and up to 3 simultaneous failures.

## System Architecture
- Each node in the cluster implements an FTP server to allow the leader to read, write, and delete files. 
- The `/data` directory holds the current working directory for each of the FTP servers.
- A full membership list is held at every node using a gossip-style protocol.
- R/W synchronization is implemented at the leader.
  - Writer have to acquire 1 write mutex, and 2 read semaphores
  - Readers have to acquire 1 read semaphore
  - This enforce up to 2 reads and 1 write. Thus, R/W and W/W cannot occur
- Written files are replicated to 4 nodes, supporting 3 simulataneous failures 
- Leader election is started on initialization

## Programtic Design
- SDFS implements multithreading to provide an asynchronous programming model
  - This allows for simultaneous R/W execution on a system that supports a multi-execution workflow
- SDFS incorporates the FTP server as a seperate process, allowing for single program execution. 
  - This makes execution of SDFS as a compact `sdfs.py` file.
- Fileops are also implemented using multithreading for asynchronous execution.

## Usage
Directly run `python3 sdfs.py` on each VM in the cluster

## Commands
- `put <localfilename> <sdfsfilename>` : writes a file to the SDFS
- `get <sdfsfilename> <localfilename>` : reads a file from the SDFS
- `delete <sdfsfilename>` : removes a file from the SDFS
- `ls <sdfsfilename>` : lists the location of the file in the SDFS
- `store` : lists the current files stored on the node in the SDFS

# MP2: Gossip-style failure detector
MP2 acts as the underlying failure detection system for SDFS, which maintains a Gossip, heartbeating-style membership list. 
This system support two types of failure detection: Gossip mode and Gossip + S mode. Gossip without suspicion is the primary mode of failure detection.

## Parameters
- `protocol-time`: The interval time for sending gossip message to other machines. With less protocol time, the machine will send message to other machines more frequently. The default time is 0.25s.
- `drop-rate`: The probability of dropping the receiving message to introduce the false positive rate

## Usage
Directly run `python3 gossip.py (-t protocol_time -d drop_rate)` on each VM in the cluster

## Commands
- `list_mem`: print out the info of membership list, the membership list contains
  - host ID
  - heartbeat
  - time stamp
  - status
- `list_self`: print out host ID
- `join` and `leave`: After typing leave, the virtual machine will temporarily leave from the group. Then type join to rejoin in the group.
- `enable_gossip` and `disable_gossip`: change to the GOSSIP+S mode and change back to GOSSIP mode

### Example output
```
ID: fa23-cs425-8002.cs.illinois.edu:12345:1695523506, Heartbeat: 26, Status: Alive, Time: 1695523538.162461
ID: fa23-cs425-8004.cs.illinois.edu:12345:1695523523, Heartbeat: 63, Status: Alive, Time: 1695523539.1641357
ID: fa23-cs425-8005.cs.illinois.edu:12345:1695523524, Heartbeat: 59, Status: Alive, Time: 1695523539.164185
ID: fa23-cs425-8006.cs.illinois.edu:12345:1695523525, Heartbeat: 54, Status: Alive, Time: 1695523539.1641378
ID: fa23-cs425-8007.cs.illinois.edu:12345:1695523526, Heartbeat: 50, Status: Alive, Time: 1695523539.164137
ID: fa23-cs425-8008.cs.illinois.edu:12345:1695523527, Heartbeat: 46, Status: Alive, Time: 1695523539.1641614
ID: fa23-cs425-8009.cs.illinois.edu:12345:1695523529, Heartbeat: 40, Status: Alive, Time: 1695523539.1641872
ID: fa23-cs425-8010.cs.illinois.edu:12345:1695523533, Heartbeat: 24, Status: Alive, Time: 1695523539.1641393
```

# MP1: Distributed-Log Querier
MP1 uses an asynchronous client to query remote servers on each VM with `grep` commands. This is primarily used to debug the
SDFS program. The SDFS program is multithreaded and has multiprocess execution, making other methods of debugging very difficult.

## Usage
Directly run `./run_mp1.sh`. Upon finishing the script, execute the client program `python3 ./log_c.py <machine_list> <grep_command>`
Example: `python3 log_c.py "9" "grep '[INFO]' /home/maxma2/cs425_mp3_team23/output.log"`
Example: `python3 log_c.py "all" "grep ' ' /home/maxma2/cs425_mp3_team23/output.log"`
