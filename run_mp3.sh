# Script Usage:
# 1.) SSH into all VMs in the cluster
# 2.) Run the appropriate server code as background process
# 3.) Use maxma2 for first argument

#! /bin/bash
directory_name="cs425_mp3_team23"


# Run node code for VMs 1 through 9
for i in {1..9}
do
ssh $1@fa23-cs425-230$i.cs.illinois.edu /bin/bash << EOF
    nohup python3 /home/$1/$directory_name/sdfs.py > /home/$1/mp3_node.log 2>&1 &

    echo "Running server for VM #230$i"
EOF
done

# Run node code for VM 10
ssh $1@fa23-cs425-2310.cs.illinois.edu /bin/bash << EOF
    nohup python3 /home/$1/$directory_name/sdfs.py > /home/$1/mp3_node.log 2>&1 &

    echo "Running server for VM #2310"
EOF