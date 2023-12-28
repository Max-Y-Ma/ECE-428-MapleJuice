# Script Usage:
# 1.) SSH into all VMs in the cluster
# 2.) Run a specific command on all VMs
# 3.) Use maxma2 for first argument

#! /bin/bash

# Clone and Manage Code for VMs 1 through 9
for i in {1..9}
do
ssh $1@fa23-cs425-230$i.cs.illinois.edu /bin/bash << EOF
    rm cs425_mp3_team23/output.log

    echo "Completed clear for VM #230$i"
EOF
done

# Clone and Manage Code for VM 10
ssh $1@fa23-cs425-2310.cs.illinois.edu /bin/bash << EOF
    rm cs425_mp3_team23/output.log

    echo "Completed clear for VM #2310"
EOF
