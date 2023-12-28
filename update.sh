# Script Usage:
# 1.) SSH into all VMs in the cluster
# 2.) Pull and build latest code 
# 3.) Use maxma2 for first argument

#! /bin/bash
directory_name="cs425_mp3_team23"
github_repo_url="git@gitlab.engr.illinois.edu:maxma2/cs425_mp3_team23.git"

# Clone and Manage Code for VMs 1 through 9
for i in {1..9}
do
ssh $1@fa23-cs425-230$i.cs.illinois.edu /bin/bash << EOF
    cd /home/$1

    if [ -d "$directory_name" ]; then
        echo "Directory '$directory_name' already exists."
    else
        git clone "$github_repo_url" "$directory_name"

        if [ $? -eq 0 ]; then
            echo "Cloning successful. Directory '$directory_name' created."
        else
            echo "Cloning failed. Please check the repository URL or your network connection."
            continue
        fi
    fi

    cd /home/$1/$directory_name
    git checkout main
    git pull

    pip3 install -r requirements.txt

    echo "Completed update for VM #230$i"
EOF
done

# Clone and Manage Code for VM 10
ssh $1@fa23-cs425-2310.cs.illinois.edu /bin/bash << EOF
    cd /home/$1

    if [ -d "$directory_name" ]; then
        echo "Directory '$directory_name' already exists."
    else
        git clone "$github_repo_url" "$directory_name"

        if [ $? -eq 0 ]; then
            echo "Cloning successful. Directory '$directory_name' created."
        else
            echo "Cloning failed. Please check the repository URL or your network connection."
        fi
    fi

    cd /home/$1/$directory_name
    git checkout main
    git pull

    pip3 install -r requirements.txt

    echo "Completed update for VM #2310"
EOF
