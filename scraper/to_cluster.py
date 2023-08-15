import subprocess

# Path to the local data directory
local_data_path = 'data/data3.json'

# HDFS directory where you want to store the data
hdfs_directory = 'hadoop-2.7.4/bin/hdfs'

# Transfer data to HDFS using subprocess
transfer_command = f"hadoop-2.7.4/bin/hdfs dfs -put {local_data_path} {hdfs_directory}"
subprocess.run(transfer_command, shell=True)