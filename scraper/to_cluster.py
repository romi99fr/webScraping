import subprocess


mkdir_command = "../../hadoop-2.7.4/bin/hdfs dfs -mkdir -p webScraping"
subprocess.run(mkdir_command, shell=True)

# Transfer data to HDFS using subprocess
transfer_command = f"../../hadoop-2.7.4/bin/hdfs dfs -put ../data/data.json webScraping/data.json"
subprocess.run(transfer_command, shell=True)

