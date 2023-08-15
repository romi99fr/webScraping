import subprocess

# Transfer data to HDFS using subprocess
transfer_command = f"../../hadoop-2.7.4/bin/hdfs dfs -put ../data/data3.json webScraping"
subprocess.run(transfer_command, shell=True)
