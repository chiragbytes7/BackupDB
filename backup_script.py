import pymysql
import json
import argparse
import subprocess
import compression
import os

parser = argparse.ArgumentParser(description="MySQL Backup Script")
parser.add_argument("--sourcedb", required=True, help="Source Database Name")
parser.add_argument("--destinationdb", required=True, help="Destination Database Name")
parser.add_argument("--source-ip", required=True, help="MySQL Source IP")
parser.add_argument("--destination-ip", required=True, help="MySQL Destination IP")
parser.add_argument("--source-port", type=int, default=3306, help="MySQL Source Port")
parser.add_argument("--destination-port", type=int, default=3306, help="MySQL Destination Port")
parser.add_argument("--source-user", required=True, help="MySQL Source User")
parser.add_argument("--source-pass", required=True, help="MySQL Source Password")
parser.add_argument("--destination-user", required=True, help="MySQL Destination User")
parser.add_argument("--destination-pass", required=True, help="MySQL Destination Password")
parser.add_argument("--backup-type" , required=True, help="Backup Type (Full, Incremental, Differential)")
parser.add_argument("--compression", required=True, help="Compression Type (Zstd, Snappy)")

args = parser.parse_args()
print(args.source_user)

# Corrected subprocess command
import subprocess

if args.sourcedb == "mysql":
    subprocess.run([
        "xtrabackup", "--backup",
        "--target-dir=/home/chirag/backup/first_backup",
        "--user=" + args.source_user,
        "--password=" + args.source_pass,
        "--host=" + args.source_ip,
        "--socket=/var/run/mysqld/mysqld.sock"
    ], check=True)

    input_dir = "/home/chirag/backup/first_backup"
    output_dir = "/home/chirag/compressedbackups"

    compressedfilemysql = compression.compressmysql(input_dir, output_dir, 3)
    print(compressedfilemysql)

    result = subprocess.run(
        ["/home/hdoop/hadoop-3.2.4/bin/hdfs", "dfsadmin", "-report"], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True
    )

    if "Live datanodes" in result.stdout:
        subprocess.run(
            ["/home/hdoop/hadoop-3.2.4/bin/hdfs", "dfs", "-put", compressedfile, "/backup/"], 
            check=True
        )
    else:
        print("Error connecting to HDFS")

else:
    MONGO_HOST = args.source_ip
    MONGO_PORT = str(args.source_port)
    CUSTOM_BACKUP_PATH = "/home/chirag/mongobackup"

# Ensure the backup directory exists
    os.makedirs(CUSTOM_BACKUP_PATH, exist_ok=True)

# Backup folder inside custom path
    backup_dir = os.path.join(CUSTOM_BACKUP_PATH, "first_mongo_backup")

# Construct the mongodump command (backup all databases)
    dump_command = [
    "mongodump",
    "--host", MONGO_HOST,
    "--port", MONGO_PORT,  # ✅ Port converted to string
    "--out", backup_dir
    ]
    
    try:
        subprocess.run(dump_command, check=True)
        print(f"Backup successful! Data saved in {backup_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error during backup: {e}")

    #now put the compression logic here 

    compressedfileformongo = compressedfileformongo()

























