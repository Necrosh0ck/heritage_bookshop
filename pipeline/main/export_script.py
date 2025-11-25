############################################################
# Script Name:     export_script.py
# Ticket No.:      XXXX
#
# Purpose:
#   Boilerplate script for export of tables to csv.
#
# Author:          Mark Diamse
# Date Created:    2025-11-21
#
# Revision History:
# ------------------------------------------------------------------
# Date          Ticket No.     Author         Description
# ------------------------------------------------------------------
#
############################################################

import json
import argparse
import os
from datetime import datetime
from pyspark.sql import SparkSession
import sys

############################################################
# Parse command-line arguments
############################################################
parser = argparse.ArgumentParser(description="Spark Datamart Table Export")
parser.add_argument("--db_connection", required=True, help="Path to db_connection.json")
parser.add_argument("--export_tables", required=True, help="Path to JSON file containing list of tables to export")
parser.add_argument("--output_dir", required=True, help="Folder path to save exported CSVs")
parser.add_argument("--log_file", required=True, help="Path to log file")
args = parser.parse_args()

############################################################
# Initialize Spark session
############################################################
spark = SparkSession.builder \
    .appName("DatamartExport") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8") \
    .getOrCreate()

############################################################
# Load DB connection and tables list
############################################################
with open(args.db_connection, "r") as f:
    db_conn = json.load(f)

with open(args.export_tables, "r") as f:
    tables_to_export = json.load(f)

# JDBC properties
datamart_jdbc = db_conn["datamart"]

datamart_properties = {
    "user": datamart_jdbc["user"],
    "password": datamart_jdbc["password"],
    "driver": datamart_jdbc["driver"]
}

############################################################
# Logging helper
############################################################
def log(msg, level="INFO"):
    """Logs messages to both stdout and a file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"{timestamp} {level}: {msg}"
    print(log_msg)
    try:
        with open(args.log_file, "a") as f:
            f.write(log_msg + "\n")
    except Exception as e:
        print(f"ERROR: Failed to write log file: {str(e)}", file=sys.stderr)

############################################################
# Export loop
############################################################
for table_name in tables_to_export:
    datenow = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table_name = table_name.replace(".", "_")
    output_path = os.path.join(args.output_dir, f"{safe_table_name}_{datenow}")
    
    log(f"Exporting table {table_name} to {output_path}")
    
    try:
        # Read table from datamart
        df = spark.read.jdbc(
            url=datamart_jdbc["host"],
            table=table_name,
            properties=datamart_properties
        )
        
        # Save as CSV
        df.write.mode("overwrite").option("header", "true").csv(output_path)
        log(f"Finished exporting {table_name} | Rows: {df.count()}")
    except Exception as e:
        log(f"Error exporting {table_name}: {str(e)}", level="ERROR")

############################################################
# Stop Spark
############################################################
spark.stop()
log("Datamart export completed.")
