############################################################
# Script Name:     batch_ingest.py
# Ticket No.:      XXXX
#
# Purpose:
#   Boilerplate script for batch ingestion of data into the
#   staging layer.
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
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp

########################################################
# Parse command-line arguments
########################################################
parser = argparse.ArgumentParser(description="Spark Batch Ingest with logging")
parser.add_argument("--tables_config", required=True, help="Path to tables_config.json")
parser.add_argument("--db_connection", required=True, help="Path to db_connection.json")
parser.add_argument("--driver_file", required=True, help="Path to driver JSON with tables to ingest")
parser.add_argument("--log_file", required=True, help="Path to log file where ingestion logs will be written")
args = parser.parse_args()

LOG_FILE = args.log_file  # required log file

############################################################
# Logging function
############################################################
def log(message_type, message):
    """
    Write log message to file and print to console.

    Args:
        message_type (str): "INFO" or "ERROR"
        message (str): Message to log
    """
    with open(LOG_FILE, "a") as f:
        f.write(f"{message_type}: {message}\n")
    print(f"{message_type}: {message}")

# ---------------------------
# Initialize Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("BatchIngestSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8") \
    .getOrCreate()

############################################################
# Load JSON config files
############################################################
with open(args.tables_config, "r") as f:
    table_configs = json.load(f)

with open(args.db_connection, "r") as f:
    db_conn = json.load(f)

with open(args.driver_file, "r") as f:
    tables_to_ingest = json.load(f)

# Map configs by source_table
config_map = {cfg['source_table']: cfg for cfg in table_configs}

############################################################
# Load JDBC connections
############################################################
source_jdbc = db_conn["source"]
target_jdbc = db_conn["target"]

source_properties = {
    "user": source_jdbc["user"],
    "password": source_jdbc["password"],
    "driver": source_jdbc["driver"]
}

target_properties = {
    "user": target_jdbc["user"],
    "password": target_jdbc["password"],
    "driver": target_jdbc["driver"]
}

############################################################
# Ingest function
############################################################
def ingest_table_sql(config):
    """
    Ingest a single table from source to target PostgreSQL database.

    Args:
        config (dict): Table configuration dictionary with keys:
            - source_schema, source_table
            - target_schema, target_table
            - fields (comma-separated or '*')
            - load_type ('full' or 'delta')
            - delta_column (optional)
            - batch_size (optional)
    """
    source_table = f"{config['source_schema']}.{config['source_table']}" if config.get("source_schema") else config['source_table']
    target_table = f"{config['target_schema']}.{config['target_table']}" if config.get("target_schema") else config['target_table']

    fields = config['fields'] if config['fields'] != "*" else "*"
    load_type = config['load_type'].lower()
    delta_column = config.get('delta_column', None)
    batch_size = config.get("batch_size", 10000)

    log("INFO", f"Starting ingest: {source_table} -> {target_table} | Load: {load_type} | Batch: {batch_size}")

    # Read source table
    df = spark.read.jdbc(
        url=source_jdbc["host"],
        table=source_table,
        properties={**source_properties, "fetchsize": str(batch_size)}
    )

    if fields != "*":
        df = df.select([col(f) for f in fields.split(",")])

    # Add ingest_timestamp
    df = df.withColumn("ingest_timestamp", current_timestamp())

    source_count = df.count()
    log("INFO", f"Source count for {source_table}: {source_count}")

    # Handle delta load
    if load_type == "delta" and delta_column:
        try:
            target_df = spark.read.jdbc(
                url=target_jdbc["host"],
                table=target_table,
                properties={**target_properties, "fetchsize": str(batch_size)}
            )
            max_delta = target_df.agg(spark_max(col(delta_column))).collect()[0][0]
            if max_delta is not None:
                df = df.filter(col(delta_column) > max_delta)
        except Exception as e:
            log("ERROR", f"Target table {target_table} not found. Performing full load instead. Error: {e}")

    # Determine write mode
    write_mode = "overwrite" if load_type == "full" else "append"

    # Write to target PostgreSQL
    df.write.jdbc(
        url=target_jdbc["host"],
        table=target_table,
        mode=write_mode,
        properties=target_properties
    )

    target_df_after = spark.read.jdbc(
        url=target_jdbc["host"],
        table=target_table,
        properties={**target_properties, "fetchsize": str(batch_size)}
    )
    target_count = target_df_after.count()

    log("INFO", f"Target count for {target_table}: {target_count}")
    log("INFO", f"Rows ingested: {target_count - source_count if write_mode=='append' else target_count}")
    log("INFO", f"Finished ingesting {target_table}")

############################################################
# Loop through tables in driver file
############################################################
for table_name in tables_to_ingest:
    if table_name in config_map:
        ingest_table_sql(config_map[table_name])
    else:
        log("ERROR", f"No config found for table {table_name}")

############################################################
# Stop Spark
############################################################
spark.stop()
log("INFO", "Spark session stopped.")
