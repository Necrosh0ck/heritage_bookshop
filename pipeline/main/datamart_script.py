############################################################
# Script Name:     datamart_script.py
# Ticket No.:      XXXX
#
# Purpose:
#   Boilerplate script for creation of datamart tables in
#   BI layer.
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
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, sha2, col, lit, concat, substring
import sys
from datetime import datetime

############################################################
# Parse command-line arguments
############################################################
parser = argparse.ArgumentParser(description="Spark Datamart")
parser.add_argument("--db_connection", required=True, help="Path to db_connection.json")
parser.add_argument("--datamart_config", required=True, help="Path to datamart_config.json")
parser.add_argument("--log_file", required=True, help="Path to log file")
args = parser.parse_args()

############################################################
# Initialize Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("DatamartIngest") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8") \
    .getOrCreate()

############################################################
# Load DB connection and datamart config
############################################################
with open(args.db_connection, "r") as f:
    db_conn = json.load(f)

with open(args.datamart_config, "r") as f:
    datamarts = json.load(f)

# JDBC properties
source_jdbc = db_conn["target"]     # read from staging
datamart_jdbc = db_conn["datamart"] # write to datamart

source_properties = {
    "user": source_jdbc["user"],
    "password": source_jdbc["password"],
    "driver": source_jdbc["driver"]
}

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
# PII masking helper
############################################################
def mask_pii(df, mask_columns):
    """
    Masks PII columns in the DataFrame.
    
    mask_columns: dict of {column_name: mask_type} 
        mask_type can be 'hash', 'fixed', 'partial'
    """
    for col_name, mask_type in mask_columns.items():
        if col_name not in df.columns:
            continue
        if mask_type.lower() == "hash":
            df = df.withColumn(col_name, sha2(col(col_name).cast("string"), 256))
        elif mask_type.lower() == "fixed":
            df = df.withColumn(col_name, lit("XXX"))
        elif mask_type.lower() == "partial":
            df = df.withColumn(col_name, concat(substring(col(col_name).cast("string"), 1, 1), lit("*****")))
        else:
            log(f"Unknown mask type '{mask_type}' for column '{col_name}'", level="ERROR")
    return df

############################################################
# Datamart ingestion loop
############################################################
for dm in datamarts:
    target_table = dm["target_table"]
    sql_query = dm["sql"]
    mask_columns = dm.get("mask_columns", {})

    log(f"Creating/Overwriting datamart table: {target_table}")

    try:
        # Read from source (staging) using SQL query
        df = spark.read.jdbc(
            url=source_jdbc["host"],
            table=f"({sql_query}) AS src",
            properties=source_properties
        )

        # Apply PII masking if defined
        if mask_columns:
            df = mask_pii(df, mask_columns)

        # Add load_timestamp column
        df = df.withColumn("load_timestamp", current_timestamp())

        # Write to datamart with overwrite mode
        df.write.jdbc(
            url=datamart_jdbc["host"],
            table=target_table,
            mode="overwrite",
            properties=datamart_properties
        )

        log(f"Finished writing {target_table} | Rows: {df.count()}")
    except Exception as e:
        log(f"Error processing {target_table}: {str(e)}", level="ERROR")

############################################################
# Stop Spark
############################################################
spark.stop()
