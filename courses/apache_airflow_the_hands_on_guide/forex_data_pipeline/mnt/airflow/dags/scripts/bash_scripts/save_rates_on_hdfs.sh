#!/bin/bash

# Create the directory '/forex' in HDFS if it doesn't exist
hdfs dfs -mkdir -p /forex

# Copy the 'forex_rates.json' file from AIRFLOW_HOME to the '/forex' directory in HDFS
hdfs dfs -put -f "$AIRFLOW_HOME"/dags/files/forex_rates.json /forex