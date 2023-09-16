### How To Run

1. Run the following: `start.sh` bash by `./start.sh`
2. Login Airflow: Navigate `localhost:9091` airflow;airflow
3. Login Hue (To watch and query HDFS data): Navigate to `localhost:32762` root;root

### Flow Chart of the Forex Data Pipeline

1. **Check Availability**
    - [ ] Forex Rates

2. **File Availability**
    - [ ] File containing currencies to watch

3. **Data Acquisition**
    - [ ] Download forex rates using Python

4. **Data Storage**
    - [ ] Save forex rates in HDFS

5. **Data Structuring**
    - [ ] Create a Hive table to store forex rates from HDFS

6. **Data Processing**
    - [ ] Process forex rates with Spark

7. **Notifications**
    - [ ] Send an Email Notification
    - [ ] Send a Slack Notification

