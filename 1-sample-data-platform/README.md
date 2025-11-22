# Description

This is a sample data platform for ingesting, transforming and analyzing rail data. 

Data used in this project is from ['Open Rail Data Wiki'](https://wiki.openraildata.com/index.php/Main_Page) which is an open data source available from the rail industry in Great Britain. 

# Sample data platform components

- **Data lake** : HDFS
- **Data warehouse** : Apache Hive
- **ETL** : Apache PySpark
- **Data Visualization** : Apache Superset
- **Orchestration** : Prefect

# Data lake management

For this sample data platform, an ETL (Extract, Transform, Load) approach is used. 

To achieve this :
1. Data is first loaded from data sources to a landing zone "Bronze layer". No transformation in this stage, only adding some metadata like the ingestion date time.
2. Data is then deduplicated, cleaned and transformed into multidimensional star format in a staging zone "Silver layer".
3. Finally, data is structured in "Gold layer" for Analytics and for answering Business questions.

Notes : 
* Data in all of the above steps resides in HDFS which is used as a data lake for this sample data platform project.
* PySpark is used for creating the ETL pipelines.

# Data warehouse

Multidimensional Silver tables and Gold analytics tables are created and exposed through Hive Data warehouse.

# HDFS files

e.g. HDFS Silver files

![alt text](doc/hdfs-silver-files.png)

# Hive queries

![alt text](doc/hive-queries.png)

# YARN application

![alt text](doc/yarn-applications.png)

# Superset Dashboard

![alt text](doc/superset-dashboard.png)

![alt text](doc/supersert-chart.png)


# Prefect

Daily Prefect deployment run

![alt text](doc/prefect-daily-run.png)

# Technical Architecture

![alt text](doc/sample-dp-technical-architecture.png)

