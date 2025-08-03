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
1. Data is first loaded from data sources to a landing zone that we call "Bronze layer". No transformation in this stage, only adding some metadata like the ingestion date time.
2. Data is then deduplicated, cleaned and transformed in a staging zone that we call "Silver layer".
3. Finally, data is structured in "Gold layer" for Analytics and for answering Business questions.

Notes : 
* Data in all of the above steps resides in HDFS which is used as a data lake for this sample data platform project.
* PySpark is used for creating the ETL pipelines.

# Data warehouse

Data from the Gold layer is loaded to Hive.
