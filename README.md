# Local Hadoop ecosystem project

This project is created for learning purposes. 

It provides a way to test Hadoop related tasks in local machine. All components are run in docker environment.

Note that only basic Hadoop components are part of this project.

# Code structure

`cluster/docker-compose.yml` contains services allowing to set up a Hadoop cluster, Hive etc.


## Currently available components

### 1. Hadoop cluster

The docker image used for the Hadoop cluster is built locally from the Dockerfile available in this repository : https://github.com/bigdatafoundation/docker-hadoop/blob/master/3.3.6/Dockerfile

This image installs Java 8, Hadoop 3.3.6 and YARN.

#### 1.1. Namenode


Hadoop Namenode is responsible for managing a set of data nodes. Namenode does not contain actual data. It stores metadata.

* In the `docker-compose` file the Namenode is created in the `namenode` service.
* Access the Namenode UI : http://localhost:9870/

    ![Alt text](doc/namenode-ui.png)


#### 1.2. Datanode

Responsible of storing actual data. A Hadoop cluster can have multiple datanodes. If a datanode goes down then it will not affect the Hadoop cluster due to replication.

* In the `docker-compose` file, two Datanodes are created. `datanode-1` and `datanode-2` services.
* Access the Datanode UI : 
    * datanode-1 : http://localhost:9864
    * datanode-2 : http://localhost:9865

    ![Alt text](doc/datanode-ui.png)


#### 1.3. HDFS (Hadoop Distributed file system)

This is the file system of a Hadoop cluster. When a file is loaded into HDFS, it is actually split to multiple blocks and each block will be stored in multiple data nodes.
This will enable fault tolerance.

* To interact with HDFS, multiple options are there : 
    * By entering the Namenode container :
        * `docker exec -it [namenode_container_id] bash`
        * Run HDFS commands : e.g. `hadoop fs -ls /`
    * By creating another hadoop client container and connecting it to the Namenode ...

* It is also possible to interact with HDFS using the Namenode UI : http://localhost:9870/explorer.html
    ![alt text](doc/hdfs-exporer-ui.png)

#### 1.4. YARN (Yet another resource negotiator)

Manage the Hadoop cluster resources, schedule compute resources, allocate resources for jobs execution etc. 

* In the `docker-compose` file, YARN is part of both Namenode and Datanode services. Yarn Resource Manager is run in the Namenode, and YARN Node Manager is run in the Datanodes.
* Access YARN from the UI : http://localhost:8088/

    ![Alt text](doc/yarn-ui.png)

### 2. Apache Hive
Apache hive is a distributed data warehouse system. https://hive.apache.org/

In this project, Hive is created on top of the Hadoop cluster explained above. It can also work on top of other distributed systems like S3.
Hive only manages Tables structures and metadata, actual data is saved in HDFS.

Hive provides HiveQL as a SQL-like query tool. It executes MapReduce jobs behind the scenes.

* In the `docker-compose` file, three services are created for managing the Hive local stack : 
    * hive-server service : Responsible for creating the Hive Server 2 to enable running queries against Hive. It interacts with the Hive metastore behind the scenes.
    * hive-metastore service : The Hive metastore manages and stores the structure of tables/ columns etc. in the data warehouse. It serializes and deserializes data from HDFS.
    * hive-metastore-db : A Postgresql database used by Hive Metastore in order to stor metadata.

* Hive server and Hive metastore services are created using the official apache Hive Docker image. https://hub.docker.com/r/apache/hive

* Access Hive Server UI : http://localhost:10002

    ![alt text](doc/hive-server-ui.png?)

* Interact with hive :
    * Connect to Hive : 
        * `docker exec -it cluster-hive-server-1 beeline -u 'jdbc:hive2://localhost:10000/'`
    * Create an example table on top of HDFS:
        * `create table users(id string, name string, age int);`
    * Create an external table connected to HDFS : 
        ````
            CREATE EXTERNAL TABLE IF NOT EXISTS USERS(
                ID INT,
                NAME STRING,
                AGE INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            location 'hdfs://namenode:9000/warehouse';
        ```
    * Load the created table from a csv file in HDFS
        * `LOAD DATA INPATH 'hdfs://namenode:9000/users.csv' OVERWRITE INTO TABLE users;`
    * Query the table :
        * `SELECT * FROM users;`
        ![alt text](doc/hive-users-query.png)

# TODO

* [code-structure] split the main docker-compose.yml file to multiple service specific files. (One file for Hadoop cluster, one file for Hive etc.)
* [design] add the project technical architecture design of different services and components.
* [configuration] ability to update Hadoop cluster default settings (namenode, datanode and YARN) using a local volume.
 