# Stream Processing : Real-time Click Attribution and Dynamic E-commerce Insights

## Table of Contents
1. [Introduction](#1-introduction)
2. [Architecture](#2-architecture)
3. [Design](#3-design)
   - a. [State & Watermarking](#a-state--watermarking)
   - b. [Data Retention](#b-data-retention)
   - c. [Running Jobs in Parallel](#c-running-jobs-in-parallel)
4. [Core Concepts](#4-core-concepts)
   - a. [State & Watermarking (with illustration)](#a-state--watermarking-with-illustration)
   - b. [Data Retention (with SQL script)](#b-data-retention-with-sql-script)
   - c. [Running Jobs in Parallel (Spark configuration)](#c-running-jobs-in-parallel-spark-configuration)
5. [Project Structure](#5-project-structure)
   - [Directory structure explanation](#directory-structure-explanation)
   - [Explanation of each component's role](#explanation-of-each-components-role)
6. [Settings](#6-settings)
   - [Docker](#docker)
   - [Running the application](#running-the-application)
7. [Visualization](#7-visualization)
   - [Connecting PostgreSQL to Grafana](#connecting-postgresql-to-grafana)
   - [Importing and configuring the dashboard](#importing-and-configuring-the-dashboard)
   - [Visualizing real-time data in Grafana](#visualizing-real-time-data-in-grafana)





## 1. Introduction 
An E-commerce website handles massive amount of requests generated every day, the company is interested in identifying which click lead to a checkout, allowing them to assess marketing effectiveness. Furthermore, the company also demands for near real-time insights into business performance as well.

We will use First Click Attribution within the last 10 minutes.

### Technology uses :
- Java (with Maven)
- Kafka 
- Spark (Structured Streaming)
- PostgreSQL 
- Grafana (visualization)
- Docker 

## 2. Architecture
The diagram illustrate the conceptual view of the streaming pipeline. Data will be generated and sent to kafka topics, Spark retrieves them, perform operations (enrich, join, filter, re-calculate, aggregation) and forward them to postgres sink, finally Grafana will pull processed aggregate data for near real-time visualization.

<div style="display: flex; flex-direction: column;">
<img src=/Assets/brief_architecture.png alt = "brief_architecture">

<p style="text-align: center;"> Brief architecture </p>
</div>

<br>

## 3. Design 
The diagram illustrate a more detail streaming pipeline.
<div style="display: flex; flex-direction: column;">
<img src=/Assets/detail_architecture.png alt = "detail_architecture">

<p style="text-align: center;"> Detail stream pipeline </p>
</div>

- Data generated and sent to two topics "Clicks" and "Purchases"
- ```Get_min_click``` : To filter rows with min ```click_time``` for each ```user_id```, we first perform aggregation to retrieve ```user_id``` + ```min_click_time```, send it to topic "Min_clicks". Then we'll join topic "Clicks" and topic "Min_clicks" to get the row with full information. Spark Structured Streaming doesn't support aggregation and stream-stream join at the same time, so we create two different jobs within the SparkSession to handle tasks separately.


- ```Checkouts``` : We perform checkout attribution between topic "Purchases" and topic "Final_clicks", result will be sent to a postgres sink and topic "checkouts".

- ```Stream_cal_final_result``` : read data from topic "checkouts", perform near-real time aggregation within the last hour, and forward results to a postgres sink, from here Grafana will visualize them.

# 4. Core concept :
### a. State & watermarking : 
Due to the nature of late data arrival, watermark has been introduced to overcome the problem - it specify how late data can be accepted.

In stream aggregation, we'll use time window to calculate the result for each time interval, spark maintains intermediate results of each time window and wait to aggregate any late data correspond to them, until the current max ```event_time``` has passes the current watermark threshold - this means it won't accept any later data and will discard those states.

<div style="display: flex; flex-direction: column;">
<img src=/Assets/stream-aggregation.png alt = "stream aggregation">

<p style="text-align: center;"> Stream window aggregation example </p>
</div>


In stream-stream joins, spark will buffer each streaming record to a corresponding state, await for future joins. As data arrives, the joined output will be generated incrementally and written to the query sink. However, spark will maintain the states indefinitely -> states grow unbound since spark doesn't know if any future related events would happen or not. For this we need to specify join conditions, letting spark drops "out of service" states.

<div style="display: flex; flex-direction: column;">
<img src=/Assets/stream-stream_join.png alt = "stream-stream join">

<p style="text-align: center;"> Stream-stream join architecture in checkouts </p>
</div>

### b. Data retention : 

Aggregated result that serves visualization in grafana  will be kept for 48 hours, we'll create a cron job which will delete old data automatically every day.

```bash
-----Dockerfile-----

RUN apt-get update
RUN apt-get -y install cron

RUN crontab -l | { cat; echo "* * * * * /app/src/main/java/com/postgres/delete_old_data.sh"; } | crontab -
```

```bash
-----delete_old_data.sh----
#!/bin/bash

DB_HOST="postgres"
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="mypassword"

SQL_FILE="/app/src/main/java/com/postgres/delete_old_data.sql"

SQL_COMMAND=$(cat $SQL_FILE)

psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND"
```
<br>

<b> delete_old_data.sql </b> 
<div style="display: flex; flex-direction: column;">
<img src=/Assets/delete_old_data_sql.png alt = "stream aggregation" height = 300 width = 400>
</div>

### c. Running jobs in parallel :
We use ```.config("spark.streaming.concurrentJobs", "n") ``` to set the number of concurrentJobs that Spark will handle inside a SparkSession. In ```Get_min_click``` n is set to 2, performing aggregation and join simulatenously. In ```Stream_cal_final_result``` n is set to 5, writting 5 result tables to postgres sink.

Finally, we use <b> spark.stream().awaitAnyTermination() </b> to start all the queries.

### 5. Project Structure 
```bash

├── Input_data
│   ├── Products.csv
│   └── Users.csv
├── src
│   ├── main
│   │   └── java
│   │       └── com
│   │           ├── kafka
│   │           │     ├── Click_data.java
│   │           │     ├── Purchase_data.java
│   │           │     └── Kafka_Producer.java
│   │           ├── postgres
│   │           │     ├── create_schema.sql
│   │           │     ├── delete_old_data.sh
│   │           │     ├── delete_old_data.sql
│   │           │     └── Init_db.java
│   │           ├── stream_processing
│   │           │     ├── Kafka_df.java
│   │           │     ├── Get_min_click.java
│   │           │     ├── Checkouts.java
│   │           │     └── Stream_cal_final_result.java
│   │           └── Main.java
│   └── Grafana
│          └── dashboard.json
├── pom.xml
├── docker-compose.yaml
├── Dockerfile
├── Assets
│     └── many images
├── Checkpoints
├── spark-warehouse
├── .gitignore
├── readme.md
```

### Overview :
```bash 
kafka
├── Click_data.java
├── Purchase_data.java
└── Kafka_Producer.java

``````

<b> Click_data.java : </b> Generate random clicks 

<b> Purchase_data.java : </b> Generate random purchases

<b> Kafka_Producer.java : </b> Produce messages and send to kafka topics.
 
<br> <br>

```bash
postgres
    ├── create_schema.sql
    ├── delete_old_data.sh
    ├── delete_old_data.sql
    └── Init_db.java
```


<b> create_schema.sql : </b> SQL script to initialize schema for postgreSQL database "postgres"

<b> delete_old_data.sql : </b> Delete data from schema "result" older than 48 hours

<b> delete_old_Data.sh : </b> script which runs <b> delete_old_data.sql </b>

<b> Init_db.java : </b> establish connection to postgreSQL, run <b> create_schema.sql </b>, Insert data from folder ```Input_data``` to corresponding table.
<b>

<br> <br>

```bash

stream_processing
    ├── Kafka_df.java
    ├── Get_min_click.java
    ├── Checkouts.java
    └── Stream_cal_final_result.java

```
</b>

<b> Kafka_df.java : </b> A java class that implements operations between kafka topics and dataset, below are some main methods :
- ```Read_from_kafka() -> Dataset ``` : Read streaming data source from kafka 
- ```Write_to_kafka() -> StreamingQuery ``` : Write streaming dataset to output sink
- ```Explode_kafka_to_df() -> Dataset ``` : Convert kafka messages into valid streaming dataset

<b> Get_min_click.java : </b>
- ```Write_min_click() -> void ``` : Find min "click_time" for each "user_id" using window aggregation, write result to topic "Min_clicks"
- ```Filter_final_clicks() -> void ``` : Join streaming data between topic "clicks" and topic "Min_clicks" to retrieve rows with full information, watermarks for both are set to "10 minutes", states in buffer are maintained at most 10 minutes. Final results will be written to topic "Final_clicks".

<b> Checkouts.java : </b> 
Perform attribution between "purchases" and "Final_clicks"
- Enrich purchase data with user & product data retrieve from postgreSQL
- Re-calculate column "total_cost" to resolve inconsistencies -> Create column "profit"
- Perform stream-stream join (illustrate above)
- Write result to topic Checkouts

<b> Stream_cal_final_result.java : </b> 
- Perform aggregation and write aggregated results to corresponding PostgreSQL tables

<br> <br>

```bash
Main.java
```
This java file runs the entire application, it first execute ```Init_db.java``` to initialize database, then create 4 threads to handle jobs simultaneously : ```Kafka_Producer.java```, ```Get_min_click.java```, ```Checkouts.java```, ```Stream_cal_final_result.java```


## 6. Setting 

### Docker
Dockerfile includes two steps : build & run

<br>

<div style="display: flex; flex-direction: column;">
<img src=/Assets/Dockerfile.png alt = "Dockerfile">
</div>

<br>

a. Build (build jar file of app and dependency jar files) : 
- Docker pull image <b> maven:3.8.6-openjdk-11 </b>, copy entire app into docker container.
- Run ```mvn clean``` which removes any previously built artifacts.
- RUN ```mvn install dependency:copy-dependencies``` which compiles the application and packages it into a JAR file. The ```dependency:copy-dependencies``` copies all project dependencies (JAR files) into the /app/target/dependency/ directory.

b. Run :
- Docker pull image <b> openjdk:11.0.11-jre-slim </b>.
- Copy entire /app/ from build phase to a new directory /app/.
- A cron job is set up to run a shell script (delete_old_data.sh) periodically.
- Set the default command "CMD" to run Java application using the specified classpath and main class (com.Main).

### Running
Navigate the the root directory of the project, type command ```docker-compose up``` and the application will run.

<b> Note </b> 
- It might take several minutes (probably 2 - 3 minutes) for result in postgreSQL show up, this is due to ```Append``` output mode of the aggregated dataframe, spark will not write the intermediate results to the final results until it has passes the watermark threshold, for this we have set watermark threshold - time window to 1 minute each.


## 7. Visualization : 
To visualize result in grafana :
 - Access ```localhost:3000```, username and password are both "admin". Click on 'Datasources' -> "PostgreSQL" -> type the config below -> Click 'Save & test'
 - Copy the uid in red


<div style="display: flex; flex-direction: column;">
<img src=/Assets/postgres-grafana.png alt = "postgres_grafana">

<p style="text-align: center;"> Change PostgreSQL Connection only </p>
</div>

 - Choose import dashboard  from the search bar, navigate to folder ```src/Grafana``` and choose 'dashboard.json'
 - After this, the dashboard will display no data, this is due to mismatch of datasource uid. Click on the setting icon, Choose <b> Json Model </b>, then replace all uids within all datasources to the copied text before (You can do it quickly with ctrl + F -> replace)

<div style="display: flex; flex-direction: column;">
<img src=/Assets/Replace_datasource_uid.png alt = "postgres_grafana">

<p style="text-align: center;"> Replace datasource uid </p>
</div>

<div style="display: flex; flex-direction: column;">
<img src=/Assets/grafana_1st.png alt = "postgres_grafana">

</div>


<div style="display: flex; flex-direction: column;">
<img src=/Assets/grafana_2nd.png alt = "postgres_grafana">

<p style="text-align: center;"> Real-time dashboard  </p>
</div>
