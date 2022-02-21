# Orchestration of AWS Redshift Data Warehouse

This project uses an Airflow directed acyclic graph (DAG) to collect raw data from S3 buckets, create tables for data staging, and create a data warehouse adhering to a snowflake schema. Our DAG is comprised of the following tasks:

![DAG](https://github.com/Aaron-O-Gonzalez/Orchestration_Redshift_Warehouse/blob/master/airflow_dag.png)

# Staging Tables

The two datasets, LOG_DATA and SONG_DATA, are json files located in separate S3 buckets which are copied into separate tables called **staging_events** and **staging_songs**, respectively.

**Staging_events** This table is comprised of the following fields: artistName, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

**Staging_songs** This table is comprised of the following fields:num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

# Development of Star Schema

The staging tables are used for generating the **songplays**, **users**, **songs**, **artists**, and **time** tables.

## Fact Table

**songplays** This fact table is composed of the following fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

## Dimension Tables

**users**: This dimension table is composed of user_id, first_name, last_name, gender, level

**songs**: This dimension table is composed of song_id, title, artist_id, year, duration

**artists**: This dimension table is composed of artist_id, name, location, latitutde, longitude

**time**: This dimension table is composed of timestamp, hour, day, week, month, year, weekday

# Quality Control
Specific to this project, the primary mode of quality control is ensuring that no tables are empty. 
