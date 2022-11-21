## Introduction

This is a demo datawarehouse created from user activity logs and metadata. Appropriate fact and dimension tables are created. The target db used is `postgresql` and the raw data is a dump of `json` files.

### Raw Data
The dataset contains the **user activity** of subscribers of a music streaming application. The files are partitioned by year and month. Example file paths are given below

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

The second source of raw data is about the metadata on songs played by subscribers. This dataset is partitioned by the first three letters of each song's track ID. Example file paths are given below

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

### Schema for Song Play Analysis

A star schema is created using the user activity and song metadata. Following tables are created:

**Fact Table**

1. **songplays**: records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
2. users - users in the app
- user_id, first_name, last_name, gender, level
3. songs - songs in music database
- song_id, title, artist_id, year, duration
4. artists - artists in music database
- artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday


### Running the project

`config.py` contains the credentials to postgres sever, fill up username and password before you run the project

`create_tables.py` run this to create `sparkify` db and associated tables
`etl.py` run this to complete the data load from json files to sql server
`sql_queries.py` this contains the queries needed to create fact and dimension tables as well as insert data in them

`etl.ipynb` this has the prototype code for the whole project