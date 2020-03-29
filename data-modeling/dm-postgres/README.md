# Sparkify Postgres ETL

This project consists on putting into practice the following concepts:
- Data modeling with Postgres
- Database star schema created 
- ETL pipeline using Python

## Context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Goal is to create a database schema and ETL pipeline for this analysis.

### Data
[Million Song Dataset] (http://millionsongdataset.com "Data Set Used")
- **Song datasets**: all json files are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Database Schema
The schema used for this exercise is the Star Schema: 
There is one main fact table containing all the measures associated to each event (user song plays), 
and 4 dimentional tables, each with a primary key that is being referenced from the fact table.

On why to use a relational database for this case:
- The data types are structured (we know before-hand the sctructure of the jsons we need to analyze, and where and how to extract and transform each field)
- The amount of data we need to analyze is not big enough to require big data related solutions.
- Ability to use SQL that is more than enough for this kind of analysis
- Data needed to answer business questions can be modeled using simple ERD models
- We need to use JOINS for this scenario

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id (INT) PRIMARY KEY: ID of each user song play 
- start_time (DATE) NOT NULL: Timestamp of beggining of user activity
- user_id (INT) NOT NULL: ID of user
- level (TEXT): User level {free | paid}
- song_id (TEXT) NOT NULL: ID of Song played
- artist_id (TEXT) NOT NULL: ID of Artist of the song played
- session_id (INT): ID of the user Session 
- location (TEXT): User location 
- user_agent (TEXT): Agent used by user to access Sparkify platform

#### Dimension Tables
**users** - users in the app
- user_id (INT) PRIMARY KEY: ID of user
- first_name (TEXT) NOT NULL: Name of user
- last_name (TEXT) NOT NULL: Last Name of user
- gender (TEXT): Gender of user {M | F}
- level (TEXT): User level {free | paid}

**songs** - songs in music database
- song_id (TEXT) PRIMARY KEY: ID of Song
- title (TEXT) NOT NULL: Title of Song
- artist_id (TEXT) NOT NULL: ID of song Artist
- year (INT): Year of song release
- duration (FLOAT) NOT NULL: Song duration in milliseconds

**artists** - artists in music database
- artist_id (TEXT) PRIMARY KEY: ID of Artist
- name (TEXT) NOT NULL: Name of Artist
- location (TEXT): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist

**time** - timestamps of records in songplays broken down into specific units
- start_time (DATE) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time 
- year (INT): Year associated to start_time
- weekday (TEXT): Name of week day associated to start_time


## Project structure

Files used on the project:
1. **data** folder nested at the home of the project, where all needed jsons reside.
2. **sql_queries.py** contains all sql queries, and is imported into the files below.
3. **create_tables.py** drops and creates tables. Run this file to reset tables before each time you run ETL scripts.
4. **config.py** parse the database.ini file and returns the credentials to connect with database.
5. **log.py** returns a logger with level set to INFO. 
6. **etl.py** reads and processes files from song_data and log_data and loads them into  tables. 
7. **metrics.py** gives the metrics (time and memory usuage of functions) use to monitor functions.
8. **string_iterator.py** StringIteratorIO class is used to create a file like object, so in memory CSV file is not created.
9. **postgres-learning.py and queries.py** are files made to explore the psycopg module.

### Break down of steps followed

1º Wrote DROP, CREATE and INSERT query statements in sql_queries.py

2º Run in console
 ```
python create_tables.py
```

3º Run etl in console, and verify results:
 ```
python etl.py
```


## ETL pipeline

Prerequisites: 
- Database and tables created

1. On the etl.py we start our program by connecting to the sparkify database, and begin by processing all songs and logs related data.

2. EXTRACT   : Walking through the tree files under /data/song_data (ldJson) and /data/log_data (ndJson).

3. TRANSFORM : Filtering logs having page value 'NextSong',  and creates a song_lookup object

4. LOAD      : Creating file like object and usinng COPY method to load the batch into respective tables 



## Author

* **Darsh Shukla (Dminor7)** - [Github](https://github.com/Dminor7) - [LinkedIn](https://www.linkedin.com/in/darsh-shukla-277825174/)
