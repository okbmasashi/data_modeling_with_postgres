# Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
Creation of a Postgres database with tables designed to optimize queries on song play analysis and ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

# Database design
Using following fact table and dimension tables, I composed a star schema optimized for queries on song play analysis
## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong

|Columns|Type|Primary Key|
|-------|----|-----------|
|songplay_id|serial|○|
|start_time|bigint||
|user_id|int||
|level|varchar||
|song_id|varchar||
|artist_id|varchar||
|session_id|int||
|location|varchar||
|user_agent|varchar||

## Dimension Tables
### users
represents users in the app

|Columns|Type|Primary Key|
|-------|----|-----------|
|user_id|int|○|
|first_name|varchar||
|last_name|varchar||
|gender|varchar||
|level|varchar||

### songs
represents songs in music database

|Columns|Type|Primary Key|
|-------|----|-----------|
|song_id|varchar|○|
|title|varchar||
|artist_id|varchar||
|year|int||
|duration|float||

### artists
represents artists in music database

|Columns|Type|Primary Key|
|-------|----|-----------|
|artist_id|varchar|○|
|name|varchar||
|location|varchar||
|latitude|float||
|longitude|float||


### time
represents timestamps of records in songplays broken down into specific units

|Columns|Type|Primary Key|
|-------|----|-----------|
|start_time|bigint|○|
|hour|int||
|day|int||
|week|int||
|month|int||
|year|int||
|weekday|int||

# ETL pipeline design
## Song data processing
- Extract song and artists information from `song_data` consists of the json files.
- Record the information in the songs and artists dimensional tables.

## Log data processing
- Extract user play log information from `log_data` consists of the json files.
- Record the play time information in the time table.
- Record the user information in the users dimension table.
- Record the songplay information in the songplays fact table with 
`user_id`, `song_id`, `artist_id`, `start_time` which can link to the record of the demension tables.