## Problem Statement
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Task
Create a Postgres database with tables designed to optimize queries on song play analysis by creating a database schema and ETL pipeline for this analysis.

## Dataset
The dataset used in the project is from Million Song Dataset. The files (songs and logs) are in json format.

## Schema
<p> Using the song and log datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the fact and dimension tables. This can help Sparkify solve common business queries, which includes information about what songs users are listening to.</p>
1. recommending popular songs of the day/week to other users.<br/>
2. Favorite songs of user based on week/day <br/>
3. Clustering the users based on artists they listen and recommending them other popular songs of the cluster.<br/>

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables
1. users - users in the app(user_id, first_name, last_name, gender, level)
2. songs - songs in music database (song_id, title, artist_id, year, duration)
3. artists - artists in music database (artist_id, name, location, latitude, longitude)
4. time - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

## Project Structure
In addition to the data files, the project workspace includes six files:

> `test.ipynb` displays the first few rows of each table to let you check your database.</br>
> `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.<br/>
> `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.<br/>
> `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.<br/>
> `sql_queries.py` contains all your sql queries, and is imported into the last three files above.<br/>
> `README.md` provides discussion on your project.<br/>

## Usuage
The dependencies are listed in requirement.txt<br/>
`etl.ipynb` and `test.ipynb` are jupyter notebook files and can be run using jupyter notebook<br/>
`create_tables.py` and `etl.pycan be run using python terminal `python filename.py`<br/>
