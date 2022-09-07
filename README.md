# Project: AWS Datawarehouse

This project is for Udacity's Data Engineering Nanodegree

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to extra the data zip files using Spark, then loading the data back into a set of dimension tables.


## Data

### Example JSON Song File:
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Data contained in log Files:
artist, auth, firstName, gender, itemInSession, lastName, length, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

## Staging tables:

### staging_events_table
artist, auth, firstName, gender, itemInSession, lastName, length, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

### staging_songs_table
num_songs, artist_id, artist_latitude, artist_longitude, artist_name, song_id, title, duration, year



## Schema for Song Play Analysis

### Fact Table
#### songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
#### users - users in the app
user_id, first_name, last_name, gender, level

#### songs - songs in music database
song_id, title, artist_id, year, duration

#### artists - artists in music database
artist_id, name, location, lattitude, longitude

#### time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## How to run
1. Add AWS IAM Credentials in the dl.cfg file
2. Specify an output file path within the main function of etl.py
3. run etl.py