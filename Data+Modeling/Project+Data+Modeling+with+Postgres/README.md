- Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

# Data Modeling with Postgres

## Database context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

The goal is to create a **database schema** and **ETL pipeline** for this analysis. Also be able to test the database created and ETL pipeline by running queries.


## The project workspace content

### An explanation of the files in the repository

- `test.ipynb` displays the first few rows of each table to let you check your database.
- `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
- `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.
- `sql_queries.py` contains all your sql queries, and is imported into the last three files above.

### Database schema design and ETL pipeline

Schema for Song Play Analysis
> Using the song and log datasets, you'll need to create a **star schema** optimized for queries on song play analysis. This includes the following tables.

**Fact Table**
1. songplays - records in log data associated with song plays i.e. records with page NextSong

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
2. users - users in the app
user_id, first_name, last_name, gender, level

3. songs - songs in music database
song_id, title, artist_id, year, duration

4. artists - artists in music database
artist_id, name, location, latitude, longitude

5. time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

### How to run the Python scripts

> You will not be able to run `test.ipynb`, `etl.ipynb`, or `etl.py` until you have run `create_tables.py` at least once to create the **sparkifydb** database, which these other files connect to.

- Run `create_tables.py` to create the database and tables.
- Run `test.ipynb` to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.


## Dataset and ETL pipeline strategy

The dataset samples are stored into `/data/` folder, with:

```
data
|   log_data
|   song_data
```

- log_data samples:

```
{
    "artist":null,
    "auth":"Logged In",
    "firstName":"Walter",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Frye",
    "length":null,
    "level":"free",
    "location":"San Francisco-Oakland-Hayward, CA",
    "method":"GET",
    "page":"Home",
    "registration":1540919166796.0,
    "sessionId":38,"song":null,
    "status":200,
    "ts":1541105830796,
    "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"39"
}
```

- song_data samples:

```
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}

```


- ETL pipeline:

based on these two dataset samples and the goal described, the following python functions were designed into the file `etl.py`.

1. **process_song_file()**
2. **process_log_file()**
3. **process_data()**


at the end, the final results for song play analysis:


| songplay_id | start_time          | user_id | level | song_id | artist_id | session_id | location                        | user_agent                                                              |
| ----------- | ------------------- | ------- | ----- | ------- | --------- | ---------- | ------------------------------- | ----------------------------------------------------------------------- |
| 0           | 2018-11-30 12:22:07 | 91      | free  | None    | None      | 829        | Dallas-Fort Worth-Arlington, TX | Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0) |
