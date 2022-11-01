import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events(
        artist              text,
        auth                text,
        first_name          text,
        gender              text,
        ItemInSession       int,
        last_name           text,
        length              float,
        level               text,
        location            text,
        method              text,
        page                text,
        registration        text,
        session_id          int,
        song                text,
        status              int,
        ts                  bigint, 
        user_agent          text, 
        user_id             int
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
        song_id             text,
        artist_id           text,
        artist_latitude     float,
        artist_longitude    float,
        artist_location     text,
        artist_name         varchar(255),
        duration            float,
        num_songs           int,
        title               text,
        year                int
    )
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
        songplay_id         int identity(0,1) primary key,
        start_time          timestamp not null sortkey distkey,
        user_id             int not null,
        level               varchar,
        song_id             varchar,
        artist_id           varchar,
        session_id          int,
        location            varchar,
        user_agent          varchar
    )
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
        user_id             varchar PRIMARY KEY,
        first_name          varchar,
        last_name           varchar,
        gender              varchar,
        level               varchar
    )
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
        song_id             varchar PRIMARY KEY NOT NULL,
        title               varchar NOT NULL,
        artist_id           varchar NOT NULL,
        year                int,
        duration            float
    )
"""

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
        artist_id           varchar PRIMARY KEY NOT NULL,
        name                varchar,
        location            varchar,
        latitude            float,
        longitude           float
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (
        start_time          timestamp not null distkey sortkey primary key,
        hour                int not null,
        day                 int not null,
        week                int not null,
        month               int not null,
        year                int not null,
        weekday             varchar not null
    ) 
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'
""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)

staging_songs_copy = ("""
copy staging_songs from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON 'auto'
""").format(bucket=SONG_DATA, role=IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * interval '1 second'),
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id, 
        e.location,
        e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        e.length = s.duration
    WHERE
        e.page = 'NextSong'
        and e.user_id IS NOT NULL
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    where user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")


time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * interval '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
