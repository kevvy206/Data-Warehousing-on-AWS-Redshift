import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS time"


# CREATE TABLES
# Primary keys and foreign keys not enforced in AWS, but written for reference
staging_events_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession VARCHAR,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId VARCHAR,
    song VARCHAR,
    status VARCHAR,
    ts DECIMAL,
    userAgent VARCHAR,
    userId VARCHAR)""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL,
    year VARCHAR)""")

# The fact table is songplays table. Since it will be joined with 4 \
# dimensional tables, even distribution style seemed appropriate.
songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time) SORTKEY,
    user_id VARCHAR REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id VARCHAR,
    location VARCHAR,
    user_agent VARCHAR)
    DISTSTYLE EVEN""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
    DISTSTYLE ALL""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR,
    artist_id VARCHAR,
    year VARCHAR,
    duration VARCHAR)
    DISTSTYLE ALL""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR)
    DISTSTYLE ALL""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour VARCHAR,
    day VARCHAR,
    week VARCHAR,
    month VARCHAR,
    year VARCHAR,
    weekday VARCHAR)
    DISTSTYLE ALL""")


# STAGING TABLES
staging_events_copy = ("""
  COPY staging_events
  FROM {}
  IAM_ROLE {}
  FORMAT AS JSON {}
  REGION 'us-west-2'
  """).format(config['S3']['LOG_DATA'],
              config['IAM_ROLE']['ARN'],
              config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
  COPY staging_songs
  FROM {}
  IAM_ROLE {}
  FORMAT AS JSON 'auto'
  REGION 'us-west-2'
  """).format(config['S3']['SONG_DATA'],
              config['IAM_ROLE']['ARN'])


# FINAL TABLES
# 'DISTINCT' used in queries because constraints are not enforced in AWS
songplay_table_insert = ("""
  INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
  SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId AS session_id,
    se.location,
    se.userAgent AS user_agent
  FROM staging_events AS se
  JOIN staging_songs AS ss
  ON se.artist = ss.artist_name
  WHERE se.page = 'NextSong'""")

user_table_insert = ("""
  INSERT INTO users
    (user_id, first_name, last_name, gender, level)
  SELECT
    DISTINCT userId as user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
  FROM staging_events
  WHERE page = 'NextSong'""")

song_table_insert = ("""
  INSERT INTO songs
    (song_id, title, artist_id, year, duration)
  SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
  FROM staging_songs""")

artist_table_insert = ("""
  INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
  SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
  FROM staging_songs""")

time_table_insert = ("""
  INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
  SELECT
    DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
  FROM staging_events""")


# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries =   [staging_events_table_drop,
                        staging_songs_table_drop,
                        songplay_table_drop,
                        user_table_drop,
                        song_table_drop,
                        artist_table_drop,
                        time_table_drop]

copy_table_queries =   [staging_events_copy,
                        staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
