import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data = config.get("S3", "LOG_DATA")
log_json_path = config.get("S3", "LOG_JSONPATH")
song_data = config.get("S3", "SONG_DATA")

arn = config.get("IAM_ROLE", "ARN")

# DROP SCHEMA
music_schema_drop = "DROP SCHEMA IF EXISTS music CASCADE"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE SCHEMA
music_schema_create = "CREATE SCHEMA IF NOT EXISTS music"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE music.staging_events(
    artist varchar(500),
    auth varchar(500),
    first_name varchar(50),
    gender varchar(50),
    item_in_session bigint,
    last_name varchar(50),
    length float,
    level varchar(50),
    location varchar(5000),
    method varchar(50),
    page varchar(50),
    registration bigint,
    session_id bigint,
    song varchar(5000),
    status bigint,
    ts bigint,
    user_agent varchar(5000),
    user_id varchar(50)
)
""")

staging_songs_table_create = ("""
CREATE TABLE music.staging_songs (
    song_id varchar(50),
    num_songs int,
    title varchar(5000),
    artist_name varchar(5000),
    artist_latitude decimal,
    year int,
    duration float,
    artist_id varchar(50),
    artist_longitude float,
    artist_location varchar(5000)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS music.songplays(songplay_id bigint IDENTITY(0,1) PRIMARY KEY
                                    ,start_time timestamp NOT NULL
                                    ,user_id varchar NOT NULL
                                    ,level varchar NOT NULL
                                    ,song_id varchar NOT NULL
                                    ,artist_id varchar NOT NULL
                                    ,session_id int
                                    ,location varchar
                                    ,user_agent varchar
                                    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS music.users(user_id varchar PRIMARY KEY
                                ,first_name varchar
                                ,last_name varchar
                                ,gender varchar
                                ,level varchar NOT NULL
                                );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS music.songs(song_id varchar PRIMARY KEY
                                ,title varchar NOT NULL
                                ,artist_id varchar NOT NULL
                                ,year int
                                ,duration float
                                );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS music.artists( artist_id varchar PRIMARY KEY
                                    ,name varchar NOT NULL
                                    ,location varchar
                                    ,latitude float
                                    ,longitude float
                                    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS music.time(start_time timestamp PRIMARY KEY
                                ,hour int NOT NULL
                                ,day int NOT NULL
                                ,week int NOT NULL
                                ,month int NOT NULL
                                ,year int NOT NULL
                                ,weekday int NOT NULL
                                );
""")

# STAGING TABLES

staging_events_copy = ("""
copy music.staging_events from {}
iam_role {}
region 'us-west-2'
format as json {};
""").format(log_data, arn, log_json_path)

staging_songs_copy = ("""
copy music.staging_songs from {}
iam_role {}
region 'us-west-2'
format as json 'auto';
""").format(song_data, arn)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO music.songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,
        e.user_id,
        e.level,
        s.song_id,
        a.artist_id,
        e.session_id,
        e.location,
        e.user_agent
FROM music.staging_events e
INNER JOIN music.songs s ON s.title = e.song AND s.duration = e.length
INNER JOIN music.artists a ON a.artist_id = s.artist_id AND a.name = e.artist
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO music.users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM music.staging_events
WHERE TRIM(user_id) <> ''
""")

song_table_insert = ("""
INSERT INTO music.songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM music.staging_songs
""")

artist_table_insert = ("""
INSERT INTO music.artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM music.staging_songs
""")

time_table_insert = ("""
INSERT INTO music.time(start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,
        DATEPART(h, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS hour,
        DATEPART(d, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')  AS day,
        DATEPART(w, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')  AS week,
        DATEPART(mon, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')  AS month,
        DATEPART(y, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')  AS year,
        DATEPART(dow, TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')  AS weekday
FROM music.staging_events
""")

# QUERY LISTS
# Get the portion of paid and free level
level_ratio = """
SELECT level,
       COUNT(song_id) AS rate
FROM music.songplays
GROUP BY level
"""

# Get top 10 songs lisned in 2018
top_listened_song_in_2018 = """
SELECT s.title AS song_title,
       COUNT(songplay_id) AS listened_times,
       a.name AS artist_name
FROM music.songplays sp
INNER JOIN music.songs s ON s.song_id = sp.song_id
INNER JOIN music.artists a ON a.artist_id = sp.artist_id
WHERE EXTRACT(YEAR FROM start_time) = 2018
GROUP BY s.title, a.name
ORDER BY listened_times DESC
LIMIT 10
 """


create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert, songplay_table_insert]

select_queries = [level_ratio,
                  top_listened_song_in_2018]
