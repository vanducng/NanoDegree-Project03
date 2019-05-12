import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events(
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
CREATE TABLE staging_songs (
    song_id varchar(50),
    num_songs int,
    title varchar(5000),
    artist_name varchar(5000),
    artist_latitude decimal,
    year int,
    duration decimal,
    artist_id varchar(50),
    CREATE TABLE staging_songs (
    song_id varchar(50),
    num_songs int,
    title varchar(5000),
    artist_name varchar(5000),
    artist_latitude decimal,
    year int,
    duration decimal,
    artist_id varchar(50),
    artist_longitude float,
    artist_location varchar(5000)
); float,
    artist_location varchar(5000)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY
                                ,first_name varchar
                                ,last_name varchar
                                ,gender varchar
                                ,level varchar NOT NULL
                                );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY
                                ,first_name varchar
                                ,last_name varchar
                                ,gender varchar
                                ,level varchar NOT NULL
                                );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY
                                ,title varchar NOT NULL
                                ,artist_id varchar NOT NULL
                                ,year int
                                ,duration float
                                );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists( artist_id varchar PRIMARY KEY
                                    ,name varchar NOT NULL
                                    ,location varchar
                                    ,latitude float
                                    ,longitude float
                                    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY
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
COPY staging_events from '{}'
credential 'aws_iam_role={}'
""").format()

staging_songs_copy = ("""

""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, 
                    session_id, location, user_agent)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE
    SET level  = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
