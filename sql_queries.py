import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(event_id INT,artist_id TEXT, auth TEXT, first_name TEXT,gender char(1), item_in_session INT, last_name TEXT, length FLOAT, user_level TEXT, location TEXT, method TEXT, page TEXT, registration NUMERIC, session_id INT, song_title TEXT, status INT, ts BIGINT,user_agent TEXT, user_id INT)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(song_id TEXT ,num_songs INT, artist_id TEXT, artist_latitude NUMERIC, artist_longitude NUMERIC, artist_location TEXT, artist_name TEXT, title TEXT, duration NUMERIC, year INT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id INT NOT NULL IDENTITY(0,1), start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, level TEXT, song_id TEXT, artist_id TEXT, session_id INT, location TEXT, user_agent TEXT, PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INT, first_name TEXT, last_name TEXT, gender char(1), level TEXT,PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id TEXT, title TEXT, artist_id TEXT, year INT, duration NUMERIC, PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id TEXT, name TEXT, location TEXT, latitude NUMERIC, longitude NUMERIC,PRIMARY KEY (artist_id),FOREIGN KEY (artist_id) REFERENCES artists(artist_id))
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP, hour INT , day INT , week INT , month INT , year INT , weekday INT,PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from '{}'
                          credentials 'aws_iam_role={}'
                          region 'us-west-2' 
                          COMPUPDATE OFF STATUPDATE OFF
                          JSON '{}'""").format(config.get('S3','LOG_DATA'),
                          config.get('IAM_ROLE', 'ARN'),
                          config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from '{}'
                          credentials 'aws_iam_role={}'
                          region 'us-west-2' 
                          TIMEFORMAT as 'epochmillisecs'
                          STATUPDATE ON
                          JSON 'auto'
                           """).format(config.get('S3','SONG_DATA'), 
                           config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, e.user_id,e.user_level,s.song_id,s.artist_id,
                         e.session_id,e.location,e.user_agent
                         FROM staging_events e
                         LEFT JOIN staging_songs s
                         ON e.song_title = s.title
                         AND e.artist_id = s.artist_id
                         WHERE page = 'NextSong'
                         
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                              SELECT distinct  user_id, first_name, last_name, gender, user_level
                              FROM staging_events
                              WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                                SELECT distinct song_id, title, artist_id, year, duration
                                FROM staging_songs
                                WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                                SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                FROM staging_songs 
                                WHERE artist_id IS NOT NULL
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                                SELECT start_time, extract(hour from start_time), extract(day from start_time),                                   
                                extract (week from start_time), extract (month from start_time), 
                                extract(year from start_time),extract(dayofweek from start_time)
                                FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
