
# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP NOT NULL, 
user_id INTEGER NOT NULL, 
level VARCHAR(50) NOT NULL, 
song_id VARCHAR(50), 
artist_id VARCHAR(50), 
session_id INTEGER NOT NULL, 
location VARCHAR(50) NOT NULL, 
user_agent VARCHAR(150) NOT NULL
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS (
user_id INTEGER PRIMARY KEY,
first_name VARCHAR(100),
last_name VARCHAR(100),
gender VARCHAR(50),
level VARCHAR(50)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS (
song_id VARCHAR(100) PRIMARY KEY,
title VARCHAR(100) NOT NULL,
artist_id VARCHAR(100) NOT NUll,
year INTEGER NOT NULL,
duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS (
artist_id VARCHAR(100) PRIMARY KEY,
name VARCHAR(100) NOT NULL,
location VARCHAR(100),
latitude double precision,
longitude double precision
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday VARCHAR
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id,start_time,user_id,level,
                      song_id,artist_id,session_id,location,user_agent)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(songplay_id) DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO USERS (user_id,first_name,last_name,gender,level)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE
SET level = excluded.level
;
""")

time_table_insert = ("""
INSERT INTO TIME (start_time,hour,day,week,month,year,weekday)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
""")


# FIND SONGS

song_select = ("""
select songs.song_id, songs.artist_id from songs
join artists on artists.artist_id = songs.artist_id
where artists.name = %s and
songs.title = %s and
songs.duration = %s;
""")

# DROP TABLE FUNCTION
def drop_table_query(tablename):
    q = f'''
    DROP TABLE IF EXISTS {tablename};
    '''
    return q


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_names = ['songplays','USERS','SONGS','ARTISTS','time']