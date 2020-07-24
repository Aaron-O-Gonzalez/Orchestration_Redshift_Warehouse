class SqlQueries:
    
    staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                 eventId BIGINT IDENTITY(0,1),
                                 artist VARCHAR,
                                 auth VARCHAR,
                                 firstName VARCHAR,
                                 gender VARCHAR(1),
                                 itemInSession INT NOT NULL,
                                 lastName VARCHAR,
                                 length FLOAT,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method VARCHAR,
                                 page VARCHAR,
                                 registration VARCHAR,
                                 sessionId INT,
                                 song VARCHAR,
                                 status INT,
                                 ts BIGINT,
                                 userAgent VARCHAR,
                                 userId INT)""")

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 num_songs INT,
                                 artist_id VARCHAR,
                                 artist_latitude VARCHAR,
                                 artist_longitude VARCHAR,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration FLOAT,
                                 year SMALLINT)""")

    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id VARCHAR(32) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            userid INT NOT NULL,
                            level VARCHAR NULL,
                            songId VARCHAR NOT NULL,
                            artistId VARCHAR NOT NULL,
                            sessionId INT,
                            location VARCHAR,
                            userAgent VARCHAR)""")

    user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        userid INT PRIMARY KEY,
                        firstname VARCHAR NULL,
                        lastname VARCHAR NULL,
                        gender VARCHAR NULL,
                        level VARCHAR NULL)""")

    song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        songId VARCHAR PRIMARY KEY,
                        title VARCHAR NULL,
                        artistId VARCHAR NULL,
                        year SMALLINT NULL,
                        duration FLOAT NULL)""")

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artistId VARCHAR PRIMARY KEY,
                        artistName VARCHAR,
                        artistLocation VARCHAR,
                        artistLatitude VARCHAR,
                        artistLongitude VARCHAR)""")

    time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour SMALLINT,
                        day SMALLINT,
                        week SMALLINT,
                        month SMALLINT,
                        year SMALLINT,
                        weekday SMALLINT)""")
    
    songplay_table_insert = ("""INSERT INTO songplays(
                                start_time,
                                songplay_id,
                                userid,
                                level,
                                songId,
                                artistId,
                                sessionId,
                                location,
                                userAgent)
                                
        SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
                md5(se.sessionId || start_time) AS songplay_id,
                se.userId AS userid, 
                se.level AS level, 
                ss.song_id AS songId, 
                ss.artist_id AS artistId, 
                se.sessionId AS sessionId, 
                se.location AS location, 
                se.userAgent AS userAgent
                
                FROM staging_events AS se
                JOIN staging_songs AS ss
                ON(se.artist = ss.artist_name)
                WHERE se.page='NextSong';""")

    user_table_insert = ("""INSERT INTO users(
                            userid,
                            firstname,
                            lastname,
                            gender,
                            level)
    
                            SELECT DISTINCT se.userId, 
                                            se.firstName, 
                                            se.lastName, 
                                            se.gender, 
                                            se.level
                                            
                            FROM staging_events as se
                            WHERE page='NextSong'""")

    song_table_insert = ("""INSERT INTO songs(
                            songId,
                            title,
                            artistId,
                            year,
                            duration)
                            
                            SELECT DISTINCT ss.song_id as songId, 
                                            ss.title as title, 
                                            ss.artist_id as artistId, 
                                            ss.year as year, 
                                            ss.duration as duration
                            
                            FROM staging_songs as ss;""")

    artist_table_insert = ("""INSERT INTO artists(
                              artistId,
                              artistName,
                              artistLocation,
                              artistLatitude,
                              artistLongitude)
                              
                              SELECT distinct ss.artist_id, 
                                              ss.artist_name, 
                                              ss.artist_location, 
                                              ss.artist_latitude, 
                                              ss.artist_longitude
                                              
                              FROM staging_songs as ss""")

    time_table_insert = ("""INSERT INTO time(
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday)
                            
                            SELECT start_time, extract(hour from start_time), extract(day from start_time), 
                            extract(week from start_time), extract(month from start_time), 
                            extract(year from start_time), extract(dayofweek from start_time)
                            
                            FROM songplays""")