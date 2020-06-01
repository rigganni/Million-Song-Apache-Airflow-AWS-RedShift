class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays(
                              playid,
                              start_time,
                              userid,
                              "level",
                              songid,
                              artistid,
                              sessionid,
                              location,
                              user_agent
                              )
                              
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            LEFT JOIN (SELECT playid -- do not insert records that already exist
        FROM songplays) songplays
            ON md5(events.sessionid || events.start_time) = songplays.playid
            WHERE songplays.playid IS NULL
    """)

    user_table_insert = ("""
        INSERT INTO users(
                              userid,
                              first_name,
                              last_name,
                              gender,
                              "level"
                              )
        SELECT distinct staging_events.userid, firstname, lastname, staging_events.gender, staging_events.level
        FROM staging_events
        LEFT JOIN users -- do not add user if already exists in users table
          ON staging_events.userid = users.userid
        WHERE page='NextSong'
              AND
              users.userid IS NULL
    """)

    song_table_insert = ("""
        INSERT INTO songs(
                              songid,
                              title,
                              artistid,
                              year,
                              duration
                              )
        SELECT distinct song_id, staging_songs.title, artist_id, staging_songs.year, staging_songs.duration
        FROM staging_songs
        LEFT JOIN songs -- do not add songs that already exist
          ON staging_songs.song_id = songs.songid
        WHERE songs.songid IS NULL
    """)

    artist_table_insert = ("""
        INSERT INTO artists(
                              artistid,
                              name,
                              location,
                              lattitude,
                              longitude
                              )
        SELECT distinct artist_id, staging_songs.artist_name, staging_songs.artist_location, staging_songs.artist_latitude, staging_songs.artist_longitude
        FROM staging_songs
        LEFT JOIN artists -- only add artists that do not already exist
          ON staging_songs.artist_id = artists.artistid
    """)

    time_table_insert = ("""
        INSERT INTO time(
                              start_time,
                              "hour",
                              "day",
                              week,
                              "month",
                              "year",
                              weekday
                              )
        SELECT  songplays.start_time,
                extract(hour from songplays.start_time),
                extract(day from songplays.start_time),
                extract(week from songplays.start_time),
                extract(month from songplays.start_time),
                extract(year from songplays.start_time),
                extract(dayofweek from songplays.start_time)
        FROM songplays
        LEFT JOIN time -- only add time values that do not already exist
          on songplays.start_time = time.start_time
        WHERE time.start_time IS NULL
    """)
