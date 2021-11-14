import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    for index, row in df.iterrows():
        # insert song record
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)        
        print(song_data)
        try:
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            print("Error: Insert Song Table")
            break

    
        # insert artist record
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        try:
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print("Error: Insert Artist Table")
            break

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'].astype(int), unit='ms')

    # insert time data records
    time_data = pd.concat([df['ts'], 
              t.dt.hour,
              t.dt.day,
              t.dt.week,
              t.dt.month,
              t.dt.year,
              t.dt.weekday],axis=1)
    column_labels = (['datetime',
                  'hour',
                  'day',
                  'week',
                  'month',
                  'year',
                  'weekday'])
    time_df = time_data.copy()
    time_df.set_axis(column_labels, axis=1)
    
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Insert Time Table")
            break

    # load user table
    user_df = pd.concat([df['userId'],
                     df['firstName'],
                     df['lastName'],
                     df['gender'], 
                     df['level']], axis=1)

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Insert User Table")
            break

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        try:
            print(song_select % (row.song, row.artist, row.length))
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            try:
                songplay_data = (row.ts, row.userId, row.level, songid, artistid, 
                                 row.sessionId, row.location, row.userAgent)
                print(songplay_data)
                cur.execute(songplay_table_insert, songplay_data)
            except psycopg2.Error as e:
                print("Error: Insert Songplay Table")
                break

        except psycopg2.Error as e:
            print("Error: Query Song")
            break
            
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()