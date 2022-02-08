from logging import exception
from textwrap import indent
from urllib import request
from wsgiref import headers
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime,timedelta
import sqlite3


def check_if_valid_data(df : pd.DataFrame) -> bool:

    if df.empty:
        print("No song downloaded. Finishing execution")
        return False

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary key is voilated')

    if df.isnull().values.any():
        raise Exception('Null values found')

    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second = 0, microsecond = 0)

    # timestamps = df['timestamp'].tolist()
    # for timestamp in timestamps:
    #     if datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         raise Exception('At least on of the returned songs does not come from within last 24 hours ')
    
    return True

def run_spotify_etl():

# if __name__ =='__main__':
    DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
    USER_ID = 'bxfpt7fugp8bl4r0bp2x8ttg7'
    TOKEN ="BQB-hyZ3lWl-51o89QxW0wvTQoeNIgUvrt8z52UkXF72X2vtZ0uc9RQC9sBEJ4zpehwj4fX7zHiBmOpIxdVI8WzQ3tRbEon6hatdRHagiKKGEik2pvBMjWdl0U_3K2KCGCrt-uV3_tWySctNOkyJJHh1za3O_etzWyo0"

    headers = {
        "Accept": "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    print('hiya')
    today = datetime.now()
    # print(today)
    yesterday = today - timedelta(days=1)
    # print(yesterday)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    # print(yesterday_unix_timestamp)

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers=headers)
    data = r.json()
    # json_response = json.dumps(data, indent=2,sort_keys=True)

    # print(data)
    # print(json_response)
    song_name = []
    artist_name = []
    played_at = []
    timestamps=[]
    for song in data["items"]:
        song_name.append(song['track']['name'])
        artist_name.append(song['track']['album']['artists'][0]['name'])
        played_at.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
    song_dict = {
        'song_name': song_name,
        'artist_name' : artist_name,
        'played_at' : played_at,
        'timestamp' : timestamps
    }
    song_df = pd.DataFrame(song_dict, columns=['song_name','artist_name','played_at','timestamp'])
    # print(data)

    if check_if_valid_data(song_df):
        print('Data valid,proceed to load stage')

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCAHR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY(played_at)
        )"""
    cursor.execute(sql_query)
    print(' opened databse succesfully')

    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print("Data already exist in the database")
    conn.close()
    print('Database closed successfully')    
    # print(song_df)
    # print(timestamps)
