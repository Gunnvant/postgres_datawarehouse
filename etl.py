import os
import psycopg2
import logging
import json
from sql_queries import *
import create_tables
import config
from datetime import datetime
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def get_paths(path:str)->list:
    '''
    Recursively scans a directory and returns absolute path of 
    json files
    
    Input(s)
    path(str): path to directory which has to be scanned
    Return
    paths(list): list of paths to json data files
    '''
    paths = []
    for roots,dirs,files in os.walk(path):
        for file in files:
            if file.endswith(".json"):
                paths.append(os.path.join(roots,file))
    return paths

def read_json(path):
    '''
    Reads a json file and
    returns a dict
    '''
    with open(path,'r') as f:
        data = json.loads(f.read())
    return data

def execute_query(conn,query):
    '''
    Runs a sql query given query string
    and connection object
    '''
    logging.info(f"Executing query {query}")
    cur = conn.cursor()
    try:
        cur.execute(query)
    except:
        logging.error(f"Query: {query} failed")
        cur.rollback()
    cur.close()

def process_song_files(conn,paths):
    '''
    Inserts records to song and artist tables
    '''
    curr = conn.cursor()
    for path in paths:
        data = read_json(path)
        data_artist = [data.get('artist_id'),data.get('artist_name'),
                                         data.get('artist_location'),
                                         data.get('artist_latitude'),
                                         data.get('artist_longitude')]
        curr.execute(artist_table_insert,data_artist)
        data_song = [data.get('song_id'),
                                        data.get('title'),
                                        data.get('artist_id'),
                                        data.get('year'),
                                        data.get('duration')]
        curr.execute(song_table_insert,data_song)
    curr.close()

def get_user_info(data):
    '''
    Parses log file to get user data
    '''
    info = (data.get('userId'), 
            data.get('firstName'), 
            data.get('lastName'), 
            data.get('gender'), 
            data.get('level'))
    return info

def get_time_info(data):
    '''
    Parses log file to get time data
    '''
    weekday_dict = {'0':'Sunday',
               '1': 'Monday',
               '2': 'Tuesday',
               '3':'Wednesday',
               '4':'Thursday',
               '5': 'Friday',
               '6':'Saturday'}
    ts = data.get('ts')
    ts = ts/1000
    ts = datetime.fromtimestamp(ts)
    hour = int(ts.strftime("%H"))
    day = int(ts.strftime("%d"))
    week = int(ts.strftime("%V"))
    month = int(ts.strftime("%m"))
    year = int(ts.strftime("%Y"))
    weekday = weekday_dict[ts.strftime("%w")]
    time_stamp = ts.strftime("%X")
    start_time = f"{year}-{month}-{day} {time_stamp} zulu"
    info = (start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday)
    return info 

def get_log_data(paths_logs):
    '''
    Parses log files to a 
    list of dicts
    '''
    log_data = []
    for path in paths_logs:
        with open(path,'r') as f:
            log_data.extend(json.loads(x) for x in f.readlines())
    return log_data

def process_log_data(conn,paths_logs):
    '''
    Processes log data and
    inserts into songplays, users and
    time tables
    '''
    curr = conn.cursor()
    user_data = []
    time_data = []
    execute_query(conn,songplay_table_create)
    execute_query(conn,user_table_create)
    execute_query(conn,time_table_create)
    log_data = get_log_data(paths_logs)
    for index,row in enumerate(log_data):
        if row.get('page')=='NextSong':
            info_user = get_user_info(row)
            info_time = get_time_info(row)
            user_data.append(info_user)
            time_data.append(info_time)
            name = row.get('artist')
            title = row.get('song')
            duration = row.get('length')
            curr.execute(song_select,(name,title,duration))
            result = curr.fetchone()
            if result:
                song_id,artist_id = result
            else:
                song_id,artist_id = None, None
            vals = [index, info_time[0],info_user[0],
                    info_user[-1],song_id,artist_id,row.get('sessionId'),
                    row.get('location'),row.get('userAgent')]
            curr.execute(songplay_table_insert,vals)
            curr.execute(user_table_insert,get_user_info(row))
            curr.execute(time_table_insert,get_time_info(row))
    curr.close()
    



def main():
    conn = create_tables.create_conn(config.user_name,config.passwd,"sparkify")
    path_songs = get_paths("./data/song_data")
    path_logs = get_paths("./data/log_data")
    process_song_files(conn,path_songs)
    process_log_data(conn,path_logs)
    
    conn.close()

if __name__ == "__main__":
    main()