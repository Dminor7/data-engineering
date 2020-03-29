import log
import glob
import json
import ndjson
from string_iterator import StringIteratorIO
import psycopg2
from config import config
import sql_queries
from metrics import *
from datetime import datetime
import copy

# GLOBAL LOGGER
logger = log.get_logger(__name__,'etl.log')

@profile
def make_connection(database):
    configuration = config()
    configuration['database'] = database
    try:
        connection = psycopg2.connect(**configuration)
        connection.autocommit = True
    except(Exception) as e:
        logger.exception("Error occured {}".format(e))
    else:
        return connection

@profile
def get_data(location):
    try:
        files = glob.iglob(location + "**/*.json" ,recursive=True)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))
    
    if("song_data" in location):
        song_data = []
        for file in files:
            with open(file) as f:
                data = json.loads(f.read())
                song_data.append(data)
        return song_data
    
    if("log_data" in location):
        log_data = []
        for file in files:
            with open(file) as f:
                data_list = ndjson.load(f)
                for data in data_list:
                    log_data.append(data)
        return log_data

@profile
def get_lookup(songs):
    song_map = {}
    for song in songs:
        title = song['title'].lower()
        artist_name = song['artist_name'].lower()
        song_id = song['song_id']
        artist_id = song['artist_id']
        key = (title,artist_name)
        value = (song_id,artist_id)
        song_map[key] = value
    return song_map

@profile
def parse_logs(logs,song_map):
    logs = filter(lambda d: d['page'] == "NextSong" ,logs)
    data = []
    for log in logs:
        if(song_map.get((log['song'].lower(),log['artist'].lower()))):
            song_id,artist_id = (log['song'],log['artist'])
            new_log = copy.deepcopy(log)
            new_log['song_id'] = song_id
            new_log['artist_id'] = artist_id
            data.append(new_log)
    else:
        new_log = copy.deepcopy(log)
        new_log['song_id'] = None
        new_log['artist_id'] = None
        data.append(new_log)
    return data


def copy_string_iterator(connection, table_name, data, size = 1024):
    with connection.cursor() as cursor:
        cursor.copy_from(data, table_name, sep="|", size=size)

@profile
def insert_song_data(connection,data):

    song_data = ('|'.join(map(str,
        [ 
            d['song_id'],
            d['title'], 
            d['artist_id'], 
            d['year'],
            d['duration']
        ]
    )
        ) + "\n" 
    for d in data)

    artist_data = ('|'.join(map(str,
        [ 
            d['artist_id'],
            d['artist_name'], 
            d['artist_location'], 
            d['artist_latitude'],
            d['artist_longitude']
        ]
    )
        ) + "\n" 
    for d in data)

    
    song_iterator   = StringIteratorIO(song_data)
    try:
        copy_string_iterator(connection,"songs",song_iterator)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))


    artist_iterator = StringIteratorIO(artist_data)
    try:
        copy_string_iterator(connection,"artists",artist_iterator)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))

@profile
def insert_log_data(connection,data):

    time_data = ("|".join(map(str,
        [
            datetime.fromtimestamp(d['ts']/1000),
            datetime.fromtimestamp(d['ts']/1000).hour,
            datetime.fromtimestamp(d['ts']/1000).day,
            datetime.fromtimestamp(d['ts']/1000).strftime("%W"),
            datetime.fromtimestamp(d['ts']/1000).month,
            datetime.fromtimestamp(d['ts']/1000).year,
            datetime.fromtimestamp(d['ts']/1000).strftime("%A")
        ]
    )) + "\n" for d in data)

    user_data = ("|".join(map(str,
        [
            d['userId'],
            d['firstName'],
            d['lastName'],
            d['gender'],
            d['level']
        ]
    )) + "\n" for d in data)

    song_play_data = ("|".join(map(str,
        [   
            i,
            datetime.fromtimestamp(d['ts']/1000),
            d['userId'],
            d['level'],
            d['song_id'],
            d['artist_id'],
            d['sessionId'],
            d['location'],
            d['userAgent']
        ]
    )) + "\n" for i,d in enumerate(data))

    time_iterator = StringIteratorIO(time_data)
    try:
        copy_string_iterator(connection,"time",time_iterator)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))


    user_iterator = StringIteratorIO(user_data)
    try:
        copy_string_iterator(connection,"users",user_iterator)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))


    song_play_iterator = StringIteratorIO(song_play_data)
    try:
        copy_string_iterator(connection,"songplays",song_play_iterator)
    except(Exception) as e:
        logger.exception("Error Occured {}".format(e))

def main():

    try:
        # MAKE CONNECTION TO DATABASE
        logger.info("Connecting to {0} database".format(sql_queries.database_name))
        connection = make_connection(sql_queries.database_name)
        logger.info("Connected to database {}".format(sql_queries.database_name))
        
        # EXTRACT DATA
        logger.info("EXTRACT")
        logger.info("Retriving JSON files")
        songs = get_data("data/song_data/")    
        logs = get_data("data/log_data/")
        logger.info("JSON files recievied")
        logger.info("EXTRACT DONE")
    
        # TRANSFORM DATA
        logger.info("TRANFORM")
        song_map = get_lookup(songs)
        logs = parse_logs(logs,song_map)
        logger.info("TRANSFORM DONE")
    
        # LOAD DATA
        logger.info("LOAD")

        try:
            insert_song_data(connection,songs)
            logger.info("Data inserted into songs table")
            logger.info("Data inserted into artists table")
        except(Exception) as e:
            logger.exception("Error occured {}".format(e))

        try:    
            insert_log_data(connection,logs)
            logger.info("Data inserted into time table")
            logger.info("Data inserted into users table")
            logger.info("Data inserted into songplays table")
            
        except(Exception) as e:
            logger.exception("Error occured {}".format(e))

        logger.info("LOAD DONE")
    
    except(Exception) as e:
        logger.exception("Error occured {}".format(e))

    
    # CLOSING CONNECTION
    try:
        connection.close()
        logger.info("Connection closed")
    except(Exception) as e:
        logger.exception("Error occured {}".format(e))    



if (__name__ == "__main__"):
    main()
    