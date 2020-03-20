import cassandra
from cassandra.cluster import Cluster
import cqueries as q

def connect():
    try:
        cluster = Cluster(["127.0.0.1"])
        session = cluster.connect()
        print("Connected")
    except(Exception) as e:
        print("Error ",e )
    
    
    # Create a workspace
    try:
        print("Creating Keyspace")
        session.execute(q.create_keyspace)
        print("KeySpace Created")
    except(Exception) as e:
        print(e)
    
    try:
        session.set_keyspace('test')
        print("Session set to keyspace 'test'")
    except(Exception) as e:
        print(e)
    
    '''
    We want to query every album that was released in the particular year
    select * from music_library WHERE YEAR=1970
    To do that:
        ->We need to be able to do a WHERE on YEAR.
        ->So, YEAR will become my partition key,
        ->artist name will be my clustering column to make each Primary Key unique.
        
            Table Name: music_library
            column 1: Album Name
            column 2: Artist Name
            column 3: Year
            PRIMARY KEY(year, artist name)
    '''
    # Create table
    try:
        session.execute(q.create_table)
        print("Table created")
    except(Exception) as e:
        print(e)

    # Insert rows
    try:
        session.execute(q.insert_rows,(1970, "The Beatles", "Let it Be"))
        print("row 1 inserted")
        session.execute(q.insert_rows,(1965, "The Beatles", "Rubber Soul"))
        print("row 2 inserted")
    except(Exception) as e:
        print(e)
    
    # Validate data inserted in table
    query = 'SELECT * FROM music_library'
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    print("Validating data")
    for row in rows:
        print (row.year, row.album_name, row.artist_name)

    # Validate data model query
    query = "select * from music_library WHERE YEAR=1970"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    print("Validating data model")    
    for row in rows:
        print (row.year, row.album_name, row.artist_name)

    # Drop the table
    query = "drop table music_library"
    try:
        rows = session.execute(query)
        print("Table Dropped")
    except Exception as e:
        print(e)

    session.shutdown()
    cluster.shutdown()

if(__name__ == "__main__"):
    connect()
    