create_keyspace = '''
                    CREATE KEYSPACE IF NOT EXISTS test
                    WITH REPLICATION =
                    {'class': 'SimpleStrategy','replication_factor':1}
                  '''
create_table = '''
CREATE TABLE IF NOT EXISTS music_library 
(year int,artist_name text,album_name text,PRIMARY KEY(year,artist_name))
               '''

insert_rows = '''
INSERT INTO music_library (year,artist_name,album_name) 
VALUES (%s,%s,%s)
              '''