import psycopg2
from config import config
import queries as q
# Making Connection to PostgreSql


def make_connection(query=None):
    try:
        connection = psycopg2.connect(**config())
        cursor = connection.cursor()

        # Print PostgreSql connection properties
        print(connection.get_dsn_parameters())

        # Print PostgreSql version
        cursor.execute('SELECT version()')
        record = cursor.fetchone()
        print("You are connected to - ",record)

        if(query):
            try:
                cursor.execute(query)
                connection.commit()
                print("Query executed successfully")
            except(Exception,psycopg2.Error) as error:
                print("Error while executing query ",error)
        
    
    except(Exception,psycopg2.Error) as error:
        print("Error while connecting to PostgreSql ",error)

    finally:
        # Closing database connection
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSql connection is closed")

def create_table(table_name,columns):
    '''
    Returns a query that creates a table for given table_name and 
    columns with data-type {'column_name1':'data-type','column_name2':'data-type'} 
    '''
    values = []
    
    for key,value in columns.items():
        values.append(key + " " + value)
    
    total_columns = len(values)
    insert_at = q.create_table.find('#')
    
     
    s = ''
    for i in range(total_columns):
        s += "{"+str(i+1)+"}, "
    
    
    query = q.create_table[:insert_at] + s[:len(s)-2]  + q.create_table[insert_at + 1:]
    create_table_query = query.format(table_name,*values)

    return create_table_query
    
def insert_rows(table_name,data):
    try:
        connection = psycopg2.connect(**config())
        cursor = connection.cursor()
    except(Exception,psycopg2.Error) as error:
        print("Error connecting",error)
    
    if(cursor):
        try:
            args_str = ','.join(['%s'] * len(data))
            insert_query = cursor.mogrify(q.insert_row.format(table_name,args_str),data)            
        except(Exception,psycopg2.Error) as error:
            print("Error",error)

        cursor.close()
        connection.close()
        return insert_query

def check():
    try:
        connection = psycopg2.connect(**config())
        cursor = connection.cursor()
    except(Exception,psycopg2.Error)as e: 
        print("Error: select *")
        print (e)


    try: 
        cursor.execute("SELECT * FROM music_library;")
        connection.commit()
    
    except(Exception,psycopg2.Error)as e: 
        print("Error: select *")
        print (e)

    row = cursor.fetchone()
    
    while row:
        print(row)
        row = cursor.fetchone()
    
    cursor.close()
    connection.close()   


if(__name__ == "__main__"):
    # make_connection()
    # columns = {'artist_name':'varchar','album_name':'varchar','year':'int'}
    # table_name = 'music_library'
    # data = [('The Beatles','Let It Be',1970),("The Beatles", "Rubber Soul",  1965)]
    # make_connection(create_table(table_name,columns))
    # make_connection(insert_rows(table_name,data))
    pass
        