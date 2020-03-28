import psycopg2
import sql_queries
from config import config
import log

logger = log.get_logger(__name__, 'create_tables.log')

def create_database():
    '''Creates and connects to sparkifydb database. Returns cursor and connection to DB'''
    # connect to default database
    try:
        logger.info("Connecting to default database")
        connection = psycopg2.connect(**config())
        logger.info("Connected to default database")
    except (Exception) as e:
        logger.exception("Error occured while connecting default database, {0} ".format(e))

    else:    
        connection.set_session(autocommit=True)
        cursor = connection.cursor()
        
        # create sparkify database with UTF8 encoding
        try:
            logger.info("Dropping {0} database if exist".format(sql_queries.database_name))
            
            cursor.execute(sql_queries.drop_database)
            
            logger.info("{0} database dropped".format(sql_queries.database_name))
            logger.info("Creating {0} database if not exist".format(sql_queries.database_name))
            
            cursor.execute(sql_queries.create_database)
            
            logger.info("{0} database created".format(sql_queries.database_name))
        except (Exception) as e:
            logger.exception("Error occured {0}".format(e))
        else:
            # close connection to default database
            logger.info("Closing default connection")
            connection.close()    
            
        # connect to sparkify database

    configuration = config()
    configuration['database'] = sql_queries.database_name
    
    try:
        logger.info("Connecting to {0} database".format(sql_queries.database_name))
        connection = psycopg2.connect(**configuration)
        logger.info("Connected to {0} database".format(sql_queries.database_name))
    except (Exception) as e:
        logger.exception("Error occured, {0}".format(e))
    
    else:
        cursor = connection.cursor()
        return cursor, connection


def drop_tables(cursor, connection):
    '''Drops all tables created on the database'''
    logger.info("Dropping all tables")
    for query in sql_queries.drop_table_queries:
        try:
            cursor.execute(query)
            connection.commit()
        except(Exception) as e:
            logger.exception("Error occured {0}".format(e))
    


def create_tables(cursor, connection):
    '''Created tables defined on the sql_queries script: [songplays, users, songs, artists, time]'''
    logger.info("Creating all tables")
    for query in sql_queries.create_table_queries:
        try:
            cursor.execute(query)
            connection.commit()
        except (Exception) as e:
            logger.exception("Error occured {0}".format(e))


def main():
    """ Function to drop and re create sparkifydb database and all related tables.
        Usage: python create_tables.py
    """
    cursor, connection = create_database()
    
    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    logger.info("Closing connection")
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()