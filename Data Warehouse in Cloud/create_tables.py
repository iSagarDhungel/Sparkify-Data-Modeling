import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    responsible for deleting pre-existing tables to ensure that our database does not throw any error
    Parameters:
        curr: cursor for the database
        conn: database connection made using conection parameters 
    Returns:
        None
   """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    responsible for creating pre-existing tables
    Parameters:
        curr: cursor for the database
        conn: database connection made using conection parameters 
    Returns:
        None
   """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    this function uses configuration parameters to create database connection and cursor to drop and create tables
   """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()