import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    responsible for staging pre-existing tables to ensure that our database does not throw any error
    Parameters:
        curr: cursor for the database
        conn: database connection made using conection parameters 
    Returns:
        None
   """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    responsible for inserting data to  tables
    Parameters:
        curr: cursor for the database
        conn: database connection made using conection parameters 
    Returns:
        None
   """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    this function uses configuration parameters to create database connection and loads and insert data into staging table
   """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()