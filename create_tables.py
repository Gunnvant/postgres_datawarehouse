import psycopg2
from sql_queries import create_table_queries,drop_table_names,drop_table_query
from config import user_name,passwd
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def create_conn(user_name,passwd,db_name='postgres'):
    conn = psycopg2.connect(f"dbname= {db_name} user={user_name} password={passwd}")
    conn.set_session(autocommit=True)
    return conn

def drop_if_db_exists(conn,db_name):
    cur = conn.cursor()
    try:
        logging.info(f"Checking if {db_name} exists already")
        cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    except Exception as  e:
        logging.error(f"Could not drop database {db_name}, error {e}")
    cur.close()

def createdb(conn,db_name):
    cur = conn.cursor()
    try:
        logging.info(f"Creating database {db_name}")
        cur.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'utf8' TEMPLATE template0")
    except Exception as e:
        logging.error(f"Could not create database {db_name}, error {e}")
    cur.close
    


def drop_tables(conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    curr = conn.cursor()
    for name in drop_table_names:
        query = drop_table_query(name)
        curr.execute(query)
    curr.close()


def create_tables(conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    curr = conn.cursor()
    for query in create_table_queries:
        curr.execute(query)
    curr.close()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database 
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    conn = create_conn(user_name,passwd)
    drop_if_db_exists(conn,"sparkify")
    createdb(conn,"sparkify")
    conn.close()
    
    conn = create_conn(user_name,passwd,"sparkify")
    drop_tables(conn)
    create_tables(conn)
    conn.close()


if __name__ == "__main__":
    main()