import configparser
import psycopg2
from sql_queries import create_tables_list, drop_table_queries, copy_table_queries


def drop_tables(cur, conn):
    """
    Procedure to drop tables.
    INPUTS:
        cur -> psycopg2 cursor to execute command.
        conn -> connection to postgresql database for committing executed action/command
    Returns none.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Procedure to create tables.
    INPUTS:
        cur -> psycopg2 cursor to execute command.
        conn -> connection to postgresql database for committing executed action/command
    Returns none.
    """
    for query in create_tables_list:
        cur.execute(query)
        conn.commit()
        
        

        

def main():
    """
    Procedure to define the connection and cursor arguments used above in create_ and drop_tables functions
    It executes the create_tables and drop_tables functions.
    It closes the connection.
    Returns None.
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
