import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# Function that deletes all tables
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('drop_tables: complete')


# Function that creates all tables
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('create_tables: complete')


def main():
    # Read from dwh.cfg using configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to AWS Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop all tables, in case there is any, and create all tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
