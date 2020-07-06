import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Function that loads two staging tables with data from S3
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('load_staging_tables: complete')

# Function that loads final star schema tables with data from staging tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('insert_tables: complete')


def main():
    # Read from dwh.cfg using configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to AWS Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load staging tables, then load final star schema tables with data from staging tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
