import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Copy the staging tables to S3 bucket: staging_events and staging_songs'''
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(query + ' - Done!')
        except:
            print(query + ' - Fail!')
            pass

def insert_tables(cur, conn):
    '''Load the values from staging table into the tables: songplays, users, songs, artists, time'''
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(query + ' - Done!')
        except:
            print(query + ' - Fail!')
            pass

def main():
    '''Read AWS requirements from dwh.cfg file'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    '''Connect to the Redshift cluster'''
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    '''Run above functions according to sql_queries.py'''
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()