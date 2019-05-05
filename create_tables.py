import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute(""" 
                CREATE TABLE staging_events(
                artist varchar(100) NULL,
                auth varchar(50) NULL,
                first_name varchar(50) NULL,
                last_name varchar(50) NULL,
                length float NULL,
                level varchar(50) NULL,
                location varchar(250) NULL,
                method varchar(50) NULL,
                page varchar(50) NULL,
                registration decimal(16,2) NULL,
                session_id int NULL,
                song varchar(250) NULL,
                status int NULL,
                ts int NULL,
                user_agent varchar(500) NULL,
                user_id varchar(50) NULL)
                """)
    conn.commit()
#     drop_tables(cur, conn)
#     create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
