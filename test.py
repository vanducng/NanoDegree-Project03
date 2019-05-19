import configparser
import psycopg2
from sql_queries import select_queries
import pandas as pd


def analysis_query(cur, conn):
    """
    Run the analysis test to evaluate the data integration result
    """
    for query in select_queries:
        df = pd.read_sql_query(query, conn)
        conn.commit()
        print(df.to_string())


def run_test():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    analysis_query(cur, conn)

    conn.close()


if __name__ == "__main__":
    run_test()
