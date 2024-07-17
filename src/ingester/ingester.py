from src.connection.db import db

import polars as pl
from io import StringIO
# import platform
from tqdm import tqdm
import sqlalchemy


def print_progress(dataframe, table, conn ,chunk_size=10000):
    '''
    create a wrapper around the chunk-ingester that prints out progress using tqdm
    args:
    dataframe: polars dataframe
    table: name of table in db
    conn: simple connection pool from psycopg2. note that we use con.getconn and
          con.putconn in this function. if ingesting multiple csvs, it'd be
          worth setting close=False so it recycles pooled connections
    chunksize(optional): rows per chunk of csv to be ingested
    '''
    simple_con_pool = conn
    con = simple_con_pool.getconn()
    cur = con.cursor()

    try:
        for i in tqdm(range(0, dataframe.shape[0], chunk_size)):
            f = StringIO()
            chunk = dataframe[i:(i + chunk_size)]
            f.write(chunk.write_csv(None, include_bom=True, include_header=False, separator='\t', null_value='\\N'))
            f.seek(0)
            # print(f.read())
            cur.copy_from(f, f'{table}', columns=[f'{i}' for i in dataframe.columns])
            con.commit()

    except sqlalchemy.exc.DBAPIError as e:
        print(e)
        cursor.rollback()
    cur.close()
    simple_con_pool.putconn(con,close=True)


def ingest(dataframe, table, connection):
    df = dataframe.clone()
    print_progress(df,table, connection)
