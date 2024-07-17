from src.connection.db import db
from src.utils.schemas import create_command
def tablecheck(tablename, schema, conn):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """

    simple_con_pool  = db(conn)
    con = simple_con_pool.getconn()
    cur = con.cursor()
    tableschema = schema

    try:
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
    cur.close()
    simple_con_pool.putconn(con,close=True)


def table_create(tablename, schema, conn):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    # d = dbc.db('maindev')
    simple_con_pool  = db(conn)
    con = simple_con_pool.getconn()
    cur = con.cursor()
    tableschema = schema
    try:
        comm = create_command(tablename, tableschema)
        cur.execute(comm)
        con.commit()

    except Exception as e:
        print(e)
        con.rollback()

    cur.close()
    simple_con_pool.putconn(con,close=True)
