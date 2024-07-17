from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool
from sqlalchemy import create_engine, URL

def config(filename='src/connection/db.ini', section='local'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db

def db(sec):
    if "local" in sec:
        params = config(section=sec)
        params['options'] = "-c search_path=public_test"
        return SimpleConnectionPool(minconn=1,maxconn=10,**params)


def engine(string):
    d = config(section=f'{string}')
    return create_engine(URL.create(
        "postgresql+psycopg2",
        username=d["user"],
        password=d["password"],
        host=d["host"],
        port=d["port"],
        database=d["dbname"]
    ))
