import polars as pl
import os
from datetime import datetime


def dataframe_assembler(df,schema):
    # read raw csv
    tempdf = df.clone()
    # read schemaplan
    # add any missing column with null values
    cols = tempdf.columns
    newcols = cols
    for field in schema["Field"].to_numpy().tolist():
        if field not in [i for i in tempdf.columns]:
            ordernum = schema['Field'].to_numpy().tolist().index(field)
            newcols.insert(ordernum, pl.lit(None).alias(field))
    tempdf = tempdf.select(cols)
    return tempdf





def assemble(tablename):
    dir = os.path.normpath(os.path.join(os.getcwd(),'talltables/'))
    os.listdir(dir)
    tallfiles = {
        os.path.splitext(i)[0]:os.path.normpath(f"{dir}/{i}") for
            i in os.listdir(dir) if not i.endswith(".xlsx")
            and not i.endswith(".ldb")
            and not i.startswith("~$")
            }
    schema = schema_chooser(tablename)

    tempdf = pl.read_csv(
        tallfiles[tablename],

    )
    tempdf = dataframe_assembler(tempdf, schema)

    # needs
    return tempdf

def schema_todict(tablename):
    sche = schema_chooser(tablename)
    fieldtypes = {}
    datatypes = schema_chooser(tablename)['DataType'].to_numpy().tolist()
    fields = schema_chooser(tablename)['Field'].to_numpy().tolist()
    for field in fields:
        for type in datatypes:
            fieldtypes[field] = type
            datatypes.remove(type)
            break
    return fieldtypes

def pg2pandas(pg_schema):
    trans = {
        "text":"object",
        "varchar": "object",
        "geometry": "object",
        "integer":"Int64",
        "bigint":"Int64",
        "bit":"object",
        "smallint":"Int64",
        "real":"float64",
        "double precision":"float64",
        "numeric":"float64",
        "public.geometry":"object",
        "date":"object", 
        "timestamp":"object" 
    }
    return {k:trans[v.lower()] for k,v in pg_schema.items()}

def schema_chooser(tablename, path=None):
    path = "./schemas/LDC_SchemaPlan_1.1.9.csv"
    if path:
        excel_dataframe = pl.read_csv(path, encoding='unicode_escape' )
        return excel_dataframe.filter(pl.col("Table")==tablename)
    else:
        print("Not Implemented")
        return None

# def table_create():


def create_command(tablename,dbschema):
    """
    creates a complete CREATE TABLE postgres statement
    by only supplying tablename
    currently handles: HEADER, Gap
    """

    str = f'CREATE TABLE {dbschema}."{tablename}" '
    str+=field_appender(tablename)
    str = table_fixes(str, tablename)

    return str

def table_fixes(str,tablename):
    """
    adds primary key constraint with the rid SERIAL
    adds project reference to header if necessary
    fixes height, aero and tblRHEM
    geometry type fix: public_dev is not postgis enabled; https://stackoverflow.com/a/55408170
    """
    fix = str.replace('" ( ', '" ( rid SERIAL PRIMARY KEY,')
    if "Header" not in tablename:
        fix = fix.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT REFERENCES gisdb.public_test."dataHeader"("PrimaryKey"), ')
        # return fix
    else:
        fix = fix.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT PRIMARY KEY,')
        fix = fix.replace('PUBLIC.GEOMETRY,', 'PUBLIC.GEOMETRY(POINT, 4326),')
        # return fix
    if "aero" in tablename or "RHEM" in tablename:
        fix = fix.replace('"ProjectKey" TEXT, ', ' ')
    if "Project" in tablename:
        fix = fix.replace('"ProjectKey" TEXT, ', '"ProjectKey" TEXT PRIMARY KEY REFERENCES gisdb.public_test."dataHeader"("ProjectKey"), ')
        # return fix
    if "Height" in tablename:
        fix = fix.replace('" ( ', '" ( ri)"')
    fix = fix.replace('"DateLoadedInDB" DATE','"DateLoadedInDb" DATE',)
    fix = fix.replace('"DateLoadedInDb" DATE','"DateLoadedInDb" DATE DEFAULT CURRENT_DATE')

    return fix

def field_appender(tablename):
    """
    uses schema_chooser to pull a schema for a specific table
    and create a postgres-ready string of fields and field types
    """

    str = "( "
    count = 0

    datatypes = schema_chooser(tablename)['DataType'].to_numpy().tolist()
    fields = schema_chooser(tablename)['Field'].to_numpy().tolist()

    di = {}
    for field in fields:
        for type in datatypes:
            di[field] = type
            datatypes.remove(type)
            break

    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    return str
