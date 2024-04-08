import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

SERVER = ''
DATABASE = ''
USERNAME = ''
PASSWORD = ''


def extactsqlserv():
    try:
        connection = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection})
        src_engine = create_engine(connection_url)
        src_tables = ['Production.Product', 
                      'Production.ProductSubcategory', 
                      'Production.ProductCategory', 
                      'Sales.SalesTerritory']
        with src_engine.connect() as conn:
            for table in src_tables:
                df = pd.read_sql_query(f'select * FROM {table}', conn)  
                load(df, table) 
    except Exception as e:
        print("Data extract error: " + str(e))


def load(df, table):
    try:
        engine =f'postgresql://{USERNAME}:{PASSWORD}@localhost:5432/AdventureWorks'
        df.to_sql(f'stg_{table}', engine, if_exists='replace', index=False, chunksize=100000)
    except Exception as e:
        print("Data load error: " + str(e))
    return None

if __name__ == "__main__":
    extactsqlserv()
