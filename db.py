from sqlalchemy import create_engine
import pandas as pd
import pyodbc,sqlite3
import config
from contextlib import contextmanager

class Db:
    def __init__(self,server = None,database = None,uid = None,pwd = None,access_path = None,sqlite_dbpath = None,driver = '{SQL Server}'):

        self.engine = None
        self.sqlite = False
        if access_path is None and sqlite_dbpath is None:
            connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};Trusted_Connection=yes"
            odbc_prefix = "mssql+pyodbc:///?odbc_connect="
        elif sqlite_dbpath is not None:
            connection_string = sqlite_dbpath
            odbc_prefix = "sqlite:///"
            self.sqlite = True
        else:
            connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_path}"
            odbc_prefix = "access+pyodbc:///?odbc_connect="

        self.engine = create_engine(f"{odbc_prefix}{connection_string}")
        self.connection_string = connection_string
    
    def handle_encoding_error(self,function):

        # Access sucks
        def decode_sketchy_utf16(raw_bytes):
            s = raw_bytes.decode("utf-16le", "ignore")
            try:
                n = s.index('\u0000')
                s = s[:n]  # respect null terminator
            except ValueError:
                pass
            return s
        
        prev_converter = self.connection.get_output_converter(pyodbc.SQL_WVARCHAR)
        self.connection.add_output_converter(pyodbc.SQL_WVARCHAR, decode_sketchy_utf16) # restore previous behaviour
        ## Running function
        try:
            return function()
        ## Running function
        finally:
            self.connection.add_output_converter(pyodbc.SQL_WVARCHAR, prev_converter)

    def old_connect(self):
        try:
            if not self.sqlite:
                self.connection = pyodbc.connect(self.connection_string)
            else:
                self.connection = sqlite3.connect(self.connection_string)
            self.cursor = self.connection.cursor()
        except pyodbc.Error as e:
            print(f"Error connecting to the database: {e}")
    
    @contextmanager
    def connect(self) -> sqlite3.Connection: # type: ignore
        if not self.sqlite:
            connection = pyodbc.connect(self.connection_string)
        else:
            connection = sqlite3.connect(self.connection_string)
        yield connection
        connection.close()
    
    def execute(self,query:str,result = False,operation = "operation",df = True):

        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            if operation != "select":
                connection.commit()
            if result:
                try:
                    if not df:
                        return cursor.fetchall()
                    rows = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(rows, columns=column_names)
                except UnicodeDecodeError:
                    return self.handle_encoding_error(self.cursor.fetchall)
    
    def operation(self,query,operation,df = True):
        if operation == "select":
            return self.execute(query,result=True,operation=operation,df = df)
        else:
            self.execute(query,operation=operation)
            print(f"{operation} operation succesful.")

    def select(self, query,df = True):
        return self.operation(query,"select",df)

    def insert(self, query):
        self.operation(query,"insert")

    def update(self, query):
        self.operation(query,"update")

class sqlServerDb(Db):
    def __init__(self, server=None, database=None, uid=None, pwd=None, access_path=None):
        super().__init__(server, database, uid, pwd, access_path,driver)

class dbAtivos(Db):
    def __init__(self, server=None, database=None, uid=None, pwd=None, access_path=None, sqlite_dbpath=None, driver='{SQL Server}'):
        sqlite_dbpath = config.db_path
        super().__init__(server, database, uid, pwd, access_path, sqlite_dbpath, driver)
    
    def get_feriados(self):

        return self.select(
            "Select * from Feriados"
        )

    def update_feriados(self):

        link = "https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls"
        Table = "Feriados"
        df = pd.read_excel(link)
        df.dropna(inplace=True)
        df['Data'] = df['Data'].apply(lambda dt:dt.date())

        df.to_sql(
            Table,db.engine,if_exists='replace',index=False
        )
    
    def delete_if_exist(self,df:pd.DataFrame,Table:str,keyCols:list):
        
        sql = f"""
            DELETE From {Table} where {",".join(keyCols)}
        """
    
