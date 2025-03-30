import os
import sqlite3
import numpy as np
import pandas as pd
from glob import glob

sqlite3.register_adapter(np.int64, lambda x: int(x))
sqlite3.register_adapter(np.int32, lambda x: int(x))

PATH_DB = 'data/nyc_health.sqlite'    
PATH_TO_LOAD = 'data/to_load/'
PATH_LOADED = 'data/loaded/'
FILE_PATTERN = 'nyc*.csv'

class BaseDB:
    
    def __init__(self, 
                 path_db: str,
                 create: bool = False
                ):

        # Internal flag to indicate if we are connected to the database
        self._connected = False

        # Normalize path format (e.g., windows vs. mac/linux)
        self.path = os.path.normpath(path_db)

        # Check if the database exists, then either create it
        # or throw an error if create=False
        self._check_exists(create)
        return
        
    def run_query(self,
                  sql: str,
                  params: dict = None,
                  keep_open: bool = False
                 ) -> pd.DataFrame:

        # Make sure we have an active connection
        self._connect()

        try:
            # Run the query
            results = pd.read_sql(sql, self._conn, params=params)
        except Exception as e:
            raise type(e)(f'sql: {sql}\nparams: {params}') from e
        finally:
            if not keep_open:
                self._close()
        
        return results

    def run_action(self,
                   sql: str,
                   params: dict = None,
                   keep_open: bool = False
                  ) -> int:
        # print('running an action')
        # Make sure we have an active connection
        self._connect()
    
        try:
            if params is not None:
                self._curs.execute(sql, params)
            else:
                self._curs.execute(sql)
        except Exception as e:
            self._conn.rollback()
            self._close()
            raise type(e)(f'sql: {sql}\nparams: {params}') from e
        finally:
            if not keep_open:
                self._close()
        
        return self._curs.lastrowid
        
    def _check_exists(self, create: bool) -> None:
        '''
        Check if the database file (and all directories in the path)
        exist. If not create them if create=True, or raise an error
        if create=False.
        
        If database did not exist, set self._existed=False, otherwise
        set self._existed=True.
        '''

        self._existed = True

        # Split the path into individial directories, etc.
        path_parts = self.path.split(os.sep)

        # Starting in the current directory,
        # check if each subdirectory, and finally the database file, exist
        n = len(path_parts)
        for i in range(n):
            part = os.sep.join(path_parts[:i+1])
            if not os.path.exists(part):
                self._existed = False
                if not create:
                    raise FileNotFoundError(f'{part} does not exist.')
                if i == n-1:
                    print('Creating db')
                    self._connect()
                    self._close()
                else:
                    os.mkdir(part)
        return

    def _connect(self) -> None:
        if not self._connected:
            self._conn = sqlite3.connect(self.path)
            self._curs = self._conn.cursor()
            self._curs.execute("PRAGMA foreign_keys=ON;")
            self._connected = True
        return

    def _close(self) -> None:
        self._conn.close()
        self._connected = False
        return

class NYCDB(BaseDB):
    def __init__(self, 
                 create: bool = True
                ):
        # Call the constructor for the parent class
        super().__init__(PATH_DB, create)

        # If the database did not exist, we need to create it
        if not self._existed:
            self._create_tables()
        
        return

    def _create_tables(self) -> None:
        sql = """
            CREATE TABLE tCuisine (
                cuisine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cuisine_desc TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)
        print('tCuisine maybe created')

        sql = """
            CREATE TABLE tViol (
                viol_id TEXT NOT NULL PRIMARY KEY,
                viol_desc TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)
        print('tViol maybe created')

        sql = """
            CREATE TABLE tAction (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_desc TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)
        print('tAction maybe created')
        
        sql = """
            CREATE TABLE tRest (
                camis INTEGER PRIMARY KEY,
                dba TEXT NOT NULL,
                boro TEXT NOT NULL,
                building TEXT NOT NULL,
                street TEXT NOT NULL,
                zip INTEGER,
                phone INTEGER,
                cuisine_id INTEGER NOT NULL REFERENCES tCuisine(cuisine_id)
            )
            ;"""
        self.run_action(sql)
        print('tRest maybe created')

        sql = """
            CREATE TABLE tInsp (
                camis INTEGER NOT NULL REFERENCES tRest(camis),
                insp_date TEXT NOT NULL CHECK(insp_date LIKE '__/__/____'),
                insp_time TEXT NOT NULL CHECK(insp_time LIKE '__:__:__'),
                viol_id TEXT NOT NULL REFERENCES tViol(viol_id),
                action_id INTEGER NOT NULL REFERENCES tAction(action_id),
                PRIMARY KEY (camis, insp_date, insp_time, viol_id)
            )
            ;"""
        self.run_action(sql)
        print('tInsp maybe created')
        
        return


    def load_new_data(self) -> None:
        '''
        Check if there are any files that need to be loaded
        into the database, and pass them to load_nyc_file
        '''

        files = glob(PATH_TO_LOAD + FILE_PATTERN)

        for file in files:
            print(f'Loading {file}')
            try:
                self.load_nyc_file(file)
                # If the file loaded succesfully, move it into the loaded directory
                os.rename(file, file.replace(PATH_TO_LOAD, PATH_LOADED))
            except Exception as e:
                type(e)(f'Problem loading file: {file}')
            
        return
        
    def revert(self) -> None:
        files = glob(PATH_LOADED + FILE_PATTERN)
        
        for file in files:
            os.rename(file, file.replace(PATH_LOADED, PATH_TO_LOAD))
        return

    def load_nyc_file(self,
                            file_path: str
                           ) -> None:
        '''
        Clean and load a nyc*.csv into the database
        '''
    
        df = pd.read_csv(file_path)
        df.rename(columns={'CUISINE DESCRIPTION': 'cuisine_desc',
                           'INSPECTION DATE': 'insp_date',
                           'ACTION':'action_desc',
                           'VIOLATION CODE':'viol_id',
                           'VIOLATION DESCRIPTION':'viol_desc'}, 
                  inplace=True)
        #print(df.head())
        np.random.seed(7)
        h = np.random.randint(9, 17+1, size = df.shape[0])
        m = np.random.randint(0, 60+1, size = df.shape[0])
        s = np.random.randint(0, 60+1, size = df.shape[0])
        
        t = [f'{str(hi).zfill(2)}:{str(mi).zfill(2)}:{str(si).zfill(2)}' for (hi,mi,si) in zip(h,m,s)]
        df['insp_time'] = t

        for row in (df.to_dict(orient='records')):
            #print(row)
            
            cuisine_id = self.get_cuisine(row['cuisine_desc'])
            
            #print(row['cuisine_desc'])
            #print(f'cuisine id: {cuisine_id}')
            
            action_id = self.get_action(row['action_desc'])
            
            #print(row['action_desc'])
            #print(f'action id: {action_id}')

            #print('load viol start')
            
            self.load_viol(row['viol_id'],
                           row['viol_desc'])
            
            #print('load viol done')
            #print('load rest start')
            
            self.load_rest(row['CAMIS'],
                           row['DBA'],
                           row['BORO'],
                           row['BUILDING'],
                           row['STREET'],
                           row['ZIPCODE'],
                           row['PHONE'],
                           cuisine_id)
            
            #print('load rest done')
            #print('load insp start')
            #print(row['CAMIS'])
            #print(row['insp_date'])
            #print(row['insp_time'])
            #print(row['viol_id'])
            #print(action_id)
            
            self.load_insp(row['CAMIS'],
                           row['insp_date'],
                           row['insp_time'],
                           row['viol_id'],
                           action_id)
            
            #print('load insp done')
            
                           
            

        self._conn.commit()
        self._close()
        return
    
    def get_cuisine(self, 
                    cuisine: str
                   ) -> int:
        '''
        Get (and create if needed) a cuisine_id.
        '''
        sql_select = """
            SELECT cuisine_id
            FROM tCuisine
            WHERE cuisine_desc = :cuisine_desc
        ;"""
    
        sql_insert = """
            INSERT INTO tCuisine (cuisine_desc)
            VALUES (:cuisine_desc)
        ;"""
        
        params = {'cuisine_desc': cuisine}
    
        # Will return a cuisine_id if it exists,
        # otherwise the dataframe will be empty
        query = self.run_query(sql_select, params, keep_open=True)
        
        # Create the cuisine_id if it did not exist
        if len(query) == 0:
            cuisine_id = self.run_action(sql_insert, params, keep_open=True)
        else:
            cuisine_id = query.values[0][0]
        
        return cuisine_id
        
    def get_action(self,
                  action_desc: str
                  ) -> int:
        '''
        Get (and create if needed) an action_id.
        '''
        
        sql_select = """
            SELECT action_id
            FROM tAction
            WHERE action_desc = :action_desc
        ;"""
    
        sql_insert = """
            INSERT INTO tAction (action_desc)
            VALUES (:action_desc)
        ;"""
        
        params = {'action_desc': action_desc}
    
        # Will return an action_id if it exists,
        # otherwise the dataframe will be empty
        query = self.run_query(sql_select, params, keep_open=True)
        
        # Create the cuisine_id if it did not exist
        if len(query) == 0:
            action_id = self.run_action(sql_insert, params, keep_open=True)
        else:
            action_id = query.values[0][0]
        
        return action_id

    def load_rest(self,
                 camis,
                 dba,
                 boro,
                 building,
                 street,
                 zip,
                 phone,
                 cuisine_id
                 ):
        
        #print('running insert')
        
        sql_select = """
            SELECT camis, dba, boro, building, street, zip, phone, cuisine_id
            FROM tRest
            WHERE camis = :camis
        ;"""
        
        sql_insert = """
            INSERT INTO tRest (camis, dba, boro, building, street, zip, phone, cuisine_id)
            VALUES (:camis, :dba, :boro, :building, :street, :zip, :phone, :cuisine_id)
        ;"""
        
        params = {'camis': camis, 
                  'dba': dba, 
                  'boro': boro, 
                  'building': building, 
                  'street': street, 
                  'zip': zip, 
                  'phone': phone, 
                  'cuisine_id': cuisine_id
                 }
        # Will return an action_id if it exists,
        # otherwise the dataframe will be empty
        query = self.run_query(sql_select, params, keep_open=True)
        
        # Create the cuisine_id if it did not exist
        if len(query) == 0:
            self.run_action(sql_insert, params, keep_open=True)
        else:
            pass
        
        #print('insert maybe complete')
        
        return
        
    def load_insp(self,
                 camis,
                 insp_date,
                 insp_time,
                 viol_id,
                 action_id):
        
        #print('running insp')
        #print(camis)
        #print(insp_date)
        #print(insp_time)
        #print(viol_id)
        #print(action_id)
        
        sql_select = """
            SELECT camis, insp_date, insp_time, viol_id, action_id
            FROM tInsp
            WHERE camis = :camis 
                AND insp_date = :insp_date 
                AND insp_time = :insp_time 
                AND viol_id = :viol_id 
        ;"""
        
        sql_insert = """
            INSERT INTO tInsp (camis, insp_date, insp_time, viol_id, action_id)
            VALUES (:camis, :insp_date, :insp_time, :viol_id, :action_id)
        ;"""
        
        params = {'camis': camis, 
                  'insp_date': insp_date, 
                  'insp_time': insp_time, 
                  'viol_id': viol_id, 
                  'action_id': action_id
                 }
        # Will return an action_id if it exists,
        # otherwise the dataframe will be empty
        #print('running query')
        query = self.run_query(sql_select, params, keep_open=True)
        
        # Create the cuisine_id if it did not exist
        #print('creating...')
        if len(query) == 0:
            self.run_action(sql_insert, params, keep_open=True)
        else:
            pass


        return
        
    def load_viol(self,
                  viol_id,
                  viol_desc):
        
        sql_select = """
            SELECT viol_id, viol_desc
            FROM tViol
            WHERE viol_id = :viol_id
        ;"""
        
        sql_insert = """
            INSERT INTO tViol (viol_id, viol_desc)
            VALUES (:viol_id, :viol_desc)
        ;"""
        
        params = {'viol_id': viol_id, 
                  'viol_desc': viol_desc
                 }
        
        query = self.run_query(sql_select, params, keep_open=True)

        if len(query) == 0:
            self.run_action(sql_insert, params, keep_open=True)
        else:
            pass
        
        return
    