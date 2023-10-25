import os
import yaml
import pymysql.cursors
import logging


class db_conn(object):
    '''
    Simple database connection and query layer.  
    Opens and closes and new connection each query.
    Define db connection params in config file.

    Config file follows yaml format and should contain one dict entry per database:
    {
        "(database name)":
        {
            "server" : (server name/ip),
            "user"   : (db username),
            "pwd"    : (db password,
            "port"   : (db server port),
            "type"   : (db type: mysql, postgresql),
        },
    }
    Inputs:
    - configFile: Filepath to yaml config file
    - configKey: Optionally define a config dict key if config is within a larger yaml file.
    '''

    def __init__(self, configFile, configKey=None, persist=False, logger_name='koa_dep'):

        self.persist = persist
        self.readOnly = 0
        self.logger = logging.getLogger(logger_name)

        #parse config file
        assert os.path.isfile(configFile), f"ERROR: config file '{configFile}' does not exist.  Exiting."
        with open(configFile) as f: self.config = yaml.safe_load(f)
        if configKey:
            assert configKey in self.config, f"ERROR: config key '{configKey}' does not exist in config file. Exiting." 
            self.config = self.config[configKey]

        #keep dict of database connections
        self.conns = {}

    def connect(self, database):
        '''
        Connect to the specified database.  
        '''

        #see if we already have a connection and if so ping it to keep alive and return it.
        #todo: read about conn.open == False?
        if self.persist:                
            if database in self.conns and self.conns[database]:
                conn = self.conns[database]
                conn.ping(reconnect=True)
                return conn

        #get db connect data
        assert database in self.config, f"ERROR: database '{database}' not defined in config file.  Exiting."
        config = self.config[database]
        server       = config['server']
        db           = config.get('db', database)
        user         = config['user']
        pwd          = config['pwd']
        port         = int(config['port']) if 'port' in config else 0

        #connect
        try:
            conv = pymysql.converters.conversions.copy()
            conv[10] = str       # convert dates to strings
            conn = pymysql.connect(
                user=user, password=pwd, host=server, database=db,
                connect_timeout=10, read_timeout=10, write_timeout=10,
                autocommit=True, conv=conv
            )
        except Exception as err:
            conn = None
            self.logger.error(f"Could not connect to database. {err}")

        #save connection
        if self.persist:
            self.conns[database] = conn

        #return
        return conn

    def close(self, database=None):
        #close all connections unless they specify one
        for key, conn in self.conns.items():
            if database and key != database: 
                continue
            if conn:
                conn.close()

    def query(self, database, query, values=False, getOne=False,
              getColumn=False, getInsertId=False):
        '''
        Executes basic query.  Determines query type and returns fetchall on
        select, otherwise rowcount on other query types.
        Returns false on any exception error.  Opens and closes a new
        connection each time.
        '''

        result = False
        cursor = None
        conn   = None

        self.logger.debug('starting query')
        try:
            # determine query type and check for read only restriction
            qtype = query.strip().split()[0]
            if self.readOnly and qtype in ('insert', 'update'):
                self.logger.error('Attempting to write to DB in read-only mode.')
                return False

            # get cursor
            conn = self.connect(database)
            self.logger.debug('connected')

            cursor = conn.cursor(pymysql.cursors.DictCursor)
            self.logger.debug('got cursor', level=2)

        except Exception as err:
            self.clean_up(conn, cursor)
            self.logger.error(f'ERROR getting cursor: {err}')
            return False

        self.logger.debug('connection completed.')

        try:
            # execute query and determine return value by qtype
            if cursor:
                if not values:
                    cursor.execute(query)
                else:
                    cursor.execute(query, values)
            else:
                self.logger.error(f'cursor is not defined.')
        except Exception as err:
            self.clean_up(conn, cursor)
            self.logger.error(f'ERROR executing query: {query} {values}, {err}')
            return False

        self.logger.debug('query executed.')

        try:
            if cursor:
                if qtype == 'select':
                    result = cursor.fetchall()
                elif getInsertId:
                    result = cursor.lastrowid
                else:
                    result = cursor.rowcount
                cursor.close()
        except Exception as e:
            self.clean_up(conn, cursor)
            self.error(f'ERROR getting result: {e}')
            return False

        self.logger.debug('got results.')

        try:
            # requesting one result
            if getOne and isinstance(result, list):
                if len(result) == 0:
                    result = False
                else:
                    result = result[0]

            # requesting single column (to remove associative / dictionary
            # key for easy query)
            if getColumn and result:
                if isinstance(result, list):
                    result = [row[getColumn] for row in result]
                else:
                    result = result[getColumn]

        except Exception as e:
            self.logger.error(f'ERROR parsing result: {e}')
            return False


        finally:
            self.clean_up(conn, cursor)

        self.logger.debug('query and cleanup complete.')

        return result

    def clean_up(self, conn, cursor):
        if not self.persist:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


