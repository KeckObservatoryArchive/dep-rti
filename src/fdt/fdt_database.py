"""
FDT Database Connection Class
"""

import pymysql
from pymysql.constants import CLIENT

class DatabaseConnect:
    def __init__(self, db_cfg):
        self.db_cfg = db_cfg
        self.db = None

    def connect(self):
        """
        Connect to the database.
        """
        self.db = pymysql.connect(
            # host=self.db_cfg['host'],
            user=self.db_cfg['user'],
            password=self.db_cfg['pwd'],
            database=self.db_cfg['db'],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            client_flag=CLIENT.FOUND_ROWS
        )

    def ensure_connected(self):
        """
        Check the connection,  reconnect if necessary.
        """
        if self.db is None:
            self.connect()
        else:
            self.db.ping(reconnect=True)

    def close(self):
        """
        Close the database connection.
        """
        if self.db is not None:
            try:
                self.db.close()
            finally:
                self.db = None


