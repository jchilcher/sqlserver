import pyodbc
from import_params import (dec2str_fields)

class SqlServer:
    """SQL Server client for running simple queries and commands"""

    def __init__(self, server=None, database=None, username=None, passwd=None,
                 driver=None, keepalive=False):
        self.server = server
        self.username = username
        self.passwd = passwd
        self.driver = driver
        self.database = database
        self.cx = None
        if keepalive:
            self.cx = self.connect()

    def connect(self):
        """Connects to the database

        Keyword Arguments:
            keepalive {bool} -- keeps the connection open (default: {False})
        Raises:
            Exception: missing parameters
        Returns:
            pyodbc.connection -- connection to the database
            https://github.com/mkleehammer/pyodbc/wiki/Connection
        """
        try:
            if None in (self.server, self.database, self.username, self.passwd,
                        self.driver):
                raise Exception('A server, username, password, and driver must'
                                ' be specified')
            cx = pyodbc.connect(
                'DRIVER={{{0}}};SERVER={1};'
                'DATABASE={2};UID={3};PWD={4}'.format(
                    self.driver, self.server, self.database,
                    self.username, self.passwd)
                )
            return cx
        except Exception as e:
            print(str(e))
            return False
        
    def disconnect(self):
        """Disconnects from the database
           It is very good practice to close db connections when done using them"""
        if self.cx is not None:
            self.cx.close()
            self.cx = None

    def dictList_ExecuteMany(self, table_name, dictionaryList):
        # build field names and value list from first image
        # returns the count {int} of records that were inserted
        # Written by Jeanette Durham 2/5/2021
        sql_string = 'INSERT INTO [{0}]'.format(table_name)
        k = v = ''
        for key, value in dictionaryList[0].items():
            k += (',' if k != '' else '') + '[{}]'.format(key)  # [col1],[col2],..
            v += (',' if v != '' else '') + '?'
        sql_string += ' ({0}) VALUES ({1});'.format(k, v)
        
        # build the data values we'll pass to the executemany func
        data_rows, rec_knt = [], 0
        for dictItem in dictionaryList:
            v_list = []
            for key, value in dictItem.items():
                # handle any specific fields we are explicitly setting
                if key in dec2str_fields:
                    # floats tend to go into the db as nums like: 0.17999999999999999
                    # so we either insert them as strings or you can set the field in t-sql
                    #   such as: Latitude decimal(11,6)
                    # integers that are floats such as 150.0 insert fine <shrugs> ~Jeanette 2/8/2021
                    if value is not None: value = str(value)
                else:
                    # modify any string types as needed (will work with a sql datetime field)
                    if value is not None and isinstance(value, str):
                        # Convert '2021-01-02T12:37:08.873-00:00' -> '2021-01-02 12:37:08.873'
                        if len(value) == 29 and value[10] == 'T' and value[-6:] == '-00:00':
                            value = value[0:10] + ' ' + value[11:23]
                v_list.append(value)
            data_rows.append(tuple(v_list))
            rec_knt += 1
            
        # insert all rows and commit
        crsr = self.cx.cursor()
        crsr.fast_executemany = True  # new in pyodbc 4.0.19
        crsr.executemany(sql_string, data_rows)
        self.cx.commit()
        crsr.close()
        return rec_knt  # track how many we inserted & parsed

    def insert(self, table, **kwargs):
        """performs an insert using kwargs as the column/data

        Arguments:
            table {string} -- Name of the table
        Returns:
            int -- rows affected
        """
        sql_string = 'INSERT INTO [{0}]'.format(table)
        k = ''
        v = ''
        v_list = []
        for key, value in kwargs.items():
            v_list.append(value)
            if key == '' or v == '':
                k = '[{}]'.format(key)
                v = "?"
            else:
                k += ', [{}]'.format(key)
                v += ", ?"

        sql_string += ' ({0}) VALUES ({1});'.format(k, v)
        return self.update(sql_string, tuple(v_list))

    def select(self, query):
        """run query that returns rows

        Arguments:
            query {str} -- Query
        Returns:
            list -- All rows (be careful with large returns as all rows will
                    be stored into memory)
        """
        if self.cx is None:
            with self.connect() as cx:
                res = cx.execute(query).fetchall()
        else:
            res = self.cx.execute(query).fetchall()

        return res
        
    def update(self, query, *args):
        """run query that writes to the database

        Arguments:
            query {str} -- Query
        """
        if self.cx is None:
            with self.connect() as cx:
                res = cx.execute(query, *args)
        else:
            res = self.cx.execute(query, *args)

        res.commit()
        return res.rowcount

    def upsert(self, table, keyname, keyvalue, **kwargs):
        """performs an upsert
           checks if a key exists and either updates or inserts data

        Args:
            table (string): table name
            key (string): name of primary key
            keyvalue: value of key to compare existing primary key

        Returns:
            int -- rows affected
        """
        # (table, key, keyvalue, update_string, insert_keys, insert_values)
        sql_string = (
            "IF EXISTS (SELECT * FROM dbo.[{0}] WHERE [{1}] = {2}) "
            "UPDATE dbo.[{0}] "
            "SET {3} "
            "WHERE [{1}] = {2}; "
            "ELSE "
            "INSERT dbo.[{0}] ( {4} ) "
            "VALUES ( {5} );"
        )

        k = ''
        v = ''
        update_string = ''
        v_list = []
        for key, value in kwargs.items():
            v_list.append(value)
            if key == '' or v == '':
                k = '[{}]'.format(key)
                v = "?"
                update_string += '[{}] = ?'.format(key)
            else:
                k += ', [{}]'.format(key)
                v += ", ?"
                update_string += ', [{}] = ?'.format(key)
        #print(sql_string.format(table, keyname, keyvalue, update_string, k, v))
        return self.update(
            sql_string.format(table, keyname, keyvalue, update_string, k, v),
            tuple(v_list + v_list)
        )
