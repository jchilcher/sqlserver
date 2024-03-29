import pyodbc


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

    def __enter__(self):
        return self
    
    def __exit__(self):
        self.cx.close()

    def connect(self):
        """Connects to the database

        Keyword Arguments:
            keepalive {bool} -- keeps the connection open (default: {False})

        Raises:
            Exception: missing parameters

        Returns:
            pyodbc.connection -- connection to the database
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
            keyvalue (string): value of key to compare existing primary key
            kwargs (dict): key value pairs to be updated

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
