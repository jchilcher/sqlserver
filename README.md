# SqLServer
 Installable Python Class utilizing pyodbc to run common queries as functions.

## Getting Started

start by importing the class and instatiating with database credentials
```bash
python
>>> from sqlserver import SqlServer
>>> client = SqlServer(db_server, db_name, db_user, db_passwd, db_driver, keepalive)
```

### Prerequisites

You will need Python 3 or higher installed and pip, see [Built With]().

use pip to install the required python packages:
```bash
pip install setup.py
```

## Built With

* [Python 3](https://www.python.org/) - The programming language used
* [Pyodbc](https://github.com/mkleehammer/pyodbc) - SQL Server client

## Authors

* **Johnathen Chilcher** - *Initial work* - [jchilcher](https://github.com/jchilcher)
