# db_connection.py
import pyodbc

CONNECTIONS = {
    "Psystem": {
        "server": "TISS-SQLSRV2",
        "database": "ProdDB",
        "user": "psys_user",
        "password": "psysuser"
    },
    "ASS": {
        "server": "TISS-SQLSRV2",
        "database": "ASS",
        "user": "ass",
        "password": "ass"
    },
    "AssemblySched": {
        "server": "TISS-SQLSRV2",
        "database": "AssemblySched",
        "user": "digiassy",
        "password": "digiassy"
    },
    "db02": {
        "server": "trkbaan",
        "database": "odbc",
        "user": "odbc",
        "password": "odbc"
    },
    "BomMst": {
        "server": "192.168.134.11",
        "database": "002Stock",
        "user": "BomMstUser",
        "password": "BomMstUser"
    },
    "stock_repair": {
        "server": "TISS-SQLSRV1",
        "database": "stock_repair",
        "user": "buhin_user",
        "password": "buhin"
    }
}

def get_connection(env):
    cfg = CONNECTIONS[env]
    return pyodbc.connect(
        fr"DRIVER={{ODBC Driver 17 for SQL Server}};"
        fr"SERVER={cfg['server']};"
        fr"DATABASE={cfg['database']};"
        fr"UID={cfg['user']};"
        fr"PWD={cfg['password']}"
    )
