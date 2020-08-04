import psycopg2
import os
_connection = None

def get_connection():
    global _connection
    if not _connection:
        pl_host = os.getenv('PL_DB_HOST')
        pl_db = 'pitcher-list'
        pl_user = os.getenv('PL_DB_USER')
        pl_password = os.getenv('PL_DB_PW')
        _connection = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)
    return _connection