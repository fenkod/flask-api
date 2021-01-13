import os

class base_config():
    PL_DB_HOST = os.environ.get('PL_DB_HOST')
    PL_DB_DATABASE = os.environ.get('PL_DB_DATABASE', 'pitcher-list')
    PL_DB_USER = os.environ.get('PL_DB_USER')
    PL_DB_PW = os.environ.get('PL_DB_PW')
    BYPASS_CACHE = os.environ.get('BYPASS_CACHE', False)
    CACHE_INVALIDATE_HOUR = os.environ.get('CACHE_INVALIDATE_HOUR', 10)
    REDIS_URL = os.environ.get('REDIS_URL', '')