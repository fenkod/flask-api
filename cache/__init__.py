from datetime import datetime, timedelta
from .cache import *

cache_invalidate_hour = 10

def cache_timeout(hour):
    current_time = datetime.now()
    fiveam = current_time.replace(hour=hour, minute=0, second=0)
    if fiveam > current_time:
        delta = fiveam - current_time
    else:
        tomorrow = current_time + timedelta(days=1)
        tomorrow_fiveam = tomorrow.replace(hour=hour, minute=0, second=0)
        delta = tomorrow_fiveam - current_time

    return delta.seconds