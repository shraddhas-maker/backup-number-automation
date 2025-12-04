# utils.py
import logging, os
from datetime import datetime, timedelta
from config import LOG_DIR, LOG_ROTATE_MB

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "backup_run.log")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # file rotating basic
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(log_file, maxBytes=LOG_ROTATE_MB*1024*1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def compute_time_window(lookback_minutes):
    now = datetime.utcnow()
    end_time = now - timedelta(minutes=lookback_minutes)
    # StartTime is start of current day (00:00 UTC) — if you need tenant timezone adjust accordingly
    start_of_day = datetime(now.year, now.month, now.day)
    return start_of_day, end_time

def safe_list(val):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [x.strip() for x in str(val).split(',') if x.strip()]
