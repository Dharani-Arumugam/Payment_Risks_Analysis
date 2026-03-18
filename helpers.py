import math
from datetime import datetime, timedelta, timezone
import random

def ts_after(base: datetime, min_hours: float = 0.5,
             max_hours: float = 72) -> datetime:
    """
    Return a timestamp that is guaranteed to fall AFTER `base`.
    Used for settled_at, refund processed_at, chargeback resolved_at.
    Ensures time-coherent data — no refund can precede its transaction.
    """
    offset = timedelta(hours=random.uniform(min_hours, max_hours))
    return base + offset


def skewed_amount(low=1.0, high=8000.0):
    """
        Power-law-like amount distribution.
        Most transactions are small; a few are very large.
        Achieved by exponentiating a uniform draw — far more realistic
        than uniform(1, 8000) which would give too many $4 000 transactions.
        """
    log_low = math.log(low)
    log_high = math.log(high)
    return round(10** random.uniform(log_low, log_high), 2)


def random_past_ts(max_days_back=90):
    """
    Give me a random time sometime in the last N days.
    :param max_days_back: 90
    :return: datetime
    """
    delta_seconds = random.randint(0, max_days_back * 86_400)
    return datetime.now(timezone.utc)- timedelta(seconds=delta_seconds)


def iso(dt):
    """ISO-8601 string — the format Auto Loader infers as TimestampType."""
    return dt.strftime('%Y-%m-%dT%H:%M:%S')