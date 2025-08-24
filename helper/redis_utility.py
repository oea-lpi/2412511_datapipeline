import logging
import time
import threading

import redis


logger = logging.getLogger(__name__)

def start_heartbeat(redis_client: redis.Redis, key: str, interval: int = 60, ttl: int = 180) -> threading.Thread:
    """
    Starts thread, every 'interval' seconds write key with 'ttl' expiry into redis.
    Writes value "1".

    Args:
        redis_client: Redis client to upload key into.
        key: Name of the heartbeat key in Redis.
        logger: Logger to log status into.
        interval: Heartbeat interval.
        ttl: Time To Live.

    Return:
        thread: Heartbeat thread.
    """
    def loop():
        while True:
            try:
                redis_client.set(key, "1", ex=ttl)
            except Exception:
                logger.exception(f"Failed to write {key}.")
            finally:
                time.sleep(interval)

    t = threading.Thread(target=loop, daemon=True, name=key)
    t.start()