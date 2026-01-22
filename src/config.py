import logging
import os
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class Config:
    _REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    _REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    _REDIS_DB = int(os.getenv('REDIS_DB', 0))
    _REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    _connection = redis.Redis(
        host = _REDIS_HOST,
        port = _REDIS_PORT,
        db = _REDIS_DB,
        password = _REDIS_PASSWORD,
        decode_responses = True 
    )
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../"
    players_path = os.path.join(BASE_DIR, "data", "players.csv")
    schedule_path = os.path.join(BASE_DIR, "data", "schedule.csv")
    game_records_path = os.path.join(BASE_DIR, "data", "game_records.csv")

    @classmethod
    def get_redis_connection(cls):
        """
        Returns a connection to the Redis database
        """
        try:
            if cls._connection.ping():
                logger.info(f"Successfully connected to redis:\n{cls._connection}")
                return cls._connection
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to redis:\n{e}")
        return None

if __name__ == '__main__':
    logger.info("Testing connection to redis...")
    connection = Config.get_redis_connection()
