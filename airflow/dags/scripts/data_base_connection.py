from sqlalchemy import create_engine, pool
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DbConnection:
    def __init__(self):
        self.db_url = "postgresql://airflow:airflow@postgres/airflow"
        self.engine = create_engine(self.db_url, echo=True, pool_size=10, max_overflow=20)

    def connect(self):
        try:
            conn = self.engine.connect()
            logger.info('Database connected successfully')
            return conn

        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise
