import psycopg2
from lib.config import DatabaseConfig


class DatabaseAdapter:
    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__()
        self.config = config
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password
        )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

