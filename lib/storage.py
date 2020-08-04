from abc import abstractmethod

import psycopg2

from lib.config import DatabaseConfig
from lib.message import Message, Persistent
import logging

logger = logging.getLogger(__name__)


class StorageException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class StorageDelegate:
    @abstractmethod
    def save(self, message: Message):
        pass


class PostgresStorageDelegate(StorageDelegate):

    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__()
        self.config = config
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
        return self._connection

    def create_version_table(self):
        """Create the version table."""
        cursor = self.connection.cursor()
        self.connection.autocommit = True
        stmt = """create table public.message_version
                  (
                      message_type varchar(128) not null 
                      constraint message_version_pk primary key,
                      version varchar(10)
                  );
                  alter table public.message_version owner to postgres
                  """
        cursor.execute(stmt)

    def version_table_exists(self):
        """Determine if the schema version table exists in the configured database."""
        cursor = self.connection.cursor()
        self.connection.autocommit = True

        cursor.execute("SELECT * FROM information_schema.tables "
                       "WHERE table_schema = 'public' "
                       "AND table_name = 'message_version'")
        return cursor.rowcount > 0

    def table_exists(self, table_name: str):
        """Determine if the table exists in the configured database."""
        cursor = self.connection.cursor()
        self.connection.autocommit = True

        cursor.execute("SELECT * FROM information_schema.tables "
                       "WHERE table_schema = 'public' "
                       "AND table_name = %(table_name)s", {'table_name': table_name})
        return cursor.rowcount > 0

    def create_table(self, message: (Message, Persistent)):
        """Create the table in the database."""
        cursor = self.connection.cursor()
        self.connection.autocommit = True
        cursor.execute(message.create_table_statement)
        version_stmt = """
            INSERT INTO public.message_version(message_type, version) VALUES(%s, %s)
            ON CONFLICT (message_type)
            DO UPDATE SET version = EXCLUDED.version 
        """
        cursor.execute(version_stmt, (message.message_type, message.message_version))

# TODO: - Modify to take a list of messages to facilitate batch writes.
    def save(self, message: (Message, Persistent)):
        # do the insert and look for errors to save time
        cursor = self.connection.cursor()
        self.connection.autocommit = True
        try:
            (stmt, values) = message.insert_statement
            cursor.execute(stmt, values)
        except Exception as e:
            logger.warning("Unable to insert row, checking to ensure table exists.")
            if not self.table_exists(message.table_name):
                self.create_table(message)
            try:
                (stmt, values) = message.insert_statement
                cursor.execute(stmt, values)
            except Exception as e:
                logger.exception(f"Unable to store message of type {message.message_type}. {str(e)}")
                raise StorageException(f"Unable to store message of type {message.message_type}. {str(e)}") from e


