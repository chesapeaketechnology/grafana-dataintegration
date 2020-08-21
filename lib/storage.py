import logging
from abc import abstractmethod
from datetime import datetime
from pprint import pformat
from typing import List

import psycopg2
from psycopg2.extras import execute_batch

from lib.config import DatabaseConfig
from lib.message import Message
from lib.persistent import Persistent

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """
    Indicates that there was an error storing data to the configured database.
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class StorageVersionError(StorageError):
    """
    Indicates an incompatibility between the message type and the existing database schema.
    """
    def __init__(self, current_schema_version: str, *args: object) -> None:
        super().__init__(*args)
        self.current_schema_version = current_schema_version


class MessageStorageDelegate:
    """
    Provides storage services for incoming data.
    """
    @abstractmethod
    def save(self, messages: List[Persistent]):
        """
        Save a :class:List of :class:Persistent objects to the database.

        :param List[Persistent] messages: The messages to save.
        :return: None
        """
        pass

    @abstractmethod
    def evict(self, older_than: datetime):
        """
        Evict stored messages older than the provides timestamp.
        :param datetime older_than: cutoff for data to keep
        :return: None
        """
        pass


class PostgresMessageStorageDelegate(MessageStorageDelegate):
    """
    Provides storage services using a configured postgres database. Configuration information
    is provided via a DatabaseConfig object.
    """
    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__()
        self.config = config
        self._connection = None

    def connected(self):
        return self._connection and self._connection.closed == 0

    @property
    def connection(self):
        """
        Return the database connection. Will create the connection if it doesn't exist.
        :return: db_api connection object
        """
        if not self.connected():
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
        return self._connection

    def table_exists(self):
        """Determine if the table exists in the configured database."""
        with self.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM information_schema.tables "
                               "WHERE table_schema = 'public' "
                               "AND table_name = %(table_name)s", {'table_name': Message.table_name()})
                return cursor.rowcount > 0

    def create_table(self):
        """Create the table in the database."""
        with self.connection as conn:
            with conn.cursor() as cursor:
                for statement in Message.create_table_statements():
                    cursor.execute(statement)

    def evict(self, older_than: datetime):
        statement = Message.delete_statement()
        try:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    cursor.execute(statement, {'device_timestamp': older_than})
        except Exception as e:
            raise StorageError(f"Unable to delete messages prior to [{older_than}]") from e

    def save(self, messages: List[Message]):
        """
        Save the messages to the data store. The messages must all be of the same message type.

        :param List[Message] messages: a list of Message objects
        :return: None
        """
        if messages:
            statement = Message.insert_statement()
            try:
                with self.connection as conn:
                    with conn.cursor() as cursor:

                        all_messages = [{
                            **vars(message)
                        } for message in messages]

                        try:
                            logger.info(f"Inserting {len(all_messages)} messages. "
                                        f"(from: {all_messages[0]['device_timestamp'].isoformat()} "
                                        f"to: {all_messages[-1]['device_timestamp'].isoformat()})")
                            logger.debug(pformat(all_messages))
                            execute_batch(cursor, statement, all_messages)
                        except Exception as e:
                            logger.warning("Unable to insert row, checking to ensure table exists.")
                            if not self.table_exists():
                                self.create_table()
                                execute_batch(cursor, statement, all_messages)
            except Exception as e:
                raise StorageError(f"Unable to store messages [{messages}]") from e
