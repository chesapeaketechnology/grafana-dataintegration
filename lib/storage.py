from abc import abstractmethod
from typing import List, Type

import psycopg2
from psycopg2.extras import execute_batch

from lib.config import DatabaseConfig
from lib.message import PersistentMessage, Message
import logging

logger = logging.getLogger(__name__)


class StorageException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class StorageDelegate:
    @abstractmethod
    def save(self, messages: List[PersistentMessage]):
        pass


class PostgresStorageDelegate(StorageDelegate):

    def __init__(self, config: DatabaseConfig, message_class: Type[PersistentMessage]) -> None:
        super().__init__()
        self.config = config
        self._connection = None

    def validate_schema(self, message_class: Type[PersistentMessage]):
        try:
            if not self.table_exists(message_class):
                self.create_table(message_class)
            self.version_supported(message_class)
        except Exception as e:


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
        self._connection.autocommit = True
        return self._connection

    def create_version_table(self):
        """Create the version table."""
        with self.connection.cursor() as cursor:
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
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM information_schema.tables "
                           "WHERE table_schema = 'public' "
                           "AND table_name = 'message_version'")
            return cursor.rowcount > 0

    def version_supported(self, message_class: Type[PersistentMessage]):
        def __version_supported():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM public.message_version "
                    "WHERE message_type = %(message_type)s",
                    {'message_type': message_class.table_name()}
                )
                cursor.foreach
                if cursor.rowcount < 1:
                    raise StorageException(f"Database schema does not support message type {message_type} "
                                           f"with schema version of {schema_version}")
                return True
        try:
            return __version_supported()
        except Exception as e:
            logger.warning(e)
            try:
                if not self.version_table_exists():
                    self.create_version_table()
                    return __version_supported()
            except Exception as pe:
                raise pe

    def table_exists(self, message_class: Type[PersistentMessage]):
        """Determine if the table exists in the configured database."""
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM information_schema.tables "
                           "WHERE table_schema = 'public' "
                           "AND table_name = %(table_name)s", {'table_name': message_class.table_name()})
            return cursor.rowcount > 0

    def create_table(self, message_class: Type[PersistentMessage]):
        """Create the table in the database."""
        with self.connection.cursor() as cursor:
            cursor.execute(message_class.create_table_statement())
            version_stmt = """
                INSERT INTO public.message_version(message_type, version) VALUES(%s, %s)
                ON CONFLICT (message_type)
                DO UPDATE SET version = EXCLUDED.version 
            """
            cursor.execute(version_stmt, (message_class.table_name(), message_class.supports().schema_version))

    def save(self, messages: List[PersistentMessage]):
        """
        Save the messages to the data store. The messages must all be of the same message type.
        :param messages: a list of message objects that implement the Message and Persistent classes
        :return: None
        """
        if messages:
            message_type = messages[0]
            if self.version_supported(message_type):
                statement = messages[0].insert_statement()
                try:
                    self.connection.autocommit = True
                    with self.connection.cursor() as cursor:

                        all_messages = [{
                            **message,
                            'device_datetime': message.device_datetime,
                            'device_timestamp': message.device_timestamp,
                            'unique_id': message.unique_id
                        } for message in messages]

                        try:
                            execute_batch(cursor, statement, all_messages)
                        except Exception as e:
                            logger.warning("Unable to insert row, checking to ensure table exists.")
                            if not self.table_exists(message_type.table_name()):
                                self.create_table(message_type)
                                execute_batch(cursor, statement, all_messages)
                except Exception as e:
                    raise StorageException(f"Unable to store messages [{messages}] of "
                                           f"type {message_type.message_type}") from e
