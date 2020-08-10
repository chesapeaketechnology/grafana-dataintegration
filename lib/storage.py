from abc import abstractmethod
from typing import List, Type

import psycopg2
from psycopg2.extras import execute_batch, DictCursor

from lib.config import DatabaseConfig
from lib.messages.message import PersistentMessage
import logging

from lib.migrations.migration import Migrations

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


class StorageDelegate:
    """
    Provides storage services for incoming data.
    """
    @abstractmethod
    def save(self, messages: List[PersistentMessage]):
        pass


class PostgresStorageDelegate(StorageDelegate):
    """
    Provides storage services using a configured postgres database. Configuration information
    is provided via a DatabaseConfig object.
    """
    def __init__(self, config: DatabaseConfig, message_class: Type[PersistentMessage]) -> None:
        super().__init__()
        self.config = config
        self._connection = None
        self.message_class = message_class
        self.validate_schema(message_class)

    def validate_schema(self, message_class: Type[PersistentMessage]):
        if not self.table_exists(message_class):
            self.create_table(message_class)
        try:
            if self.version_supported(message_class):
                return True
        except StorageVersionError as sve:
            # See if we can find any migrations
            message_specifier = message_class.latest_supported()
            migrations = Migrations.find_migration_for(from_version=sve.current_schema_version,
                                                       to_version=message_specifier.storage_schema_version)
            if migrations:
                migrations.execute(self.connection)
                return True
            raise StorageError(f"Message type with specifier {message_specifier} is not supported for this "
                               f"database version and migrations were not found.")

    @property
    def connection(self):
        """
        Return the database connection. Will create the connection if it doesn't exist.
        :return: db_api connection object
        """
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
        """Create the schema version table."""
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
        """
        Determine is the version of the specified message type is currently supported by the configured database.
        :param message_class: The class "type" of the message being handled by this instance.
        :return: True if supported, else StorageError or StorageVersionError
        """
        message_specifier = message_class.latest_supported()

        def __version_supported():
            with self.connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM public.message_version "
                    "WHERE message_type = %(message_type)s "
                    "ORDER BY version desc",
                    {'message_type': message_class.table_name()}
                )
                if cursor.rowcount < 1:
                    raise StorageError(f"Database schema does not support message "
                                       f"type {message_specifier.message_type} with schema "
                                       f"version of {message_specifier.storage_schema_version}")

                record = cursor.fetchone()
                current_schema_version = record['version']

                # Assumes backward compatibility, this may need to be modified in the future.
                if current_schema_version >= message_specifier.storage_schema_version:
                    return True
                else:
                    raise StorageVersionError(current_schema_version=current_schema_version)
        try:
            return __version_supported()
        except Exception as e:
            logger.warning(e)
            if not self.version_table_exists():
                self.create_version_table()
                return __version_supported()

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
            cursor.execute(version_stmt, (message_class.table_name(),
                                          message_class.latest_supported().storage_schema_version))

    def save(self, messages: List[PersistentMessage]):
        """
        Save the messages to the data store. The messages must all be of the same message type.
        :param messages: a list of message objects that implement the Message and Persistent classes
        :return: None
        """
        if messages:
            statement = self.message_class.insert_statement()
            try:
                self.connection.autocommit = True
                with self.connection.cursor() as cursor:

                    all_messages = [{
                        **vars(message),
                        'device_datetime': message.device_datetime,
                        'device_timestamp': message.device_timestamp,
                        'unique_id': message.unique_id
                    } for message in messages]

                    try:
                        execute_batch(cursor, statement, all_messages)
                    except Exception as e:
                        logger.warning("Unable to insert row, checking to ensure table exists.")
                        if not self.table_exists(self.message_class):
                            self.create_table(self.message_class)
                            execute_batch(cursor, statement, all_messages)
            except Exception as e:
                raise StorageError(f"Unable to store messages [{messages}] of "
                                   f"type {self.message_class.message_type}") from e
