import logging
from dataclasses import dataclass
import os
from datetime import timedelta

from dynaconf import Dynaconf

current_directory = os.path.dirname(os.path.realpath(__file__))
settings = Dynaconf(
    envvar_prefix="GDI",
    settings_files=[f"{current_directory}/../settings.yaml", f"{current_directory}/../.secrets.yaml"],
    merge_enabled=True
)


@dataclass
class ConsumerConfig:
    """
    Structure for housing configuration parameters related to the EventHub topic consumer
    """
    topic: str
    key: str
    fully_qualified_namespace: str
    shared_access_policy: str
    consumer_group: str = '$default'
    buffer_size: int = 1
    max_buffer_time_in_seconds: int = 20
    max_time_to_keep_data_in_seconds: int = timedelta(days=7).total_seconds()
    data_eviction_interval_in_seconds: int = timedelta(hours=2).total_seconds()
    checkpoint_after_messages: int = 500
    checkpoint_store_conn_str: str = None
    checkpoint_store_container_name: str = None


@dataclass
class DatabaseConfig:
    """
    Structure for housing the configuration related to the grafana database
    """
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str
    max_connection_attempts: int = 120


@dataclass
class Configuration:
    """
    Provides functionality from retrieving configuration parameters from settings files and the environment.

    For more information on the configuration library, please refer to `DynaConf <https://www.dynaconf.com>`_.
    """
    consumer: ConsumerConfig
    database: DatabaseConfig

    @staticmethod
    def get_settings():
        """Access to the underlying settings structure"""
        return settings

    @staticmethod
    def log_level():
        """Get the configured log level"""
        return logging.getLevelName(settings.get('LOG_LEVEL', 'ERROR'))

    @staticmethod
    def get_config():
        """
        Get the configuration information
        :return: a :class:`lib.config.Configuration` object
        """
        return Configuration(
            consumer=ConsumerConfig(
                topic=settings.get('TOPIC'),
                key=settings.get('KEY'),
                fully_qualified_namespace=settings.get('NAMESPACE'),
                shared_access_policy=settings.get('SHARED_ACCESS_POLICY'),
                consumer_group=settings.get('CONSUMER_GROUP', '$default'),
                buffer_size=int(settings.get('BUFFER_SIZE', 1)),
                max_buffer_time_in_seconds=int(settings.get('MAX_BUFFER_TIME_IN_SEC', 20)),
                max_time_to_keep_data_in_seconds=int(settings.get('MAX_TIME_TO_KEEP_DATA_IN_SEC',
                                                                  timedelta(days=7).total_seconds())),
                data_eviction_interval_in_seconds=int(settings.get('DATA_EVICT_INTERVAL_IN_SEC',
                                                                   timedelta(hours=2).total_seconds())),
                checkpoint_store_conn_str=settings.get('CHECKPOINT_STORE_CONNECTION'),
                checkpoint_store_container_name=settings.get('CHECKPOINT_STORE_CONTAINER')

            ),
            database=DatabaseConfig(
                host=settings.get('DB_HOST'),
                port=int(settings.get('DB_PORT')),
                database=settings.get('DB_DATABASE'),
                user=settings.get('DB_USER'),
                password=settings.get('DB_PASSWORD'),
                schema=settings.get('DB_SCHEMA'),
                max_connection_attempts=int(settings.get('DB_MAX_CONN_ATTEMPTS', 120))
            )
        )

