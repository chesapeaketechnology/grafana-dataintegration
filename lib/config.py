import logging
from dataclasses import dataclass
import os
from dynaconf import Dynaconf

current_directory = os.path.dirname(os.path.realpath(__file__))
settings = Dynaconf(
    envvar_prefix="GDI",
    settings_files=[f"{current_directory}/../settings.yaml", f"{current_directory}/../.secrets.yaml"],
    merge_enabled=True
)


@dataclass
class ConsumerConfig:
    topic: str
    key: str
    fully_qualified_namespace: str
    shared_access_policy: str
    consumer_group: str = '$default'
    buffer_size: int = 1
    checkpoint_store_conn_str: str = None
    checkpoint_store_container_name: str = None


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str


@dataclass
class Configuration:
    consumer: ConsumerConfig
    database: DatabaseConfig

    @staticmethod
    def get_settings():
        return settings

    @staticmethod
    def log_level():
        return logging.getLevelName(settings.get('LOG_LEVEL', 'ERROR'))

    @staticmethod
    def get_config():
        return Configuration(
            consumer=ConsumerConfig(
                topic=settings.get('TOPIC'),
                key=settings.get('KEY'),
                fully_qualified_namespace=settings.get('NAMESPACE'),
                shared_access_policy=settings.get('SHARED_ACCESS_POLICY'),
                consumer_group=settings.get('CONSUMER_GROUP', '$default'),
                buffer_size=int(settings.get('BUFFER_SIZE', 1)),
                checkpoint_store_conn_str=settings.get('CHECKPOINT_STORE_CONNECTION'),
                checkpoint_store_container_name=settings.get('CHECKPOINT_STORE_CONTAINER')

            ),
            database=DatabaseConfig(
                host=settings.get('DB_HOST'),
                port=int(settings.get('DB_PORT')),
                database=settings.get('DB_DATABASE'),
                user=settings.get('DB_USER'),
                password=settings.get('DB_PASSWORD'),
                schema=settings.get('DB_SCHEMA')
            )
        )

