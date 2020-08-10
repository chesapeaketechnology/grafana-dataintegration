from dataclasses import dataclass
from os import environ


@dataclass
class ConsumerConfig:
    topic: str
    message_type: str
    message_version: str
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
    def get_config():
        return Configuration(
            consumer=ConsumerConfig(
                topic=environ.get('GDI_TOPIC'),
                message_type=environ.get('GDI_MESSAGE_TYPE'),
                message_version=environ.get('GDI_MESSAGE_VERSION'),
                key=environ.get('GDI_KEY'),
                fully_qualified_namespace=environ.get('GDI_NAMESPACE'),
                shared_access_policy=environ.get('GDI_SHARED_ACCESS_POLICY'),
                consumer_group=environ.get('GDI_CONSUMER_GROUP', '$default'),
                buffer_size=int(environ.get('GDI_BUFFER_SIZE', 1)),
                checkpoint_store_conn_str=environ.get('GDI_CHECKPOINT_STORE_CONNECTION'),
                checkpoint_store_container_name=environ.get('GDI_CHECKPOINT_STORE_CONTAINER')

            ),
            database=DatabaseConfig(
                host=environ.get('GDI_DB_HOST'),
                port=int(environ.get('GDI_DB_PORT', '5432')),
                database=environ.get('GDI_DB_DATABASE'),
                user=environ.get('GDI_DB_USER'),
                password=environ.get('GDI_DB_PASSWORD'),
                schema=environ.get('GDI_DB_SCHEMA')
            )
        )

    # def __init__(self, config_file) -> None:
    #     super().__init__()
    #     self.config_file = config_file
    #
    # def get_database_config(self) -> DatabaseConfig:
    #     with open(self.config_file, 'r') as file:
    #         config_data = yaml.safe_load(file)
    #         db_config = DatabaseConfig(**config_data['database'])
    #         return db_config
    #
    # def get_consumers(self) -> List[ConsumerConfig]:
    #     with open(self.config_file, 'r') as file:
    #         config_data = yaml.safe_load(file)
    #         consumer_configurations = [ConsumerConfig(**d) for d in config_data['consumers']]
    #         return consumer_configurations
