from dataclasses import dataclass
from typing import List

import yaml


@dataclass
class ConsumerConfig:
    topic: str
    messageType: str
    consumer_group: str
    primary_key: str
    secondary_key: str
    fully_qualified_namespace: str
    shared_access_policy: str
    buffer_size: int = 1


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


class Configuration:

    def __init__(self, config_file) -> None:
        super().__init__()
        self.config_file = config_file

    def get_database_config(self) -> DatabaseConfig:
        with open(self.config_file, 'r') as file:
            config_data = yaml.safe_load(file)
            db_config = DatabaseConfig(**config_data['database'])
            return db_config

    def get_consumers(self) -> List[ConsumerConfig]:
        with open(self.config_file, 'r') as file:
            config_data = yaml.safe_load(file)
            consumer_configurations = [ConsumerConfig(**d) for d in config_data['consumers']]
            return consumer_configurations



config = Configuration(config_file='../config.yaml')
consumers = config.get_consumers()
print(consumers)
print(config.get_database_config())
