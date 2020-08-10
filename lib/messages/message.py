import json
from abc import abstractmethod
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pprint import pprint
import logging
import hashlib
from typing import Union

from packaging.specifiers import SpecifierSet

logger = logging.getLogger(__name__)


class Persistent:
    """Protocol that provides necessary information for persisting a message."""
    @staticmethod
    @abstractmethod
    def table_name() -> str:
        """
        The name of the table where the data should be stored.
        :return: str - table name
        """
        pass

    @staticmethod
    @abstractmethod
    def create_table_statement() -> str:
        """
        A valid sql statement for creating the table for storing this message type.
        :return: str - create table sql statement
        """
        pass

    @staticmethod
    @abstractmethod
    def insert_statement() -> str:
        """
        A valid parameterized sql insert statement.  The parameters of the insert should match the
        variable names of the message type.
        :return: str - sql insert statement
        """
        pass


@dataclass
class MessageType:
    """
    Type and version information for a given message
    """
    message_type: str
    message_version: str


@dataclass
class MessageTypeSpecifier:
    """
    Type compatibility information for a given message type.
    """
    message_type: str
    version_specifier: SpecifierSet
    schema_version: str


@dataclass
class Message:
    """
    Data class for used as the base class for all message types.
    """
    message_type: str = 'Message'
    message_version: str = '0.1'
    device_serial_number: str = None
    device_time: str = None
    latitude: float = None
    longitude: float = None
    altitude: float = None
    mission_id: str = None
    record_number: int = None
    device_name: str = None

    @property
    def device_datetime(self):
        if self.device_time:
            return datetime.fromtimestamp(int(self.device_time)/1000)
        return datetime.fromtimestamp(0)

    @property
    def device_timestamp(self):
        return self.device_datetime

    @property
    def unique_id(self):
        d = vars(self)
        hash_string = ''.join([str(d[x]) for x in sorted(d.keys())])
        sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
        return sha_signature


    @staticmethod
    @abstractmethod
    def supports() -> MessageTypeSpecifier:
        pass

    @classmethod
    def is_compatible_with(cls, message_type: MessageType):
        message_type_specifier = cls.supports()
        return message_type.message_type == message_type_specifier.message_type \
               and message_type.message_version in message_type_specifier.version_specifier

    @staticmethod
    def create(data: dict, message_type: MessageType = None):
        if 'message_type' in data and 'message_version' in data and 'data' in data:
            # >= v0.2.0 structure
            message_type = MessageType(message_type=data.get('message_type'), message_version=data.get('message_version'))
            data = data.get('data')

        from .message_repository import MessageTypeRepository
        matching_type = MessageTypeRepository.find_message_type(message_type)
        if matching_type:
            return matching_type.from_dict(data)
        else:
            raise ValueError(f"Unsupported message type [{message_type}].")

    @classmethod
    def fields(cls):
        return fields(cls)
        # field_types = {field.name: field.type for field in fields(MyClass)}

    @classmethod
    def from_dict(cls, data: dict):
        try:
            return cls(**data)
        except Exception as e:
            pprint(data)
            raise e


PersistentMessage = Union[Message, Persistent]


class MessageEncoder(json.JSONEncoder):
    """
    A simple json encoder for correctly encoding timestamps in json.
    """
    def default(self, obj):
        if isinstance(obj, Message):
            d = asdict(obj)
            d['device_timestamp'] = obj.device_datetime.isoformat()
            return d
        # Base class default() raises TypeError:
        return json.JSONEncoder.default(self, obj)


