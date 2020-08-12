import json
from abc import abstractmethod
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pprint import pprint
import logging
import hashlib
from typing import Union, List

from packaging.specifiers import SpecifierSet, InvalidSpecifier

from lib.persistent import Persistent

logger = logging.getLogger(__name__)


@dataclass(order=True)
class MessageType:
    """
    Type and version information for a given message
    """
    message_type: str
    message_version: str
    storage_schema_version: str = None


class MessageTypeSpecifier:
    def __init__(self, message_type: str, version_specifier: str) -> None:
        super().__init__()
        self.message_type = message_type
        try:
            self.version_specifier = SpecifierSet(version_specifier)
        except InvalidSpecifier:
            try:
                self.version_specifier = SpecifierSet(f"=={version_specifier}")
            except InvalidSpecifier:
                self.version_specifier = version_specifier


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
            return datetime.fromtimestamp(int(self.device_time) / 1000)
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
    def supports() -> List[MessageType]:
        pass

    @staticmethod
    @abstractmethod
    def latest_supported() -> MessageType:
        pass

    @staticmethod
    @abstractmethod
    def earliest_supported() -> MessageType:
        pass

    @classmethod
    def is_compatible_with(cls, message_type):
        message_type_specifiers = sorted(cls.supports(), reverse=True)
        if isinstance(message_type, MessageType):
            for message_type_specifier in message_type_specifiers:
                if message_type.message_type == message_type_specifier.message_type \
                        and message_type.message_version == message_type_specifier.message_version:
                    return True
        elif isinstance(message_type, MessageTypeSpecifier):
            for message_type_specifier in message_type_specifiers:
                _message_type: MessageTypeSpecifier = message_type
                if _message_type.message_type == message_type_specifier.message_type \
                        and message_type_specifier.message_version in message_type.version_specifier:
                    return True
        return False

    @staticmethod
    def create(data: dict, message_type: MessageType = None):
        if 'message_type' in data and 'message_version' in data and 'data' in data:
            # >= v0.2.0 structure
            message_type = MessageType(message_type=data.get('message_type'),
                                       message_version=data.get('message_version'))
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


