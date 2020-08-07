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
    @staticmethod
    @abstractmethod
    def table_name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def create_table_statement() -> str:
        pass

    @staticmethod
    @abstractmethod
    def insert_statement() -> tuple:
        pass


@dataclass
class MessageType:
    message_type: str
    message_version: str


@dataclass
class MessageTypeSpecifier:
    message_type: str
    version_specifier: SpecifierSet
    schema_version: str


@dataclass
class Message:
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

    @staticmethod
    def registered_message_types():
        return [LTEMessage]

    @classmethod
    def is_compatible_with(cls, message_type: MessageType):
        message_type_specifier = cls.supports()
        specifier_set = SpecifierSet(message_type_specifier.version_specifier)
        return message_type.message_type == message_type_specifier.message_type \
               and message_type.message_version in specifier_set

    @staticmethod
    def create(data: dict):
        if 'message_type' not in data or 'message_version' not in data:
            raise ValueError(f"Unsupported message [{data}].")
        message_type = MessageType(message_type=data.get('message_type'), message_version=data.get('message_version'))
        matching_type = next(filter(lambda t: t.is_compatible_with(message_type),
                                    Message.registered_message_types()), None)
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


@dataclass
class LTEMessage(Message, Persistent):
    message_type: str = 'LTERecord'
    message_version: str = '0.1'
    ci: int = None    # cell identification
    earfcn: int = None     # E-UTRA Absolute Radio Frequency
    group_number: int = None   # Used to group records at a single point, (1 active record with multiple neighbor records)
    mcc: int = None     # mobile country code
    mnc: int = None    # mobile network code
    pci: int = None    # physical cell identifier
    rsrp: float = None   # reference signal received power
    rsrq: float = None   # reference signal received quality
    servingCell: bool = None  # bool representing whether this record is a serving cell or a neighbor cell record
    tac: int = None    # tracking area code
    lteBandwidth: str = None
    provider: str = None

    @classmethod
    def from_dict(cls, data: dict):
        if 'ta' in data:
            data['tac'] = data['ta']
            del data['ta']
        return super().from_dict(data)

    @staticmethod
    def supports() -> MessageTypeSpecifier:
        return MessageTypeSpecifier(message_type='LTERecord',
                                    version_specifier=SpecifierSet('~=0.1.0'),
                                    schema_version='0.1.0')

    @staticmethod
    def table_name():
        return "lte_message"

    @staticmethod
    def create_table_statement() -> str:
        return """
            create table public.lte_message
            (
                id varchar(64) not null 
                    constraint lte_message_pk primary key,
                device_serial_number text,
                device_timestamp timestamp with time zone,
                device_time numeric,
                device_name text,
                latitude numeric,
                longitude numeric,
                altitude numeric,
                mission_id text,
                record_number integer,
                ci integer,
                earfcn integer,
                group_number integer,
                mcc integer,
                mnc integer,
                pci integer,
                rsrp numeric,
                rsrq numeric,
                serving_cell boolean,
                tac integer,
                lte_bandwidth text,
                provider text,
                message_type text,
                message_version text
            );
            
            alter table public.lte_message owner to postgres;
        """

    @staticmethod
    def insert_statement() -> str:
        return """INSERT INTO public.lte_message  
                  VALUES (
                    %(unique_id)s, 
                    %(device_serial_number)s, 
                    %(device_timestamp)s, 
                    %(device_time)s, 
                    %(device_name)s, 
                    %(latitude)s, 
                    %(longitude)s, 
                    %(altitude)s, 
                    %(mission_id)s, 
                    %(record_number)s, 
                    %(ci)s, 
                    %(earfcn)s, 
                    %(group_number)s, 
                    %(mcc)s, 
                    %(mnc)s, 
                    %(pci)s, 
                    %(rsrp)s, 
                    %(rsrq)s, 
                    %(servingCell)s, 
                    %(tac)s, 
                    %(lteBandwidth)s, 
                    %(provider)s, 
                    %(message_type)s, 
                    %(message_version)s  
                   )
                   ON CONFLICT DO NOTHING;
                """

    # @property
    # def insert_values(self):
    #     return (
    #         self.unique_id,
    #         self.device_serial_number,
    #         self.device_timestamp,
    #         self.device_time,
    #         self.device_name,
    #         self.latitude,
    #         self.longitude,
    #         self.altitude,
    #         self.mission_id,
    #         self.record_number,
    #         self.ci,
    #         self.earfcn,
    #         self.group_number,
    #         self.mcc,
    #         self.mnc,
    #         self.pci,
    #         self.rsrp,
    #         self.rsrq,
    #         self.servingCell,
    #         self.tac,
    #         self.lteBandwidth,
    #         self.provider,
    #         self.message_type,
    #         self.message_version
    #     )

@dataclass
class WifiMessage(Message, Persistent):
    message_type: str = 'WifiBeaconRecord'
    message_version: str = '0.1'
    bssid: str = None
    ssid: str = None
    encryptionType: str = None
    wps: bool = None
    channel: int = None
    frequency: int = None
    signalStrength: float = None

    def table_name(self):
        return "wifi_message"

    def schema_version(self) -> str:
        return "0.1"

    def create_table_statement(self) -> str:
        return """
                create table public.wifi_message
                (
                    device_serial_number text
                    device_timestamp timestamp with time zone,
                    device_time numeric,
                    device_name text,
                    latitude numeric,
                    longitude numeric,
                    altitude numeric,
                    mission_id text,
                    record_number integer,
                    device_name text,
                    bssid text,
                    ssid text,
                    encryption_type text,
                    wps boolean,
                    channel integer,
                    frequency integer,
                    signal_strength numeric,
                    message_type text,
                    message_version text
                );

                alter table public.wifi_message owner to postgres;
            """

    def insert_statement(self) -> tuple:
        stmt = """INSERT INTO public.wifi_message ( 
                    device_serial_number, device_timestamp, device_time, device_name, 
                    latitude, longitude, altitude, mission_id, record_number,  
                    bssid, ssid, encryption_type, wps, channel, frequency, 
                    signal_strength
            ) 
           VALUES (
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
           );
        """
        values = (
            self.device_serial_number, self.device_timestamp, self.device_time, self.device_name,
            self.latitude, self.longitude, self.altitude, self.mission_id, self.record_number,
            self.bssid, self.encryptionType, self.wps, self.channel, self.frequency,
            self.signalStrength, self.message_type, self.message_version
        )
        return stmt, values


@dataclass
class CDMAMessage(Message):
    group_number: int = None
    sid: int = None
    nid: int = None
    bsid: int = None
    signalStrength: float = None
    ecio: float = None
    servingCell: bool = None


@dataclass
class UMTSMessage(Message):
    group_number: int = None
    mcc: int = None
    mnc: int = None
    lac: int = None
    ci: int = None
    uarfcn: int = None
    psc: int = None
    signalStrength: float = None
    servingCell: bool = None
    provider: str = None


class MessageEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Message):
            d = asdict(obj)
            d['device_timestamp'] = obj.device_datetime.isoformat()
            return d
        # Base class default() raises TypeError:
        return json.JSONEncoder.default(self, obj)



# print(LTEMessage.fields())

