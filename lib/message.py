import json
from abc import abstractmethod
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pprint import pprint
import logging

import psycopg2

from lib.config import DatabaseConfig

logger = logging.getLogger(__name__)

class Persistent:
    @property
    @abstractmethod
    def table_name(self) -> str:
        pass

    @property
    @abstractmethod
    def schema_version(self) -> str:
        pass

    @property
    @abstractmethod
    def create_table_statement(self) -> str:
        pass

    @property
    @abstractmethod
    def insert_statement(self) -> tuple:
        pass

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
        return self.device_datetime.timestamp()

    @staticmethod
    def create(data_type: str, data: dict):
        return Message.type_for(data_type).from_dict(data)

    @staticmethod
    def type_for(data_type: str):
        if data_type == "80211_beacon_message":
            return WifiMessage
        if data_type == "cdma_message":
            return CDMAMessage
        if data_type == "lte_message":
            return LTEMessage
        if data_type == "umts_message":
            return UMTSMessage
        return None

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

    @property
    def table_name(self):
        return "lte_message"

    @property
    def create_table_statement(self) -> str:
        return """
            create table public.lte_message
            (
                device_serial_number text,
                device_timestamp timestamp with time zone,
                device_time numeric,
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
                provider text
            );
            
            alter table public.lte_message owner to postgres;
        """

    @property
    def insert_statement(self) -> tuple:
        stmt = """INSERT INTO public.messages (
                    device_serial_number, 
                    device_timestamp,
                    device_time, 
                    latitude,
                    longitude,
                    altitude,
                    mission_id,
                    record_number,
                    ci,
                    earfcn,
                    group_number,
                    mcc,
                    mnc,
                    pci,
                    rsrp,
                    rsrq,
                    serving_cell,
                    tac,
                    lte_bandwidth,
                    provider) 
                   VALUES (
                     %%s, %s, %s, %s, %s, %s, %s, s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
                   );
                """
        values = (
            self.device_serial_number,
            self.device_timestamp,
            self.device_time,
            self.latitude,
            self.longitude,
            self.altitude,
            self.mission_id,
            self.record_number,
            self.ci,
            self.earfcn,
            self.group_number,
            self.mcc,
            self.mnc,
            self.pci,
            self.rsrp,
            self.rsrq,
            self.servingCell,
            self.tac,
            self.lteBandwidth,
            self.provider
        )
        return stmt, values


@dataclass
class WifiMessage(Message):
    bssid: str = None
    ssid: str = None
    encryptionType: str = None
    wps: bool = None
    channel: int = None
    frequency: int = None
    signalStrength: float = None


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

