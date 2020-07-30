import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pprint import pprint


@dataclass
class Message:
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
    def from_dict(cls, data: dict):
        try:
            return cls(**data)
        except Exception as e:
            pprint(data)
            raise e


@dataclass
class LTEMessage(Message):
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
