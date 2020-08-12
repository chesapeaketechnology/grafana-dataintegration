from dataclasses import dataclass
from typing import List
from .message import Message, MessageType
from ..persistent import Persistent


@dataclass
class LTEMessage(Message, Persistent):
    message_type: str = 'LTERecord'
    message_version: str = '0.1'
    ci: int = None    # cell identification
    earfcn: int = None     # E-UTRA Absolute Radio Frequency
    group_number: int = None   # Used to group records at a single point, (1 active record, multiple neighbor records)
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
    def supports() -> List[MessageType]:
        return sorted([MessageType(message_type='LteRecord', message_version='0.1.0', storage_schema_version='0.1')])

    @staticmethod
    def latest_supported() -> MessageType:
        return LTEMessage.supports()[-1]

    @staticmethod
    def earliest_supported() -> MessageType:
        return LTEMessage.supports()[0]

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
