import decimal
import hashlib
import json
import numbers
from dataclasses import dataclass
from datetime import datetime
from typing import Union

from dateutil import parser

from lib.location import Location
from lib.persistent import Persistent


class Message(Persistent):
    """
    Data class for used as the base class for all message types.
    """
    def __init__(self,
                 message_type: str,
                 message_version: str,
                 device_id: str,
                 device_time: int,
                 location: Location,
                 data: dict,
                 ) -> None:
        super().__init__()
        self.message_type = message_type
        self.message_version = message_version
        self.device_id = device_id
        self.device_time = device_time
        self.device_timestamp = self.convert_to_timestamp(device_time)

        self.location = location
        self.data = data
        self.json_data = json.dumps(data) if data else None
        self.id = self.generate_id()

    def convert_to_timestamp(self, device_time):
        if device_time is None:
            return None
        if isinstance(device_time, str):
            if device_time.isnumeric():
                device_time = int(device_time)
            else:
                return parser.parse(device_time)
        if isinstance(device_time, (int, float, decimal.Decimal)):
            now = datetime.now().timestamp()
            dt = device_time
            while dt > now:
                dt = device_time / 1000.0
            return datetime.fromtimestamp(dt)
        else:
            raise ValueError(f"Unable to convert device_time [{device_time}] to timestamp")

    def generate_id(self):
        d = vars(self)
        hash_string = ''.join([str(d[x]) for x in sorted(d.keys())])
        sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
        return sha_signature

    @staticmethod
    def table_name() -> str:
        return "message"

    @staticmethod
    def create_table_statements() -> [str]:
        return [
            "CREATE EXTENSION IF NOT EXISTS postgis;",
            """
                create table public.message
                (
                    id varchar(64) not null 
                        constraint message_pk primary key,
                    message_type varchar(50),
                    message_version varchar(10),
                    device_id text,
                    device_timestamp timestamp with time zone,
                    location geography(POINT),
                    data jsonb
                );
            
            """,
            "create index message_type_idx on public.message(message_type);",
            "create index message_type_version_idx on public.message(message_type, message_version);",
            "create index message_device_id_idx on public.message(device_id);",
            "create index message_device_ts_idx on public.message(device_timestamp);",
            "create index message_location_idx on public.message using GIST(location);",
            "create index message_type_device_id_idx on public.message(message_type, device_id);",
            "create index message_type_device_ts_idx on public.message(message_type, device_timestamp);",
        ]

    @staticmethod
    def insert_statement() -> str:
        return """INSERT INTO public.message  
                  VALUES (
                    %(id)s, 
                    %(message_type)s, 
                    %(message_version)s, 
                    %(device_id)s, 
                    %(device_timestamp)s, 
                    %(location)s, 
                    %(json_data)s  
                   )
                   ON CONFLICT DO NOTHING;
                """
