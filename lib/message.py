import decimal
import hashlib
import json
import uuid
from datetime import datetime, timedelta

from dateutil import parser
from dateutil.tz import UTC

from lib.location import Location
from lib.persistent import Persistent


class MessageType:
    @staticmethod
    def get_source_id(message_type: str, message_version: str, message_data: dict):
        """
        -- SQL to update existing data
        alter table message add column source_id text;
        select
            id,
            message_type,
            message_version,
            device_timestamp,
            case
                when message_type = 'LteRecord' and data->'servingCell' = 'true' then
                    coalesce(data->>'mcc', '') || '-' || coalesce(data->>'mnc', '') || '-' || coalesce(data->>'eci', '')
                when message_type = 'GsmRecord' and data->'servingCell' = 'true' then
                    coalesce(data->>'mcc', '') || '-' || coalesce(data->>'mnc', '') || '-' || coalesce(data->>'lac', '') || '-' || coalesce(data->>'ci', '')
                when message_type = 'CdmaRecord' and data->'servingCell' = 'true' then
                    coalesce(data->>'sid', '') || '-' || coalesce(data->>'nid', '') || '-' || coalesce(data->>'bsid', '')
                when message_type = 'UmtsRecord' and data->'servingCell' = 'true' then
                    coalesce(data->>'mcc', '') || '-' || coalesce(data->>'mnc', '') || '-' || coalesce(data->>'lac', '') || '-' || coalesce(data->>'cid', '')
                when message_type = 'WifiBeaconRecord' then
                    data->>'bssid'
                when message_type = 'GnssRecord' then
                    coalesce(data->>'constellation', '') || '-' || coalesce(data->>'spaceVehicleId', '')
                when message_type = 'EnergyDetection' then
                    coalesce(data->>'frequencyHz', '')
                when message_type = 'SignalDetection' then
                    coalesce(data->>'signalName', '') || '-' || coalesce(data->>'frequencyHz', '')
                when message_type = 'DeviceStatus' then
                    device_id
                else NULL
            end as "source_id",
               data
        from message
        """
        try:
            if message_type == "GsmRecord" and message_data.get('servingCell'):
                return f"{message_data.get('mcc')}-{message_data.get('mnc')}-{message_data.get('lac')}-{message_data.get('ci')}"
            elif message_type == "CdmaRecord" and message_data.get('servingCell'):
                return f"{message_data.get('sid')}-{message_data.get('nid')}-{message_data.get('bsid')}"
            elif message_type == "UmtsRecord" and message_data.get('servingCell'):
                return f"{message_data.get('mcc')}-{message_data.get('mnc')}-{message_data.get('lac')}-{message_data.get('cid')}"
            elif message_type == "LteRecord" and message_data.get('servingCell'):
                return f"{message_data.get('mcc')}-{message_data.get('mnc')}-{message_data.get('eci')}"
            elif message_type == "WifiBeaconRecord":
                return message_data.get('bssid')
            elif message_type == "GnssRecord":
                return f"{message_data.get('constellation')}-{message_data.get('spaceVehicleId')}"
            elif message_type == "EnergyDetection":
                return f"{message_data.get('frequencyHz', None)}"
            elif message_type == "SignalDetection":
                return f"{message_data.get('signalName', None)}-{message_data.get('frequencyHz', None)}"
            elif message_type == "DeviceStatus":
                return message_data.get('deviceSerialNumber', message_data.get('deviceName', None))
            else:
                return None
        except Exception:
            return None

class Message(Persistent):
    """
    Data class for used as the base class for all message types.
    """
    def __init__(self,
                 message_type: str,
                 message_version: str,
                 device_id: str,
                 source_id: str,
                 device_time: int,
                 location: Location,
                 data: dict,
                 ) -> None:
        super().__init__()
        self.message_type = message_type
        self.message_version = message_version
        self.device_id = device_id
        self.source_id = source_id
        self.device_time = device_time
        self.device_timestamp = self.convert_to_timestamp(device_time)

        self.location = location
        self.data = data
        self.json_data = json.dumps(data) if data else None
        self.id = self.generate_id()

    def convert_to_timestamp(self, device_time):
        """
        Converts the provided device_time from an integer string,
        an iso8601 string, or an int to a datetime timestamp.
        If an epoch time is provided as an integer, it will move
        precision below seconds to the right side of the decimal.

        :param device_time: epoch or iso8601 representation
        :return: datetime
        """
        if device_time is None:
            return None
        if isinstance(device_time, str):
            if device_time.isnumeric():
                device_time = int(device_time)
            else:
                return parser.parse(device_time)
        if isinstance(device_time, (int, float, decimal.Decimal)):
            now = datetime.utcnow().timestamp()
            dt = device_time
            while dt > (now * 100.0):
                dt = dt / 1000.0
            return datetime.utcfromtimestamp(dt)
        else:
            raise ValueError(f"Unable to convert device_time [{device_time}] to timestamp")

    def generate_id(self) -> str:
        """
        Generate a unique sha256 hash representing the fields of the message.

        :return: str signature
        """
        d = vars(self)
        hash_string = ''.join([str(d[x]) for x in sorted(d.keys())])
        sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
        return sha_signature
        # return str(uuid.uuid4())

    @staticmethod
    def table_name() -> str:
        return "message"

    @staticmethod
    def create_table_statements() -> [str]:
        """
        A series of sql statements for settings up database artifacts necessary to store and query the messages.

        :return: a list of sql statements
        """
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
                    data jsonb,
                    source_id text
                );
            
            """,
            "create index message_type_idx on public.message(message_type);",
            "create index message_type_version_idx on public.message(message_type, message_version);",
            "create index message_device_id_idx on public.message(device_id);",
            "create index message_source_id_idx on public.message(source_id);",
            "create index message_device_ts_idx on public.message(device_timestamp);",
            "create index message_location_idx on public.message using GIST(location);",
            "create index message_type_device_id_idx on public.message(message_type, device_id);",
            "create index message_type_device_ts_idx on public.message(message_type, device_timestamp);",
        ]

    @staticmethod
    def insert_statement() -> str:
        """Parameterized sql for inserting a message into the database."""
        return """INSERT INTO public.message
                  VALUES (
                    %(id)s,
                    %(message_type)s,
                    %(message_version)s,
                    %(device_id)s,
                    %(device_timestamp)s,
                    %(location)s,
                    %(json_data)s,
                    %(source_id)s
                   )
                   ON CONFLICT DO NOTHING;
                """

    @staticmethod
    def delete_statement() -> str:
        """Parametrized query for removing aged out messages"""
        return """delete from public.message where device_timestamp <= %(device_timestamp)s"""

