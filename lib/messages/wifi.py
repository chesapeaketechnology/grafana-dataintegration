from dataclasses import dataclass

from .message import Message, Persistent


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
