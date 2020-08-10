from dataclasses import dataclass

from .message import Message


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
