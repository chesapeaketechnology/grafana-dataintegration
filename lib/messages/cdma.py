from dataclasses import dataclass

from .message import Message


@dataclass
class CDMAMessage(Message):
    group_number: int = None
    sid: int = None
    nid: int = None
    bsid: int = None
    signalStrength: float = None
    ecio: float = None
    servingCell: bool = None
