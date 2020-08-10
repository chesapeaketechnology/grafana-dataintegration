from typing import List, Type, Union

from lib.messages.lte import LTEMessage
from lib.messages.message import MessageType, MessageTypeSpecifier


class MessageTypeRepository:
    message_types: List[Type['Message']] = [
        LTEMessage
    ]

    @staticmethod
    def find_message_type(message_type: Union[MessageType, MessageTypeSpecifier]):
        return next(filter(lambda t: t.is_compatible_with(message_type), MessageTypeRepository.message_types), None)

