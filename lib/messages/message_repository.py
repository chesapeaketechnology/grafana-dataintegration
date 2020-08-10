from typing import List, Type

from lib.messages.lte import LTEMessage
from lib.messages.message import MessageType


class MessageTypeRepository:
    message_types: List[Type['Message']] = [
        LTEMessage
    ]

    @staticmethod
    def find_message_type(message_type: MessageType):
        return next(filter(lambda t: t.is_compatible_with(message_type), MessageTypeRepository.message_types), None)

    #
    # @staticmethod
    # def register_message_type(message_class: Type['Message']):
    #     MessageTypeRepository.message_types.append(message_class)
