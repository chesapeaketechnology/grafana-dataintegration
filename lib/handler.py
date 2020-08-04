import json
from typing import List

from lib.message import Message
from lib.storage import StorageDelegate
import logging

logger = logging.getLogger(__name__)


class MessageHandler:

    def __init__(self, message_type: str, storage_delegate: StorageDelegate, buffer_size: int = 1) -> None:
        super().__init__()
        self.storage_delegate = storage_delegate
        self.buffer_size = buffer_size
        self.message_type = message_type
        self.buffer: List[Message] = []

    async def received_event(self, partition_context, event):
        # Print the event data.
        # print(partition_context)
        # print(json.loads(event.body_as_str(encoding='UTF-8')))
        try:
            logger.debug(f"Received the event: '{event.body_as_str(encoding='UTF-8') if event else 'None'}'"
                         f" from the partition with ID: '{partition_context.partition_id}'")

            data = json.loads(event.body_as_str(encoding='UTF-8'))
            self.buffer.append(Message.create(self.message_type, data))
            if len(self.buffer) > self.buffer_size:
                for message in self.buffer:
                    self.storage_delegate.save(message)
                self.buffer.clear()

            # Update the checkpoint so that the program doesn't read the events
            # that it has already read when you run it next time.
            await partition_context.update_checkpoint(event)
        except Exception as e:
            logger.exception(e)
