import json
from typing import List

from lib.message import Message
from lib.storage import StorageDelegate
import logging

logger = logging.getLogger(__name__)


class MessageHandler:

    def __init__(self, storage_delegate: StorageDelegate, buffer_size: int = 1) -> None:
        super().__init__()
        self.storage_delegate = storage_delegate
        self.buffer_size = buffer_size
        self.buffer: List[Message] = []

    async def received_event(self, partition_context, event):
        # Print the event data.
        print("Received the event: \"{}\" from the partition with ID: \"{}\""
              .format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

        data = json.loads(event.body_as_str(encoding='UTF-8'))
        self.buffer.append(Message.create(data.get('topic'), data))
        if len(self.buffer) > self.buffer_size:
            for message in self.buffer:
                self.storage_delegate.save(message)
            self.buffer.clear()

        # Update the checkpoint so that the program doesn't read the events
        # that it has already read when you run it next time.
        await partition_context.update_checkpoint(event)
