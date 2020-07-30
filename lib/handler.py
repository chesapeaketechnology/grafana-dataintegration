import json

from lib import message
from lib.adapter import Adapter
from lib.message import Message


class MessageHandler:

    def __init__(self, adapter: Adapter,  buffer_size: int = 1) -> None:
        super().__init__()
        self.adapter = adapter
        self.buffer_size = buffer_size
        self.buffer = []

    async def received_event(self, partition_context, event):
        # Print the event data.
        # print("Received the event: \"{}\" from the partition with ID: \"{}\""
        #       .format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

        data = json.loads(event.body_as_str(encoding='UTF-8'))
        self.buffer.append(Message.create(data.get('topic'), data))
        if len(self.buffer) > self.buffer_size:
            self.adapter.write(self.buffer)
            self.buffer.clear()

        # Update the checkpoint so that the program doesn't read the events
        # that it has already read when you run it next time.
        await partition_context.update_checkpoint(event)
