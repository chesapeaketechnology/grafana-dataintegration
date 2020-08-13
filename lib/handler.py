import json
from datetime import datetime
from typing import List

from lib.location import Location
from lib.message import Message
from lib.storage import MessageStorageDelegate, StorageError
import logging

logger = logging.getLogger(__name__)


class MessageHandler:
    """
    Handles the receipt of a new message. Provides for buffering N messages before
    storing to the storage subsystem and coordinates storage of the message.
    """
    def __init__(self, storage_delegate: MessageStorageDelegate, buffer_size: int = 1) -> None:
        super().__init__()
        self.storage_delegate = storage_delegate
        self.buffer_size = buffer_size
        self.buffer: List[Message] = []

    async def received_event(self, partition_context, event):
        """
        The callback function for handling a received event. The callback takes
        two parameters: partition_context which contains partition context and
        event which is the received event.

        For detailed partition context information, please refer to `PartitionContext <https://docs.microsoft.com/en-us/python/api/azure-eventhub/azure.eventhub.partitioncontext?view=azure-python>`_.
        For detailed event information, please refer to `EventData <https://docs.microsoft.com/en-us/python/api/azure-eventhub/azure.eventhub.eventdata?view=azure-python>`_.

        :param azure.eventhub.PartitionContext partition_context: The EventHub partition context. See
        :param Optional[azure.eventhub.EventData] event: The event data
        :return: None
        """
        try:
            logger.debug(f"Received the event: '{event.body_as_str(encoding='UTF-8') if event else 'None'}'"
                         f" from the partition with ID: '{partition_context.partition_id}'")
            logger.debug(partition_context)

            data = json.loads(event.body_as_str(encoding='UTF-8'))
            if 'messageType' in data and 'version' in data and 'data' in data:
                _data = data.get('data', {})
                message = Message(
                    message_type=data.get('messageType'),
                    message_version=data.get('version'),
                    device_id=_data.get('deviceSerialNumber', _data.get('deviceName', None)),
                    device_time=_data.get('deviceTime', datetime.now().timestamp()),
                    location=Location(longitude=_data.get('longitude', 0), latitude=_data.get('latitude', 0), altitude=_data.get('altitude', 0)),
                    data=_data
                )
                self.buffer.append(message)
            if len(self.buffer) > self.buffer_size:
                try:
                    self.storage_delegate.save(self.buffer)
                except StorageError as se:
                    logger.fatal(se)
                self.buffer.clear()

            await partition_context.update_checkpoint(event)
        except Exception as e:
            logger.exception(e)
