import json
from datetime import datetime, timezone
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
    def __init__(self, storage_delegate: MessageStorageDelegate,
                 buffer_size, max_buffer_time_in_sec,
                 max_time_to_keep_data_in_seconds, data_eviction_interval_in_seconds, checkpoint_after_messages ) -> None:
        super().__init__()
        self.storage_delegate = storage_delegate
        self.buffer_size = buffer_size
        self.max_buffer_time_in_sec = max_buffer_time_in_sec
        self.data_eviction_interval_in_seconds = data_eviction_interval_in_seconds
        self.max_time_to_keep_data_in_seconds = max_time_to_keep_data_in_seconds
        self.checkpoint_after_messages = checkpoint_after_messages
        self.buffer: List[Message] = []
        self.last_buffer_flush = datetime.now(timezone.utc)
        self.last_eviction_time = datetime.now(timezone.utc)
        self.checkpoint_count = 0

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
                    location=Location(longitude=_data.get('longitude', 0),
                                      latitude=_data.get('latitude', 0),
                                      altitude=_data.get('altitude', 0)),
                    data=_data
                )
                self.buffer.append(message)

            buffer_delta = datetime.now(timezone.utc) - self.last_buffer_flush
            if len(self.buffer) >= self.buffer_size or buffer_delta.total_seconds() > self.max_buffer_time_in_sec:
                self.checkpoint_count += len(self.buffer)
                self.flush_buffer()
                if self.checkpoint_count > self.checkpoint_after_messages:
                    try:
                        await partition_context.update_checkpoint(event)
                        self.checkpoint_count = 0
                        logger.info("Checkpoint Updated.")
                    except Exception as ue:
                        logger.error(str(ue))

            evict_delta = datetime.now(timezone.utc) - self.last_eviction_time
            if evict_delta.total_seconds() > self.data_eviction_interval_in_seconds:
                eviction_cutoff = datetime.fromtimestamp(
                    datetime.now(timezone.utc).timestamp() - self.max_time_to_keep_data_in_seconds, tz=timezone.utc
                )
                self.storage_delegate.evict(eviction_cutoff)

        except Exception as e:
            logger.exception(e)

    def flush_buffer(self):
        try:
            self.storage_delegate.save(self.buffer)
        except StorageError as se:
            logger.fatal(se)
        self.buffer.clear()
        self.last_buffer_flush = datetime.now(timezone.utc)
