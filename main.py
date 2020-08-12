import asyncio
from pprint import pformat
from typing import Type

from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

from lib.config import ConsumerConfig, Configuration
from lib.message_store import PostgresMessageStorageDelegate, MessageStorageDelegate
from lib.handler import MessageHandler
from lib.messages.message import PersistentMessage
from lib.messages.message_repository import MessageTypeRepository
from lib.messages.storage import StorageDelegate, PostgresStorageDelegate
import os
import logging

log_level = logging.getLevelName(os.getenv('LOG_LEVEL', 'ERROR'))
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)


async def errored(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        logger.error("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        logger.error("An exception: {} occurred during the load balance process.".format(error))


async def partition_initialized(partition_context):
    # Put your code here.
    logger.info("Partition: {} has been initialized.".format(partition_context.partition_id))


async def partition_closed(partition_context, reason):
    # Put your code here.
    logger.info("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


async def consume(config: ConsumerConfig, delegate: MessageStorageDelegate):
    """
    Setup and start a message topic consumer and storage delegate.
    :param config: A ConsumerConfig object
    :param delegate: A Storage delegate object
    :return: None
    """
    # Create a consumer client for the event hub.
    logger.info(f"Consuming {config}")
    if config.checkpoint_store_conn_str and config.checkpoint_store_container_name:
        # Use an azure blob storage container to store position within partition
        checkpoint_store = BlobCheckpointStore.from_connection_string(config.checkpoint_store_conn_str,
                                                                      config.checkpoint_store_container_name)
        client = EventHubConsumerClient(
            fully_qualified_namespace=config.fully_qualified_namespace,
            consumer_group=config.consumer_group,
            eventhub_name=config.topic,
            credential=EventHubSharedKeyCredential(config.shared_access_policy, config.key),
            checkpoint_store=checkpoint_store
        )
    else:
        client = EventHubConsumerClient(
            fully_qualified_namespace=config.fully_qualified_namespace,
            consumer_group=config.consumer_group,
            eventhub_name=config.topic,
            credential=EventHubSharedKeyCredential(config.shared_access_policy, config.key)
        )

    # message_type = message_class.latest_supported()
    # handler = MessageHandler(message_type=message_type, storage_delegate=delegate, buffer_size=config.buffer_size)
    handler = MessageHandler(storage_delegate=delegate, buffer_size=config.buffer_size)

    async with client:
        await client.receive(on_event=handler.received_event,
                             on_error=errored,
                             on_partition_close=partition_closed,
                             on_partition_initialize=partition_initialized,
                             starting_position=-1)

if __name__ == '__main__':
    configuration = Configuration.get_config()
    logger.info(f"Starting Grafana DataIntegration with \n{pformat(vars(configuration))}")
    storage_delegate = PostgresMessageStorageDelegate(config=configuration.database)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(config=configuration.consumer, delegate=storage_delegate))
    # # message_type_specifier = configuration.consumer.message_type_specifier
    # # message_class = MessageTypeRepository.find_message_type(message_type_specifier)
    # if message_class:
    #     storage_delegate = PostgresStorageDelegate(config=configuration.database, message_class=message_class)
    #     loop = asyncio.get_event_loop()
    #     loop.run_until_complete(consume(config=configuration.consumer,
    #                                     delegate=storage_delegate,
    #                                     message_class=message_class))
    #
    # else:
    #     raise ValueError("Unable to find configured message type.")

