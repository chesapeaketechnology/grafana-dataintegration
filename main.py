import asyncio
from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential, PartitionContext
from lib.config import ConsumerConfig, Configuration
from lib.handler import MessageHandler
from lib.messages.message import MessageType
from lib.messages.message_repository import MessageTypeRepository
from lib.storage import StorageDelegate, PostgresStorageDelegate
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


async def consume(config: ConsumerConfig, delegate: StorageDelegate):
    # Create a consumer client for the event hub.
    logger.info(f"Consuming {config}")
    client = EventHubConsumerClient(
        fully_qualified_namespace=config.fully_qualified_namespace,
        consumer_group=config.consumer_group,
        eventhub_name=config.topic,
        credential=EventHubSharedKeyCredential(config.shared_access_policy, config.key)
    )

    message_type = MessageType(message_type=config.message_type,
                               message_version=config.message_version)
    handler = MessageHandler(message_type=message_type, storage_delegate=delegate, buffer_size=config.buffer_size)

    async with client:
        # Call the receive method.
        # TODO - Store the state to avoid missing messages.
        # TODO - Look into running multiple processes on the same topic to help with load & availability
        await client.receive(on_event=handler.received_event,
                             on_error=errored,
                             on_partition_close=partition_closed,
                             on_partition_initialize=partition_initialized,
                             starting_position=-1)

if __name__ == '__main__':
    configuration = Configuration.get_config()
    logger.info(f"Starting Grafana DataIntegration with \n{pformat(vars(configuration))}")
    message_type = MessageType(message_type=configuration.consumer.message_type,
                               message_version=configuration.consumer.message_version)
    message_class = MessageTypeRepository.find_message_type(message_type)
    if message_class:
        storage_delegate = PostgresStorageDelegate(config=configuration.database, message_class=message_class)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(consume(config=configuration.consumer, delegate=storage_delegate))
    else:
        raise ValueError("Unable to find configured message type.")

