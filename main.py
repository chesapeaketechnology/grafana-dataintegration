import asyncio
from pprint import pformat
from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from lib.config import ConsumerConfig, Configuration
from lib.storage import PostgresMessageStorageDelegate, MessageStorageDelegate
from lib.handler import MessageHandler
import logging

logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=Configuration.log_level(),
)
logger = logging.getLogger(__name__)


async def errored(partition_context, error):
    """
    The callback function that will be called when an error is raised during receiving after retry
    attempts are exhausted, or during the process of load-balancing.

    :param azure.eventhub.PartitionContext partition_context: The EventHub partition context.
    :param error: the exception
    :return: None
    """
    if partition_context:
        logger.error("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id if partition_context else None, error
        ))
    else:
        logger.error("An exception: {} occurred during the load balance process.".format(error))


async def partition_initialized(partition_context):
    """
    The callback function that will be called after a consumer for a certain partition finishes
    initialization. It would also be called when a new internal partition consumer is created to
    take over the receiving process for a failed and closed internal partition consumer.

    :param azure.eventhub.PartitionContext partition_context: The EventHub partition context.
    :return: None
    """
    logger.info("Partition: {} has been initialized.".format(partition_context.partition_id))


async def partition_closed(partition_context, reason):
    """
    The callback function that will be called after a consumer for a certain partition is closed. It would
    be also called when error is raised during receiving after retry attempts are exhausted.

    :param azure.eventhub.PartitionContext partition_context: The EventHub partition context.
    :param reason: for the close
    :return: None
    """
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

    handler = MessageHandler(
        storage_delegate=delegate,
        buffer_size=config.buffer_size,
        max_buffer_time_in_sec=config.max_buffer_time_in_seconds,
        max_time_to_keep_data_in_seconds=config.max_time_to_keep_data_in_seconds,
        data_eviction_interval_in_seconds=config.data_eviction_interval_in_seconds
    )

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

