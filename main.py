import asyncio
from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential, PartitionContext
from lib.config import ConsumerConfig, Configuration
from lib.handler import MessageHandler
from lib.storage import StorageDelegate, PostgresStorageDelegate
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def consume(config: ConsumerConfig, storage_delegate: StorageDelegate):
    # Create a consumer client for the event hub.
    logger.info(f"Consuming {config}")
    client = EventHubConsumerClient(
        fully_qualified_namespace=config.fully_qualified_namespace,
        consumer_group=config.consumer_group,
        eventhub_name=config.topic,
        credential=EventHubSharedKeyCredential(config.shared_access_policy, config.primary_key)
    )

    handler = MessageHandler(message_type=config.topic, storage_delegate=storage_delegate, buffer_size=config.buffer_size)

    async with client:
        # Call the receive method.
        # TODO - Store the state to avoid missing messages.
        # TODO - Look into running multiple processes on the same topic to help with load & availability
        await client.receive(on_event=handler.received_event, starting_position=-1)


# TODO: Change to using environment variables for configuration.
if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, 'config.yaml')
    config = Configuration(config_file=config_path)
    configs = config.get_consumers()
    storage_delegate = PostgresStorageDelegate(config=config.get_database_config())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(config=configs[0], storage_delegate=storage_delegate))

