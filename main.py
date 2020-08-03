import asyncio
from multiprocessing import Process
from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential
from lib.config import ConsumerConfig, Configuration
from lib.handler import MessageHandler
from lib.storage import StorageDelegate, PostgresStorageDelegate
import os
import logging

logging.basicConfig(level=logging.DEBUG)
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

    handler = MessageHandler(storage_delegate=storage_delegate, buffer_size=config.buffer_size)
    async with client:
        # Call the receive method.
        # await client.receive(on_event=handler.received_event)
        await client.receive(on_event=lambda  partition_context, event: print("Received the event: \"{}\" from the partition with ID: \"{}\""
                      .format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id)))


async def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, 'config.yaml')
    config = Configuration(config_file=config_path)
    configs = config.get_consumers()
    storage_delegate = PostgresStorageDelegate(config=config.get_database_config())
    # for config in configs:
    #     # p = Process(target=consume, kwargs={'config': config, 'storage_delegate': storage_delegate})
    #     # p.start()
    #     await consume(config=config, storage_delegate=storage_delegate)
    #     # asyncio.create_task(consume(config=config, storage_delegate=storage_delegate))
    await consume(config=configs[0], storage_delegate=storage_delegate)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
    # try:
    #     main()
    #     loop.run_forever()
    # finally:
    #     loop.run_until_complete(loop.shutdown_asyncgens())
    #     loop.close()
