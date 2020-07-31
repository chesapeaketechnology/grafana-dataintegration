import asyncio
from multiprocessing import Process
from azure.eventhub.aio import EventHubConsumerClient, EventHubSharedKeyCredential
from lib.adapter import DatabaseAdapter
from lib.config import ConsumerConfig, Configuration
from lib.handler import MessageHandler


def consume(config: ConsumerConfig, adapter: DatabaseAdapter):
    # Create a consumer client for the event hub.
    client = EventHubConsumerClient(
        fully_qualified_namespace=config.fully_qualified_namespace,
        consumer_group=config.consumer_group,
        eventhub_name=config.topic,
        credential=EventHubSharedKeyCredential(config.shared_access_policy, config.primary_key)
    )

    handler = MessageHandler(adapter=adapter, buffer_size=config.buffer_size)
    async with client:
        # Call the receive method.
        await client.receive(on_event=handler.received_event)


async def main():
    config = Configuration(config_file='config.yaml')
    configs = config.get_consumers()
    with DatabaseAdapter(config.get_database_config()) as adapter:
        for config in configs:
            p = Process(target=consume, kwargs={'config': config, 'adapter': adapter})
            p.start()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
