from dev_tasks import sync_broker

sync_broker.consumer.start_consume(queue="notification", processes=2)