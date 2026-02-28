from dev_tasks import sync_broker


sync_broker.cron_consumer.start_consume(processes=3)