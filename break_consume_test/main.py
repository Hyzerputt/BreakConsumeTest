#! usr/bin/env python3.7
import json

from loguru import logger

from break_consume_test.rabbitmq import ConsumerConnector

RABBITMQ_CONFIG = {"rabbitmq_host": "127.0.0.1",
                   "rabbitmq_port": 5672,
                   "rabbitmq_user": "guest",
                   "rabbitmq_pass": "guest",
                   "consumer_queue": "test_break_consume", }

consumer = ConsumerConnector(**RABBITMQ_CONFIG)

def consumer_call_back(ch, method, properties, body):
    logger.debug("Starting Callback")
    message = json.loads(body)
    logger.debug(f"Received message {message}")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if message.get("STOP", False):
        logger.debug("Message contains stop")
        logger.warning("Stopping Consume from inside callback")
        ch.stop_consuming()
        return
    logger.debug("Message did NOT contain stop.  Continuing consume")


@logger.catch()
def perpetual_consume():
    consumer.connect()
    logger.info(f"Connected to RabbitMQ on {consumer.connection_parameters}")
    logger.info("Staring Perepetual Consume")
    consumer.listen(callback=consumer_call_back)
    logger.info("Perpetual Consume was stopped")
    consumer.disconnect()

@logger.catch()
def bounded_consume():
    consumer.connect()
    logger.info(f"Connected to RabbitMQ on {consumer.connection_parameters}")
    logger.info("Staring Bounded Consume")

    messages = consumer.consume(inactivity_timeout=30)
    logger.debug("Start message processing")
    for method, properties, body in messages:
        logger.debug(f"Processing message {body}")
        if body is None:
            logger.debug("Finish message processing")
            break


    logger.info("Bounded Consume finished")
    consumer.disconnect()

if __name__ == "__main__":
    #perpetual_consume()
    bounded_consume()