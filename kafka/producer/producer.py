import json
import signal
import time
import logging
from typing import Optional

from confluent_kafka import Producer

from config import Config
from utils import (
    fetch_stock_data_polygon,
    fetch_stock_data_alpha_vantage,
    validate_stock_data,
    format_stock_message,
    is_market_open,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

running = True


def delivery_report(err, msg) -> None:
    """Callback invoked once a message is delivered or delivery fails."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def create_producer() -> Producer:
    """Initialize and return a confluent-kafka Producer."""
    producer_config = {
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": Config.PRODUCER_ACKS,
        "compression.type": Config.PRODUCER_COMPRESSION,
        "retries": Config.PRODUCER_RETRIES,
    }
    return Producer(producer_config)


def fetch_stock_data(symbol: str) -> Optional[dict]:
    """Fetch stock data using Polygon as primary, Alpha Vantage as fallback."""
    if Config.POLYGON_API_KEY:
        data = fetch_stock_data_polygon(symbol, Config.POLYGON_API_KEY)
        if data is not None:
            return data

    if Config.ALPHA_VANTAGE_API_KEY:
        data = fetch_stock_data_alpha_vantage(symbol, Config.ALPHA_VANTAGE_API_KEY)
        if data is not None:
            return data

    logger.warning(f"Failed to fetch data for {symbol} from all sources")
    return None


def produce_stock_data(producer: Producer) -> int:
    """Fetch and publish stock data for all configured symbols.

    Returns the number of messages successfully produced.
    """
    produced_count = 0

    for symbol in Config.STOCK_SYMBOLS:
        data = fetch_stock_data(symbol)
        if data is None:
            continue

        if not validate_stock_data(data):
            continue

        logger.info(format_stock_message(data))

        producer.produce(
            topic=Config.KAFKA_TOPIC,
            key=symbol.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report,
        )
        produced_count += 1

    producer.flush()
    return produced_count


def shutdown_handler(signum, frame) -> None:
    """Set the running flag to False for graceful shutdown."""
    global running
    logger.info(f"Received signal {signum}, shutting down...")
    running = False


def run() -> None:
    """Main loop: fetch stock data and publish to Kafka on an interval."""
    Config.validate()

    producer = create_producer()
    logger.info(
        f"Producer started -- broker={Config.KAFKA_BOOTSTRAP_SERVERS}, "
        f"topic={Config.KAFKA_TOPIC}, "
        f"symbols={Config.STOCK_SYMBOLS}, "
        f"interval={Config.FETCH_INTERVAL}s"
    )

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while running:
            # if not is_market_open():
            #     logger.info("Market is closed, waiting...")
            #     time.sleep(Config.FETCH_INTERVAL)
            #     continue

            count = produce_stock_data(producer)
            logger.info(f"Produced {count}/{len(Config.STOCK_SYMBOLS)} messages")
            time.sleep(Config.FETCH_INTERVAL)
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        logger.info("Producer shut down")


if __name__ == "__main__":
    run()
