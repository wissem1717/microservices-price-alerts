# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "aiokafka>=0.11.0",
# ]
# ///

import asyncio
import json
from datetime import datetime
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "btc_alerts"
GROUP_ID = "btc-storage"

FILE_NAME = "alerts_history.jsonl"


async def main():

    print("[storage] starting consumer")

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
    )

    await consumer.start()

    try:
        async for msg in consumer:

            alert = json.loads(msg.value.decode())

            with open(FILE_NAME, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert) + "\n")

            print(
                f"[storage] saved alert {alert['direction']} "
                f"{alert['symbol']} change={alert['change_pct']:.4f}%"
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())