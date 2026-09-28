# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "aiokafka>=0.11.0",
# ]
# ///

import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER = "localhost:9092"
IN_TOPIC = "btc_alerts_compensate"
GROUP_ID = "btc-storage-compensation"

FILE_NAME = "alerts_compensated_saga.jsonl"


async def main():
    print("[storage-compensation] starting")

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
    )

    await consumer.start()

    try:
        async for msg in consumer:
            try:
                comp = json.loads(msg.value.decode())
            except Exception as e:
                print(f"[storage-compensation] invalid json: {e}")
                continue

            with open(FILE_NAME, "a", encoding="utf-8") as f:
                f.write(json.dumps(comp) + "\n")

            alert = comp.get("original_alert", {})
            print(
                f"[storage-compensation] compensation applied for "
                f"{alert.get('symbol')} reason={comp.get('reason')}"
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())