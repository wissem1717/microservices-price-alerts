# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "aiokafka>=0.11.0",
# ]
# ///

import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BROKER = "localhost:9092"
IN_TOPIC = "btc_alerts_saga"
OUT_TOPIC = "btc_alerts_stored"
GROUP_ID = "btc-storage-saga"

FILE_NAME = "alerts_history_saga.jsonl"


async def main():
    print("[storage-saga] starting")

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()

    try:
        async for msg in consumer:
            try:
                alert = json.loads(msg.value.decode())
            except Exception as e:
                print(f"[storage-saga] invalid json: {e}")
                continue

            with open(FILE_NAME, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert) + "\n")

            print(
                f"[storage-saga] stored alert "
                f"{alert['direction']} {alert['symbol']} change={alert['change_pct']:.4f}%"
            )

            await producer.send_and_wait(OUT_TOPIC, alert)
            print(f"[storage-saga] produced event -> {OUT_TOPIC}")

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())