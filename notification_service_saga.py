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
IN_TOPIC = "btc_alerts_stored"
COMP_TOPIC = "btc_alerts_compensate"
GROUP_ID = "btc-notification-saga"

FAIL_NOTIFICATIONS = False


async def main():
    global FAIL_NOTIFICATIONS

    print("[notification-saga] starting")
    print("[notification-saga] type 'fail' or 'ok' in this terminal to toggle failure")

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

    loop = asyncio.get_running_loop()

    async def command_listener():
        global FAIL_NOTIFICATIONS
        while True:
            cmd = await loop.run_in_executor(None, input, "")
            cmd = cmd.strip().lower()
            if cmd == "fail":
                FAIL_NOTIFICATIONS = True
                print("[notification-saga] FAIL_NOTIFICATIONS = True")
            elif cmd == "ok":
                FAIL_NOTIFICATIONS = False
                print("[notification-saga] FAIL_NOTIFICATIONS = False")

    asyncio.create_task(command_listener())

    try:
        async for msg in consumer:
            try:
                alert = json.loads(msg.value.decode())
            except Exception as e:
                print(f"[notification-saga] invalid json: {e}")
                continue

            if FAIL_NOTIFICATIONS:
                print("[notification-saga] notification FAILED -> sending compensation")
                compensation = {
                    "type": "COMPENSATE_ALERT",
                    "original_alert": alert,
                    "reason": "notification_failed"
                }
                await producer.send_and_wait(COMP_TOPIC, compensation)
                continue

            print(
                f"[notification-saga] notification sent for "
                f"{alert['symbol']} {alert['direction']} change={alert['change_pct']:.4f}%"
            )

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())