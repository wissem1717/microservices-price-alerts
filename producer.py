# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "confluent-kafka>=2.13.2",
#     "websockets>=16.0",
# ]
# ///

import asyncio
import json
import signal
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from websockets import connect
from websockets.exceptions import ConnectionClosed
from confluent_kafka import Producer

WS_URL = "wss://ws-live-data.polymarket.com"

SUBSCRIPTION_MSG = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": '{"symbol":"btc/usd"}',
        }
    ],
}

# Kafka config (Host machine -> localhost:9092)
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "btc_price"

STOP = asyncio.Event()


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):   #si Kafka a bien reçu le message
    if err is not None:
        print(f"[producer] delivery failed: {err}")
    else:
        print(f"[producer] delivered to {msg.topic()} p={msg.partition()} off={msg.offset()}")


def safe_float(x: Any) -> Optional[float]:    #Cette fonction essaie de convertir une valeur en nombre décimal.
    try:
        return float(x)
    except Exception:
        return None
    
#Cette fonction prend les données reçues du WebSocket et construit un événement propre.

def build_event(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol = payload.get("symbol")
    price = safe_float(payload.get("value"))
    if not symbol or price is None:
        return None
    return {
        "event_type": "PRICE_UPDATE",
        "symbol": symbol,
        "price": price,
        "timestamp": utc_iso(),
        "source": "polymarket",
    }


async def websocket_loop(producer: Producer):
    backoff = 1  # seconds (exponential)
    max_backoff = 20

    while not STOP.is_set():
        try:
            print(f"[producer] connecting websocket: {WS_URL}")
            async with connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print("[producer] websocket connected")
                await ws.send(json.dumps(SUBSCRIPTION_MSG))
                print(f"[producer] subscription sent (topic={KAFKA_TOPIC}, broker={KAFKA_BROKER})")

                backoff = 1  # reset backoff after successful connect

                async for message in ws:
                    if STOP.is_set():
                        break
                    if not message:
                        continue

                    data = json.loads(message)
                    msg_type = data.get("type")
                    payload = data.get("payload") or {}

                    if msg_type != "update":
                        # useful for demo: you can comment this line later if too verbose
                        # print(f"[producer] ws message type={msg_type}")
                        continue

                    event = build_event(payload)
                    if not event:
                        continue

                    print(f"[producer] {event['symbol']} price=${event['price']:.6f} @ {event['timestamp']}")

                    producer.produce(
                        KAFKA_TOPIC,
                        key=event["symbol"].encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)

        except (ConnectionClosed, OSError) as e:
            print(f"[producer] websocket error: {e} -> reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
        except Exception as e:
            print(f"[producer] unexpected error: {e} -> reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


async def main():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        # reliability
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 5,
        "delivery.timeout.ms": 120000,
        "request.timeout.ms": 30000,
    }
    producer = Producer(conf)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, STOP.set)
        except NotImplementedError:
            # Windows: signal handlers can be limited
            pass

    await websocket_loop(producer)

    print("[producer] flushing...")
    producer.flush(10)
    print("[producer] stopped")


if __name__ == "__main__":
    asyncio.run(main())