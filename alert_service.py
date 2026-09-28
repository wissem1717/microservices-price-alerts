# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "aiokafka>=0.11.0",
# ]
# ///
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque
from typing import Deque, Dict, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


# Config

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "btc_price")
OUT_TOPIC = os.getenv("OUT_TOPIC", "btc_alerts")

SYMBOL = os.getenv("SYMBOL", "btc/usd")

# Demo-friendly defaults
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "30"))  # sliding window length
THRESHOLD_UP_PCT = float(os.getenv("THRESHOLD_UP_PCT", "0.05"))   # +0.05%
THRESHOLD_DOWN_PCT = float(os.getenv("THRESHOLD_DOWN_PCT", "0.05"))  # -0.05%

# To avoid spamming alerts
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "10"))

# Consumer group
GROUP_ID = os.getenv("GROUP_ID", "btc-alert-service")

# Logging
VERBOSE = os.getenv("VERBOSE", "1") == "1"


@dataclass
class PricePoint:
    ts: float   # epoch seconds
    price: float


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def pct_change(from_price: float, to_price: float) -> float:
    # percent change from -> to
    if from_price == 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def build_alert(
    symbol: str,
    base_price: float,
    current_price: float,
    change_pct: float,
    base_ts: float,
    current_ts: float,
    reason: str,
    source: str,
) -> dict:
    direction = "UP" if change_pct >= 0 else "DOWN"
    return {
        "type": "PRICE_ALERT",
        "symbol": symbol,
        "direction": direction,
        "change_pct": change_pct,
        "base_price": base_price,
        "current_price": current_price,
        "window_seconds": WINDOW_SECONDS,
        "base_time_utc": datetime.fromtimestamp(base_ts, tz=timezone.utc).isoformat(),
        "current_time_utc": datetime.fromtimestamp(current_ts, tz=timezone.utc).isoformat(),
        "reason": reason,
        "source": source,
        "emitted_at_utc": utc_iso(),
    }


async def main():
    print(f"[alert] broker={KAFKA_BROKER}")
    print(f"[alert] consume={IN_TOPIC} -> produce={OUT_TOPIC}")
    print(
        f"[alert] window={WINDOW_SECONDS}s thresholds: +{THRESHOLD_UP_PCT}% / -{THRESHOLD_DOWN_PCT}% "
        f"(symbol={SYMBOL}) cooldown={COOLDOWN_SECONDS}s"
    )

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Store last WINDOW_SECONDS of prices
    window: Deque[PricePoint] = deque()

    # Cooldown per symbol
    last_alert_at: Dict[str, float] = {}

    await consumer.start()
    await producer.start()
    print("[alert] started")

    try:
        async for msg in consumer:
            # Parse JSON
            try:
                raw = msg.value
                data = json.loads(raw)
            except Exception:
                if VERBOSE:
                    print("[alert] skip: invalid json")
                continue

            symbol = data.get("symbol")
            if symbol != SYMBOL:
                # ignore other symbols if any
                continue

            price = safe_float(data.get("price"))
            if price is None:
                if VERBOSE:
                    print("[alert] skip: missing/invalid price")
                continue

            # Event time: try message timestamp field, else use now
            now_ts = datetime.now(timezone.utc).timestamp()

            # push to window
            window.append(PricePoint(ts=now_ts, price=price))

            # pop old points
            cutoff = now_ts - WINDOW_SECONDS
            while window and window[0].ts < cutoff:
                window.popleft()

            if len(window) < 2:
                continue

            base = window[0]
            current = window[-1]
            change = pct_change(base.price, current.price)

            if VERBOSE:
                print(f"[alert] price={price:.6f} window={len(window)} change={change:+.4f}%")

            # Check cooldown
            last = last_alert_at.get(symbol, 0.0)
            if now_ts - last < COOLDOWN_SECONDS:
                continue

            # Trigger conditions
            if change >= THRESHOLD_UP_PCT:
                alert = build_alert(
                    symbol=symbol,
                    base_price=base.price,
                    current_price=current.price,
                    change_pct=change,
                    base_ts=base.ts,
                    current_ts=current.ts,
                    reason=f"price increased >= +{THRESHOLD_UP_PCT}%",
                    source=str(data.get("source", "unknown")),
                )
                await producer.send_and_wait(OUT_TOPIC, alert)
                last_alert_at[symbol] = now_ts
                print(f"[alert]  EMIT {symbol} {change:+.4f}% -> {OUT_TOPIC}")

            elif change <= -THRESHOLD_DOWN_PCT:
                alert = build_alert(
                    symbol=symbol,
                    base_price=base.price,
                    current_price=current.price,
                    change_pct=change,
                    base_ts=base.ts,
                    current_ts=current.ts,
                    reason=f"price decreased <= -{THRESHOLD_DOWN_PCT}%",
                    source=str(data.get("source", "unknown")),
                )
                await producer.send_and_wait(OUT_TOPIC, alert)
                last_alert_at[symbol] = now_ts
                print(f"[alert]  EMIT {symbol} {change:+.4f}% -> {OUT_TOPIC}")

    finally:
        await consumer.stop()
        await producer.stop()
        print("[alert] stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[alert] stopped by user")