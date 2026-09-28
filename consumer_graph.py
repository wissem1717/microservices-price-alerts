# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "aiokafka>=0.11.0",
#     "matplotlib>=3.0.0",
# ]
# ///

import asyncio
import json
import threading
import queue
from collections import deque
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "btc_price"
GROUP_ID = "btc-consumer-graph"

MAX_POINTS = 60  # derniers points affichés

# queue thread-safe : Kafka thread -> main thread (graph)
data_queue = queue.Queue()

# buffers pour le graph
times = deque(maxlen=MAX_POINTS)
prices = deque(maxlen=MAX_POINTS)


async def kafka_consume_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    await consumer.start()
    print(f"[graph-consumer] connected to {KAFKA_BROKER}, topic={KAFKA_TOPIC}, group={GROUP_ID}")

    try:
        async for msg in consumer:
            raw = msg.value
            try:
                data = json.loads(raw)
            except Exception:
                continue

            price = data.get("price")
            if price is None:
                continue

            # label temps lisible
            label = datetime.now().strftime("%H:%M:%S")
            data_queue.put((label, float(price)))

            print(f"[graph-consumer] {label} price={price}")

    except Exception as e:
        print(f"[graph-consumer] error: {e}")
    finally:
        await consumer.stop()
        print("[graph-consumer] closed")


def start_kafka_thread():
    def runner():
        asyncio.run(kafka_consume_loop())

    t = threading.Thread(target=runner, daemon=True)
    t.start()


def main_plot():
    fig, ax = plt.subplots()
    ax.set_title("BTC/USD - Live price (Kafka)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price (USD)")

    line, = ax.plot([], [])  # courbe

    def update(_frame):
        # vider la queue (prendre tous les nouveaux points)
        while True:
            try:
                label, price = data_queue.get_nowait()
                times.append(label)
                prices.append(price)
                ax.set_title(f"BTC/USD - Live price (Kafka) | Last: ${prices[-1]:,.2f}")
            except queue.Empty:
                break

        if len(prices) == 0:
            return line,

        # update data
        x = list(range(len(prices)))
        y = list(prices)
        line.set_data(x, y)

        # axes dynamiques
        ax.set_xlim(0, max(10, len(prices) - 1))
        ymin = min(y)
        ymax = max(y)
        pad = (ymax - ymin) * 0.05 if ymax != ymin else 1.0
        ax.set_ylim(ymin - pad, ymax + pad)

        # ticks temps
        ax.set_xticks(x[::max(1, len(x)//6)])
        ax.set_xticklabels(list(times)[::max(1, len(times)//6)], rotation=45, ha="right")

        fig.tight_layout()
        return line,

    # IMPORTANT: garder la référence sinon animation parfois “freeze”
    anim = FuncAnimation(fig, update, interval=1000, blit=False, cache_frame_data=False)
    plt.show()


if __name__ == "__main__":
    # 1) démarre Kafka consumer dans un thread
    start_kafka_thread()

    # 2) démarre le graph dans le thread principal
    main_plot()