import os
import json
import time
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("RAW_TOPIC", "traffic.raw")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")

POINTS = [
    {"segment_id": "blr_mgroad", "lat": 12.9755, "lon": 77.6041},
    {"segment_id": "blr_silkboard", "lat": 12.9177, "lon": 77.6233},
    {"segment_id": "blr_majestic", "lat": 12.9784, "lon": 77.5720},
    {"segment_id": "blr_hebball", "lat": 13.0358, "lon": 77.5970},
]

if not TOMTOM_API_KEY:
    raise RuntimeError("Set TOMTOM_API_KEY environment variable to use TomTom producer")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=100,
        acks="all",
        retries=3,
    )

async def fetch_tomtom(session, lat, lon):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?key={TOMTOM_API_KEY}&point={lat},{lon}"
    try:
        async with session.get(url, timeout=8) as r:
            if r.status == 200:
                return await r.json()
            else:
                print(f"⚠️ HTTP {r.status} for {lat},{lon}")
    except asyncio.TimeoutError:
        print(f"⏰ Timeout for {lat},{lon}")
    except Exception as e:
        print(f"❌ Fetch error for {lat},{lon}: {e}")
    return None

async def produce_message(producer, topic, key, msg):
    """Run blocking Kafka send in background thread"""
    await asyncio.to_thread(producer.send, topic, key=key, value=msg)

async def main_async(interval_sec=30):
    producer = create_producer()
    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\n=== 🕒 Iteration {iteration} at {datetime.now().isoformat()} ===")

            async with aiohttp.ClientSession() as session:
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*[fetch_tomtom(session, p["lat"], p["lon"]) for p in POINTS]),
                        timeout=15,
                    )
                except asyncio.TimeoutError:
                    print("⚠️ Global fetch timeout — skipping this batch")
                    await asyncio.sleep(interval_sec)
                    continue
            IST = timezone(timedelta(hours=5, minutes=30))
            now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

            for p, data in zip(POINTS, results):
                if not data or "flowSegmentData" not in data:
                    print(f"TomTom returned no flowSegmentData for {p['segment_id']}")
                    continue

                flow = data["flowSegmentData"]
                freeflow = flow.get("freeFlowSpeed")
                current = flow.get("currentSpeed")
                congestion = None
                if freeflow and freeflow > 0 and current is not None:
                    congestion = round(max(0.0, min(1.0, 1.0 - (current / freeflow))), 3)

                msg = {
                    "segment_id": p["segment_id"],
                    "timestamp_ist": now_ist,
                    "lat": p["lat"],
                    "lon": p["lon"],
                    "speed_current_kmph": current,
                    "speed_freeflow_kmph": freeflow,
                    "travel_time_sec": flow.get("currentTravelTime"),
                    "congestion_index": congestion,
                    "source": "tomtom",
                }

                key = f"{p['segment_id']}|{now_ist}"
                await produce_message(producer, TOPIC, key, msg)
                print(f"🚦 Sent -> {key} | {msg}")

            await asyncio.to_thread(producer.flush)
            await asyncio.sleep(interval_sec)

    except KeyboardInterrupt:
        print("\n🛑 TomTom producer stopped by user")
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main_async(interval_sec=int(os.getenv("TOMTOM_INTERVAL", "30"))))
