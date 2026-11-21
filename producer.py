import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Connect to our local Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting Data Generators (Press Ctrl+C to stop)...")

try:
    while True:
        # 1. Simulate FleetFabric (Trucks on the N3)
        truck_data = {
            "truck_id": f"TRK-{random.randint(100, 999)}",
            "lat": round(random.uniform(-29.8, -26.2), 4), # Between Durban & Joburg
            "lon": round(random.uniform(29.0, 28.0), 4),
            "status": random.choice(["Empty", "Loaded"]),
            "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('truck-telemetry', truck_data)

        # 2. Simulate Load Board (Cargo needing return trips)
        if random.random() > 0.6: # Send less frequently
            load_data = {
                "load_id": f"LOAD-{random.randint(1000, 9999)}",
                "origin_lat": round(random.uniform(-26.3, -26.0), 4), # Near Joburg
                "origin_lon": round(random.uniform(28.0, 28.2), 4),
                "destination": random.choice(["Durban", "Maputo"]),
                "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            producer.send('load-board-available', load_data)
            print(f"Sent Load: {load_data['load_id']} | Sent Truck: {truck_data['truck_id']}")

        time.sleep(2) # Wait 2 seconds before sending the next batch
except KeyboardInterrupt:
    print("Stopping generator.")
finally:
    producer.flush()
