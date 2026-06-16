import os
import time
import random
from locust import HttpUser, task, between

class WeverseFanSimulator(HttpUser):
    host = os.getenv("LOCUST_HOST", "http://localhost:8080")
    # Simulate a realistic user delay between actions (e.g., waiting 1 to 3 seconds)
    wait_time = between(1, 3)

    @task(1)
    def join_live_stream(self):
        """Simulates a fan clicking on the live stream notification."""
        user_id = f"fan_{random.randint(100000, 999999)}"
        
        payload = {
            "user_id": user_id,
            "stream_id": "bts_live_2026",
            "event_type": "join_room",
            "timestamp": int(time.time())
        }
        
        # Send a POST request to our FastAPI ingestion gateway
        self.client.post("/v1/stream/event", json=payload)

    @task(3)
    def send_heartbeat(self):
        """Simulates the app sending continuous background pings to keep the connection alive."""
        user_id = f"fan_{random.randint(100000, 999999)}"
        
        payload = {
            "user_id": user_id,
            "stream_id": "bts_live_2026",
            "event_type": "heartbeat",
            "timestamp": int(time.time())
        }
        
        self.client.post("/v1/stream/event", json=payload)