import json
import socket
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# Switch to standard TCP for guaranteed local delivery
TCP_IP = "127.0.0.1"
TCP_PORT = 9999

def send_to_beam(message: str):
    try:
        # Open, send, and close immediately to prevent connection hanging
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((TCP_IP, TCP_PORT))
        s.sendall((message + "\n").encode('utf-8'))
        s.close()
    except Exception as e:
        print(f"❌ Socket Transmission Failed: {e}")
        raise RuntimeError(f"socket transmission failed: {e}") from e

class StreamEvent(BaseModel):
    user_id: str
    stream_id: str
    event_type: str

@app.post("/v1/stream/event")
async def ingest_event(event: StreamEvent):
    try:
        payload = json.dumps(event.model_dump())
        send_to_beam(payload)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))