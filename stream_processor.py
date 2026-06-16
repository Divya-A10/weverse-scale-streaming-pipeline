import json
import socket
import psycopg2
import threading
import queue
import datetime
import time

data_queue = queue.Queue()

# --- 1. TCP Listener Background Thread ---
def listen_to_tcp_socket():
    TCP_IP = "127.0.0.1"
    TCP_PORT = 9999
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((TCP_IP, TCP_PORT))
    server_socket.listen(10)
    print("🚀 Listening for network traffic on port 9999...")
    
    while True:
        try:
            client_socket, addr = server_socket.accept()
            data = client_socket.recv(4096)
            if data:
                data_queue.put(data.decode('utf-8').strip())
            client_socket.close()
        except Exception:
            pass

# --- 2. In-Memory Batched Aggregator Worker ---
def aggregated_worker():
    print("⚙️ In-Memory Aggregator Worker Started...")
    
    # Track the last time we flushed data to the database
    last_flush_time = time.time()
    # Memory cache structured as: {(stream_id, window_time): fan_count}
    memory_cache = {}

    while True:
        try:
            # Check the queue for new items without blocking indefinitely (timeout=1)
            element = data_queue.get(timeout=1)
            if element and element.strip() != "":
                try:
                    data = json.loads(element)
                    stream_id = data.get("stream_id")
                    
                    if stream_id:
                        # Round down to the nearest 10-second block milestone
                        now = datetime.datetime.now()
                        rounded_seconds = (now.second // 10) * 10
                        window_time = now.replace(second=rounded_seconds, microsecond=0)
                        
                        cache_key = (stream_id, window_time)
                        
                        # Increment the counter directly inside Python memory!
                        memory_cache[cache_key] = memory_cache.get(cache_key, 0) + 1
                except Exception as parse_err:
                    print(f"❌ JSON Parsing Error: {parse_err}")
        except queue.Empty:
            # No data arrived in the last second, which is fine
            pass

        # --- Check if 10 seconds have elapsed to flush to the database ---
        current_time = time.time()
        if current_time - last_flush_time >= 10.0:
            if memory_cache:
                try:
                    # Establish a single database connection for the entire batch flush
                    conn = psycopg2.connect(
                        host="localhost", database="weverse_analytics",
                        user="divyaavanigadda", password=""
                    )
                    cursor = conn.cursor()
                    
                    print(f"⏰ [10s MARK] Flushing {len(memory_cache)} aggregated stream windows to Postgres...")
                    
                    for (stream_id, window_time), total_fans in memory_cache.items():
                        query = """
                            INSERT INTO live_stream_metrics (stream_id, unique_fans, calculated_at) 
                            VALUES (%s, %s, %s)
                            ON CONFLICT ON CONSTRAINT unique_stream_window 
                            DO UPDATE SET unique_fans = live_stream_metrics.unique_fans + EXCLUDED.unique_fans;
                        """
                        cursor.execute(query, (stream_id, total_fans, window_time))
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    print("💾 [FLUSH COMPLETE] Database transaction committed successfully.")
                    
                    # Clear our memory cache for the next 10-second block
                    memory_cache.clear()
                    
                except Exception as db_err:
                    print(f"❌ Database batch insert error: {db_err}")
            
            # Reset our flush timer clock
            last_flush_time = current_time

# --- 3. Execution Pipeline Entrypoint ---
def run_streaming_pipeline():
    print("🚀 Local Scale Streaming Pipeline Initializing...")

    # Boot up our concurrent threads
    threading.Thread(target=listen_to_tcp_socket, daemon=True).start()
    threading.Thread(target=aggregated_worker, daemon=True).start()

    # Keep the main process alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down streaming pipeline smoothly.")

if __name__ == "__main__":
    run_streaming_pipeline()