import psycopg2

try:
    conn = psycopg2.connect(host="localhost", database="weverse_analytics", user="divyaavanigadda", password="")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO live_stream_metrics (stream_id, unique_fans) VALUES ('test_stream', 999);")
    conn.commit()
    print("✅ DATABASE CONNECTION SUCCESSFUL! Row inserted.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ DATABASE CONNECTION FAILED: {e}")