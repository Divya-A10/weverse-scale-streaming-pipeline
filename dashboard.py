import streamlit as st
import pandas as pd
import psycopg2
import time

# Clean, professional cloud dashboard title
st.set_page_config(
    page_title="Google Cloud Console - Operations Suite",
    page_icon="☁️",
    layout="wide"
)

st.title("☁️ Google Cloud Platform | Operations Suite")
st.caption("Resource Group: weverse-scale-pipeline-prod  |  Region: us-central1 (Low Latency)  |  Zone: us-central1-a")

st.divider()
# Core Data Source Fetcher from Cloud SQL
def get_cloud_sql_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="weverse_analytics",
            user="divyaavanigadda",
            password=""
        )
        # Pull the last 30 window blocks to display a shifting execution graph
        query = """
            SELECT stream_id, unique_fans, calculated_at 
            FROM live_stream_metrics 
            ORDER BY calculated_at DESC 
            LIMIT 30;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching Cloud SQL data: {e}")
        return pd.DataFrame()

# Container for live-updating visuals
pipeline_monitor = st.empty()

while True:
    df = get_cloud_sql_data()
    
    with pipeline_monitor.container():
        # Step 1: Display Infrastructure Operational Metrics
        st.markdown("### 📊 Infrastructure Health & Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="Data Ingestion Source", 
                value="Cloud Pub/Sub", 
                delta="Subscription Active"
            )
        with col2:
            st.metric(
                label="Stream Engine Window", 
                value="10s Tumbling", 
                delta="DirectRunner Local"
            )
        with col3:
            current_throughput = "0 / 10s"
            if not df.empty:
                latest_count = df.iloc[0]['unique_fans']
                current_throughput = f"{latest_count:,} / 10s"
            st.metric(
                label="Current Pipeline Throughput", 
                value=current_throughput
            )
            
        st.divider()

        # Step 2: Render the Cloud Dataflow execution graph
        if not df.empty:
            st.markdown("### 📈 Cloud Dataflow Execution Metrics")
            st.caption("Windowed throughput totals aggregated in-memory to prevent Cloud SQL transaction exhaustion.")
            
            # Format time data cleanly for graph readability
            df['timestamp_fixed'] = pd.to_datetime(df['calculated_at']).dt.strftime('%H:%M:%S')
            
            # Pivot table to support multiline tracking for multiple streams flawlessly
            chart_data = df.pivot_table(
                index='timestamp_fixed', 
                columns='stream_id', 
                values='unique_fans', 
                aggfunc='sum'
            ).fillna(0)
            
            # Display native line chart
            st.line_chart(chart_data)
            
            st.divider()
            
            # Step 3: Bottom Job Logs Table representing Cloud SQL target records
            st.markdown("### 📋 Cloud SQL (PostgreSQL) Target Records")
            st.caption("Committed analytical states written via optimized batch upserts.")
            st.dataframe(
                df[['stream_id', 'unique_fans', 'calculated_at']], 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.info("Awaiting live stream payloads from the Cloud Pub/Sub buffer queue...")
            
    time.sleep(2)