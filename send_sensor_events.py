import pandas as pd
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

# ---- FILL THESE IN LOCALLY, NEVER SHARE ----
CONNECTION_STR = "connection string"  # Endpoint=sb://...;SharedAccessKeyName=SendPolicy;SharedAccessKey=...
EVENTHUB_NAME = "sensor-events"
CSV_PATH = r"file path"  # update to your actual path
# ---------------------------------------------

DELAY_SECONDS = 0.5   # delay between sending each event, to simulate real-time arrival
MAX_EVENTS = 50        # limit how many rows to send in this demo burst (keeps the session short/cheap)

def main():
    df = pd.read_csv(CSV_PATH, parse_dates=['timestamp'])
    df = df.sort_values('timestamp').head(MAX_EVENTS)  # just send the first N rows for a quick demo

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )

    print(f"Sending {len(df)} events to Event Hub '{EVENTHUB_NAME}'...")

    with producer:
        for _, row in df.iterrows():
            event_payload = {
                "machine_id": row['machine_id'],
                "timestamp": row['timestamp'].isoformat(),
                "temperature_c": row['temperature_c'],
                "vibration_mm_s": row['vibration_mm_s'],
                "pressure_psi": row['pressure_psi']
            }

            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(event_payload)))
            producer.send_batch(event_data_batch)

            print(f"Sent: {event_payload['machine_id']} @ {event_payload['timestamp']}")
            time.sleep(DELAY_SECONDS)

    print("Done sending events.")

if __name__ == "__main__":
    main()
