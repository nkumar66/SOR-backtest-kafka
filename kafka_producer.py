import time
import json
import pandas as pd
from kafka import KafkaProducer


df = pd.read_csv(
    "data/l1_day.csv",
    parse_dates=["ts_event"],
)

# Debugging
print("Data timestamps span:", df.ts_event.min(), "to", df.ts_event.max())

#ADJUST THIS if necessary
start_time = pd.Timestamp("2024-08-01T13:36:32Z")
end_time   = pd.Timestamp("2024-08-01T13:45:14Z")
window = df[(df.ts_event >= start_time) & (df.ts_event <= end_time)].copy()

#Debugging
print(f"Filtering between {start_time} and {end_time} yields {len(window)} rows")

if window.empty:
    print("Still no data? Check the printed span above.")
    exit(0)


window.sort_values("ts_event", inplace=True)
window.reset_index(drop=True, inplace=True)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


prev_ts = None
for _, row in window.iterrows():
    if prev_ts is not None:
        time.sleep((row.ts_event - prev_ts).total_seconds())
    prev_ts = row.ts_event

    message = {
        "publisher_id": row["publisher_id"],
        "ask_px_00":    row["ask_px_00"],
        "ask_sz_00":    row["ask_sz_00"],
        "ts_event":     row["ts_event"].isoformat(),
    }

    producer.send("mock_l1_stream", value=message)
    print(f"Sent at {message['ts_event']}: {message['ask_px_00']} @ {message['ask_sz_00']}")

# 6. Clean up
producer.flush()
producer.close()
