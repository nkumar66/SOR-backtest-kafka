## Overview

This repo implements a real-time Smart Order Router (SOR) backtest using:

1. **Kafka** to stream Level-1 snapshots from `l1_day.csv`  
2. A **Cont & Kukanov**-based allocator across venues  
3. A simple **parameter sweep** for λ_over, λ_under, θ_queue  
4. Benchmarks vs Best-Ask, TWAP, and VWAP  

Final output prints a JSON with optimized results, baselines, and bps savings.

### 1. Prerequisites

- **Python 3.8+**  
- **Kafka 3.x** (bundled with Zookeeper)  
- `pip install -r requirements.txt`  
  ```text
  pandas
  kafka-python
  numpy

# In one terminal (or powershell)
cd C:\kafka

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# In a second terminal (or powershell)
cd C:\kafka

.\bin\windows\kafka-server-start.bat .\config\server.properties

# Create the Topic (in a third terminal)
cd C:\kafka

.\bin\windows\kafka-topics.bat --create --topic mock_l1_stream `
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Run the Simulator, in a venv
python kafka_producer.py

This will read from the csv and stream the snapshots.

# Finally, we can run the backtest
python backtest.py

The output will look something like this:

{
  "best_parameters": {"lambda_over": 0.4, "lambda_under": 0.6, "theta_queue": 0.3},
  "optimized":   {"total_cash": ..., "avg_fill_px": ...},
  "baselines":   { "best_ask": {...}, "twap": {...}, "vwap": {...} },
  "savings_vs_baselines_bps": { ... }
}

