# Link to the video recording:

https://www.loom.com/share/e8bc5595a886479b80d35432c3330636?sid=4342e9e2-e416-4709-96ca-4e57fc3da3c1


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

#Steps to run this on AWS EC2 t2.micro instance:

#install all packages, as well as necessary tools from requirements.txt

sudo yum install -y java-1.8.0-openjdk-devel wget tmux

pip install -r requirements.txt

#Download and extract kafka:
cd ~

wget "https://archives.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz"

tar -xzf kafka_2.13-3.5.1.tgz

mv kafka_2.13-3.5.1 kafka

cd kafka

#Set Kafka heap for low memory since we're on t2.micro instance:
export KAFKA_HEAP_OPTS="-Xms128M" -Xmx256M"

I used tmux to have multiple terminals in one window, but you don't have to, but keep in mind it would require 4-5 different terminals open

#Startup Zookeeper:

bin/zookeeper-server-start.sh config/zookeeper.properties

#Startup Kafka once Zookeeper is running(in a new terminal):

bin/kafka-server-start.sh config/server.properties
(This is all from the kafka_2.13-3.5.1 folder)

#Now create the topic(in a new terminal):

cd ~/kafka
bin/kafka-topics.sh --create --topic mock_l1_stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

#In a new terminal, clone my github page and download the csv file (not provided in my github repo as per instructions) and start up the kafka-producer, and in a new terminal, startup the backtest after the producer is running. 

Everything was able to run locally as well as on the AWS EC2 instance, feel free to connect with me on LinkedIn if anything doesn't work as intended. 

![image](https://github.com/user-attachments/assets/57533df8-89f8-4a50-9cbd-7b054362a6b8)

