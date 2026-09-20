# Kafka Learning Playground

A modern Apache Kafka environment running in **KRaft mode** (no ZooKeeper needed) paired with **Kafka UI** for visual inspection of topics, partitions, consumer groups, and messages.

---

## 🚀 Quick Start

### 1. Start Kafka and Kafka UI
```bash
docker compose up -d
```

### 2. Access Kafka UI
Open [http://localhost:8080](http://localhost:8080) in your browser.
Here you can:
- View brokers and cluster health
- Create and configure topics
- Produce and browse messages in real time
- Inspect consumer groups and monitor consumer lag

### 3. Stop Kafka
```bash
# Stop containers (preserves topic data in docker volume)
docker compose down

# Stop containers and reset all data/topics
docker compose down -v
```

---

## 🔌 Connection Endpoints

| Client Location | Bootstrap Server Address | Notes |
| :--- | :--- | :--- |
| **Host Machine** (your Mac: Python, Node.js, Java, Go) | `localhost:9092` | Connect directly from local apps |
| **Docker Containers** (apps running in the Docker network) | `kafka:29092` | Connect via internal Docker network |

---

## 🐍 Python Producer & Consumer Scripts

Two ready-to-run Python scripts are provided for easy message experimentation without dealing with terminal commands:

### Setup Virtual Environment
```bash
# Create and activate virtualenv
python3 -m venv .venv
source .venv/bin/activate

# Install requirements (confluent-kafka)
pip install -r requirements.txt
```

### 1. Producer (`producer.py`)

- **Interactive Mode**: Type messages, send sample orders, or specify `key:value`.
  ```bash
  python3 producer.py
  ```
  *Prompt shortcuts*:
  - Type `sample` to send a realistic matcha cafe order JSON.
  - Type `order-123:Hello` to send a message with a custom key.

- **Automated Stream Mode**: Generates a continuous stream of sample events (great for watching in Kafka UI!):
  ```bash
  python3 producer.py --auto --count 10 --delay 0.5
  ```

- **Single Message Mode**:
  ```bash
  python3 producer.py --message "Hello Kafka" --key "greeting"
  ```

### 2. Consumer (`consumer.py`)

- **Listen for New Messages**:
  ```bash
  python3 consumer.py
  ```

- **Read Entire Topic from the Beginning**:
  ```bash
  python3 consumer.py --from-beginning
  ```

- **Custom Topic or Consumer Group**:
  ```bash
  python3 consumer.py --topic my-topic --group my-custom-group
  ```

---

## 🛠 Useful Kafka CLI Commands

You can run Kafka's native CLI utilities directly using `docker exec`:

### Create a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo-topic \
  --partitions 3 \
  --replication-factor 1
```

### List Topics
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Describe a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic demo-topic
```

### Produce Messages (Interactive CLI)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic
```
*(Type messages and press Enter. Press `Ctrl + C` to exit)*

### Consume Messages from the Beginning
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --from-beginning
```

### View Consumer Groups
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Inspect Consumer Group Offsets & Lag
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group <group-id>
```

---

## 💡 Key Kafka Concepts for Beginners

- **Broker**: A Kafka server instance that stores and serves records.
- **Topic**: A logical stream of records (like a table in a database, but append-only).
- **Partition**: Topics are divided into partitions for parallelism and ordering. Ordering is guaranteed **per partition**, not across the entire topic.
- **Offset**: A unique sequential ID assigned to every message within a partition.
- **Producer**: An application that publishes messages to one or more topics.
- **Consumer Group**: A set of consumers cooperating to consume data from topics. Partitions are divided evenly across members of the group.
- **KRaft Mode**: Kafka Raft metadata mode. Modern Kafka manages metadata internally using an event-driven consensus protocol, deprecating ZooKeeper completely.
