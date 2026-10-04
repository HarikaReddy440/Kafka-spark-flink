# Real-Time Financial Fraud Detection using Apache Kafka

## Member 1 – Kafka / Event Ingestion Layer

This project implements and evaluates Apache Kafka as the event ingestion and streaming layer for a real-time financial fraud detection system.

### Research Objective

How can financial transactions be reliably ingested, distributed, and consumed in a scalable real-time fraud detection system?

---

## 1. System Architecture

The Member 1 prototype follows this architecture:

Transaction Generator
        |
        v
Kafka Producer
        |
        v
Kafka Topic: transactions
        |
        v
Kafka Consumer
        |
        v
Real-Time Transaction / Fraud Processing


The transaction generator creates synthetic financial transaction events. The producer publishes these events to Apache Kafka. Kafka stores and distributes the events through the `transactions` topic. Consumers retrieve and process the transaction events.

---

## 2. Technologies Used

- Apache Kafka 4.3.1
- KRaft mode
- Python 3
- kafka-python 3.0.11
- Java 17
- Ubuntu Linux
- CSV-based experiment result collection

---

## 3. Project Structure

```text
fraud-kafka/
│
├── kafka/
│   ├── config/
│   │   └── config.py
│   │
│   ├── producer/
│   │   ├── producer.py
│   │   ├── benchmark_producer.py
│   │   ├── partition_test_producer.py
│   │   ├── partition_scaling_benchmark.py
│   │   ├── ordering_key_producer.py
│   │   ├── parallel_producer.py
│   │   ├── producer_failure_test.py
│   │   ├── retry_behavior_producer.py
│   │   ├── retry_clean_producer.py
│   │   ├── retry_failure_producer.py
│   │   ├── idempotence_test_producer.py
│   │   ├── idempotence_failure_test.py
│   │   ├── delivery_semantics_producer.py
│   │   ├── batching_compression_benchmark.py
│   │   └── ACK test scripts
│   │
│   ├── consumer/
│   │   ├── consumer.py
│   │   ├── benchmark_consumer.py
│   │   ├── failure_test_consumer.py
│   │   ├── recovery_consumer.py
│   │   ├── parallel_consumer.py
│   │   ├── parallel_consumer_live.py
│   │   ├── consumer_group_scaling.py
│   │   ├── fraud_detection_consumer.py
│   │   ├── at_least_once_consumer.py
│   │   ├── at_most_once_consumer.py
│   │   └── acks_test_consumer.py
│   │
│   ├── transaction_generator/
│   │   └── transaction_generator.py
│   │
│   └── tests/
│       └── test_kafka_connection.py
│
├── member1_experiment_results.csv
├── member1_clean_experiment_results.csv
└── README.md
