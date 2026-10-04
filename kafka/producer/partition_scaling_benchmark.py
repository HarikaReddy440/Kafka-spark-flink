import argparse
import json
import statistics
import time

from kafka import KafkaProducer


BROKER = "localhost:9092"

TOTAL_MESSAGES = 2000
TARGET_RATE = 100


def create_transaction(number, topic_name):
    return {
        "transaction_id": f"PART-SCALE-{topic_name}-{number:04d}",
        "card_id": f"CARD-{number % 100}",
        "amount": round(100 + (number % 1000) * 1.25, 2),
        "merchant_id": f"M-{number % 100}",
        "timestamp": time.time(),
        "device_type": ["mobile", "web", "pos"][number % 3],
        "country": ["IN", "US", "UK", "AE"][number % 4],
        "is_fraud": 1 if number % 17 == 0 else 0,
    }


def calculate_percentile(values, percentile):
    if not values:
        return None

    sorted_values = sorted(values)

    index = int(percentile * len(sorted_values)) - 1
    index = max(0, min(index, len(sorted_values) - 1))

    return sorted_values[index]


def main():

    parser = argparse.ArgumentParser(
        description="Kafka partition scaling benchmark"
    )

    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic to benchmark"
    )

    args = parser.parse_args()

    topic = args.topic

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        acks="all",
        retries=0,
        linger_ms=0,
        batch_size=16384,
        request_timeout_ms=10000,
        delivery_timeout_ms=15000,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print("=" * 70)
    print("KAFKA PARTITION SCALING BENCHMARK")
    print("=" * 70)
    print(f"Broker             : {BROKER}")
    print(f"Topic              : {topic}")
    print(f"Total messages     : {TOTAL_MESSAGES}")
    print(f"Target rate        : {TARGET_RATE} tx/sec")
    print(f"ACKS               : all")
    print(f"Retries            : 0")
    print("=" * 70)
    print()

    latencies = []

    successful = 0
    failed = 0

    partition_counts = {}

    experiment_start = time.perf_counter()

    for i in range(1, TOTAL_MESSAGES + 1):

        target_send_time = experiment_start + ((i - 1) / TARGET_RATE)

        sleep_time = target_send_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)

        transaction = create_transaction(i, topic)

        key = transaction["card_id"]

        send_start = time.perf_counter()

        try:

            future = producer.send(
                topic,
                key=key,
                value=transaction
            )

            metadata = future.get(timeout=10)

            latency = (time.perf_counter() - send_start) * 1000

            latencies.append(latency)

            successful += 1

            partition = metadata.partition

            partition_counts[partition] = (
                partition_counts.get(partition, 0) + 1
            )

        except Exception as error:

            failed += 1

            print(
                f"Send failed for message {i}: "
                f"{type(error).__name__}: {error}"
            )

    producer.flush()

    experiment_end = time.perf_counter()

    producer.close()

    total_time = experiment_end - experiment_start

    print()
    print("=" * 70)
    print("EXPERIMENT RESULTS")
    print("=" * 70)

    print(f"Topic              : {topic}")
    print(f"Messages attempted : {TOTAL_MESSAGES}")
    print(f"Successful         : {successful}")
    print(f"Failed             : {failed}")

    producer_rate = (
        TOTAL_MESSAGES / total_time
        if total_time > 0
        else 0
    )

    print(f"Producer rate      : {producer_rate:.2f} tx/sec")

    if latencies:

        average_latency = statistics.mean(latencies)

        median_latency = statistics.median(latencies)

        p95_latency = calculate_percentile(
            latencies,
            0.95
        )

        p99_latency = calculate_percentile(
            latencies,
            0.99
        )

        print(
            f"Average latency    : "
            f"{average_latency:.3f} ms"
        )

        print(
            f"Median latency     : "
            f"{median_latency:.3f} ms"
        )

        print(
            f"P95 latency       : "
            f"{p95_latency:.3f} ms"
        )

        print(
            f"P99 latency       : "
            f"{p99_latency:.3f} ms"
        )

    else:

        print("Average latency    : N/A")
        print("Median latency     : N/A")
        print("P95 latency        : N/A")
        print("P99 latency        : N/A")

    print()
    print("PARTITION DISTRIBUTION")
    print("-" * 70)

    for partition in sorted(partition_counts):

        count = partition_counts[partition]

        percentage = (
            count / successful * 100
            if successful > 0
            else 0
        )

        print(
            f"Partition {partition}: "
            f"{count} messages "
            f"({percentage:.2f}%)"
        )

    print()

    print(f"Messages lost      : {failed}")

    if failed == 0 and successful == TOTAL_MESSAGES:
        print("Result             : SUCCESS")
    else:
        print("Result             : PARTIAL / FAILED")

    print("=" * 70)


if __name__ == "__main__":
    main()
