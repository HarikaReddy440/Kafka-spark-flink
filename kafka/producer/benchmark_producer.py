import argparse
import json
import statistics
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError


BROKER = "localhost:9092"


def create_transaction(transaction_number):
    return {
        "transaction_id": f"BENCH-{transaction_number}-{uuid.uuid4().hex[:8]}",
        "card_id": f"CARD-{transaction_number % 1000:04d}",
        "amount": round(100 + (transaction_number % 10000) * 0.75, 2),
        "merchant_id": f"M{(transaction_number % 100) + 100}",
        "timestamp": time.time(),
        "device_type": ["mobile", "web", "pos"][transaction_number % 3],
        "country": ["IN", "US", "UK", "AE"][transaction_number % 4],
        "is_fraud": False,
    }


def percentile(values, percentage):
    if not values:
        return 0.0

    sorted_values = sorted(values)

    index = int((percentage / 100) * len(sorted_values))

    if index >= len(sorted_values):
        index = len(sorted_values) - 1

    return sorted_values[index]


def main():
    parser = argparse.ArgumentParser(
        description="Kafka throughput, latency and reliability benchmark"
    )

    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic",
    )

    parser.add_argument(
        "--rate",
        type=int,
        required=True,
        help="Target transactions per second",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=2000,
        help="Number of transactions to send",
    )

    args = parser.parse_args()

    if args.rate <= 0:
        raise ValueError("Rate must be greater than 0")

    if args.count <= 0:
        raise ValueError("Count must be greater than 0")

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )

    print("=" * 75)
    print("KAFKA THROUGHPUT, LATENCY & RELIABILITY BENCHMARK")
    print("=" * 75)
    print(f"Broker              : {BROKER}")
    print(f"Topic               : {args.topic}")
    print(f"Target rate         : {args.rate} tx/sec")
    print(f"Message count       : {args.count}")
    print(f"ACK mode            : all")
    print(f"Retries             : 5")
    print(f"Idempotence         : enabled")
    print("=" * 75)
    print()

    interval = 1.0 / args.rate

    successful = 0
    failed = 0
    latencies = []

    start_time = time.perf_counter()
    next_send_time = start_time

    for i in range(1, args.count + 1):

        now = time.perf_counter()

        if now < next_send_time:
            time.sleep(next_send_time - now)

        transaction = create_transaction(i)

        send_start = time.perf_counter()

        try:
            future = producer.send(
                args.topic,
                key=transaction["card_id"],
                value=transaction,
            )

            metadata = future.get(timeout=10)

            send_end = time.perf_counter()

            latency_ms = (send_end - send_start) * 1000

            latencies.append(latency_ms)

            successful += 1

            print(
                f"SUCCESS | "
                f"Transaction={transaction['transaction_id']} | "
                f"Partition={metadata.partition} | "
                f"Offset={metadata.offset} | "
                f"Latency={latency_ms:.3f} ms"
            )

        except KafkaError as error:

            failed += 1

            print(
                f"FAILED  | "
                f"Transaction={transaction['transaction_id']} | "
                f"Error={type(error).__name__}: {error}"
            )

        except Exception as error:

            failed += 1

            print(
                f"FAILED  | "
                f"Transaction={transaction['transaction_id']} | "
                f"Error={type(error).__name__}: {error}"
            )

        next_send_time += interval

    print()
    print("Flushing producer...")

    try:
        producer.flush()
    except Exception as error:
        print(
            f"Flush error: {type(error).__name__}: {error}"
        )

    end_time = time.perf_counter()

    elapsed = end_time - start_time

    producer.close()

    actual_rate = successful / elapsed if elapsed > 0 else 0

    average_latency = (
        statistics.mean(latencies)
        if latencies
        else 0
    )

    median_latency = (
        statistics.median(latencies)
        if latencies
        else 0
    )

    p95_latency = percentile(latencies, 95)

    p99_latency = percentile(latencies, 99)

    print()
    print("=" * 75)
    print("BENCHMARK RESULTS")
    print("=" * 75)
    print(f"Target rate         : {args.rate:.2f} tx/sec")
    print(f"Messages attempted  : {args.count}")
    print(f"Successful          : {successful}")
    print(f"Failed              : {failed}")
    print(f"Elapsed time        : {elapsed:.3f} sec")
    print(f"Actual throughput   : {actual_rate:.2f} tx/sec")
    print(f"Average latency     : {average_latency:.3f} ms")
    print(f"Median latency      : {median_latency:.3f} ms")
    print(f"P95 latency         : {p95_latency:.3f} ms")
    print(f"P99 latency         : {p99_latency:.3f} ms")
    print(f"Producer failures   : {failed}")
    print("=" * 75)


if __name__ == "__main__":
    main()
