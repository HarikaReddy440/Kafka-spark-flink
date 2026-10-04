import argparse
import json
import time
import statistics

from kafka import KafkaProducer


BROKER = "localhost:9092"
TOPIC = "acks_test"

TOTAL_MESSAGES = 2000
TARGET_RATE = 100


# --------------------------------------------------
# Argument parser
# --------------------------------------------------

parser = argparse.ArgumentParser()

parser.add_argument(
    "--acks",
    required=True,
    choices=["0", "1", "all"]
)

args = parser.parse_args()

# Convert "0" and "1" into integers.
# Keep "all" as a string.
if args.acks in ["0", "1"]:
    args.acks = int(args.acks)


# --------------------------------------------------
# Create Kafka Producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    acks=args.acks,
    retries=0,
    linger_ms=0,
    batch_size=16384,
    request_timeout_ms=5000,
    delivery_timeout_ms=7000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


print("=" * 60)
print("KAFKA ACKS EXPERIMENT")
print("=" * 60)
print(f"Broker          : {BROKER}")
print(f"Topic           : {TOPIC}")
print(f"ACKS            : {args.acks}")
print(f"Total messages  : {TOTAL_MESSAGES}")
print(f"Target rate     : {TARGET_RATE} tx/sec")
print("=" * 60)


# --------------------------------------------------
# Experiment
# --------------------------------------------------

latencies = []

successful = 0
failed = 0

experiment_start = time.perf_counter()

next_send_time = experiment_start


for i in range(TOTAL_MESSAGES):

    # Maintain approximately TARGET_RATE messages/sec
    next_send_time = experiment_start + (i / TARGET_RATE)

    sleep_time = next_send_time - time.perf_counter()

    if sleep_time > 0:
        time.sleep(sleep_time)

    message = {
        "transaction_id": f"ACK-{args.acks}-{i}",
        "amount": 100 + (i % 1000),
        "merchant_id": f"M-{i % 100}",
        "timestamp": time.time(),
        "device_type": "mobile",
        "country": "IN",
        "is_fraud": 0
    }

    start = time.perf_counter()

    try:

        future = producer.send(
            TOPIC,
            value=message
        )

        if args.acks != 0:

            metadata = future.get(timeout=5)

            latency = (
                time.perf_counter() - start
            ) * 1000

            latencies.append(latency)

            successful += 1

        else:

            # For acks=0, Kafka does not wait for acknowledgement.
            latency = (
                time.perf_counter() - start
            ) * 1000

            latencies.append(latency)

            successful += 1

    except Exception as e:

        failed += 1

        print(
            f"Send failed for message {i}: "
            f"{type(e).__name__}: {e}"
        )


# --------------------------------------------------
# Flush and close
# --------------------------------------------------

producer.flush()
producer.close()

experiment_end = time.perf_counter()

total_time = experiment_end - experiment_start


# --------------------------------------------------
# Results
# --------------------------------------------------

print()
print("=" * 60)
print("EXPERIMENT RESULTS")
print("=" * 60)

print(f"ACKS              : {args.acks}")
print(f"Messages attempted: {TOTAL_MESSAGES}")
print(f"Successful        : {successful}")
print(f"Failed            : {failed}")

if total_time > 0:
    producer_rate = TOTAL_MESSAGES / total_time
else:
    producer_rate = 0

print(f"Producer rate     : {producer_rate:.2f} tx/sec")


if latencies:

    average_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)

    sorted_latencies = sorted(latencies)

    p95_index = int(0.95 * len(sorted_latencies)) - 1
    p99_index = int(0.99 * len(sorted_latencies)) - 1

    p95_index = max(0, min(p95_index, len(sorted_latencies) - 1))
    p99_index = max(0, min(p99_index, len(sorted_latencies) - 1))

    p95_latency = sorted_latencies[p95_index]
    p99_latency = sorted_latencies[p99_index]

    print(f"Average latency   : {average_latency:.3f} ms")
    print(f"Median latency    : {median_latency:.3f} ms")
    print(f"P95 latency       : {p95_latency:.3f} ms")
    print(f"P99 latency       : {p99_latency:.3f} ms")

else:

    print("Average latency   : N/A")
    print("Median latency    : N/A")
    print("P95 latency       : N/A")
    print("P99 latency       : N/A")


print(f"Messages lost     : {failed}")

if failed == 0:
    print("Result            : SUCCESS")
else:
    print("Result            : PARTIAL / FAILED")

print("=" * 60)
