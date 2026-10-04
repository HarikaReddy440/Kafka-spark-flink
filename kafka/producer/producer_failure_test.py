import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


BROKER = "localhost:9092"
TOPIC = "producer_failure_test"

TOTAL_MESSAGES = 300
INTERVAL = 0.5


def create_transaction(number):
    return {
        "transaction_id": f"FAIL-{number:04d}",
        "card_id": f"CARD-{number % 10}",
        "amount": round(100 + number * 5.25, 2),
        "merchant_id": f"M{100 + (number % 10)}",
        "timestamp": time.time(),
        "device_type": ["mobile", "web", "pos"][number % 3],
        "country": ["IN", "US", "UK", "AE"][number % 4],
        "sequence": number
    }


def main():

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        request_timeout_ms=3000,
        delivery_timeout_ms=10000,
        enable_idempotence=True
    )

    successful = 0
    failed = 0

    print("=" * 70)
    print("KAFKA PRODUCER FAILURE / RETRY TEST")
    print("=" * 70)
    print(f"Broker           : {BROKER}")
    print(f"Topic            : {TOPIC}")
    print(f"Total messages   : {TOTAL_MESSAGES}")
    print(f"Message interval : {INTERVAL} seconds")
    print("ACK mode         : all")
    print("Retries          : 5")
    print("Idempotence      : enabled")
    print("=" * 70)
    print()

    for i in range(1, TOTAL_MESSAGES + 1):

        transaction = create_transaction(i)

        try:

            future = producer.send(
                TOPIC,
                key=transaction["card_id"],
                value=transaction
            )

            metadata = future.get(timeout=10)

            successful += 1

            print(
                f"SUCCESS | "
                f"Transaction={transaction['transaction_id']} | "
                f"Partition={metadata.partition} | "
                f"Offset={metadata.offset}"
            )

        except KafkaError as error:

            failed += 1

            print(
                f"FAILED  | "
                f"Transaction={transaction['transaction_id']} | "
                f"Error={error}"
            )

        time.sleep(INTERVAL)

    producer.flush()
    producer.close()

    print()
    print("=" * 70)
    print("PRODUCER FAILURE TEST RESULTS")
    print("=" * 70)
    print(f"Messages attempted : {TOTAL_MESSAGES}")
    print(f"Successful         : {successful}")
    print(f"Failed             : {failed}")
    print("=" * 70)


if __name__ == "__main__":
    main()
