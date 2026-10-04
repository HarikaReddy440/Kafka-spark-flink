from kafka import KafkaConsumer
import json
import time


# --------------------------------------------------
# Kafka Configuration
# --------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions"
GROUP_ID = "fraud-detector-group"


# --------------------------------------------------
# Create Kafka Consumer
# --------------------------------------------------

consumer = KafkaConsumer(
    TOPIC,

    bootstrap_servers=BOOTSTRAP_SERVERS,

    # Consumer group
    group_id=GROUP_ID,

    # If this group has no committed offset,
    # start from the beginning of the topic
    auto_offset_reset="earliest",

    # We will commit offsets manually
    enable_auto_commit=False,

    # Convert JSON bytes → Python dictionary
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    # Convert key bytes → string
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    )
)


# --------------------------------------------------
# Consumer Statistics
# --------------------------------------------------

processed_count = 0
unique_transactions = set()
duplicate_count = 0


print("Kafka Consumer started...")
print(f"Topic       : {TOPIC}")
print(f"Consumer ID : {GROUP_ID}")
print("\nWaiting for transactions...\n")


try:

    for message in consumer:

        transaction = message.value

        transaction_id = transaction.get("transaction_id")

        processed_count += 1

        # --------------------------------------------------
        # Duplicate detection
        # --------------------------------------------------

        if transaction_id in unique_transactions:

            duplicate_count += 1

            print(
                f"DUPLICATE: {transaction_id}"
            )

        else:

            unique_transactions.add(transaction_id)

            print(
                f"[{processed_count}] "
                f"Received: {transaction_id} | "
                f"Card: {transaction.get('card_id')} | "
                f"Amount: {transaction.get('amount')} | "
                f"Partition: {message.partition} | "
                f"Offset: {message.offset}"
            )

        # --------------------------------------------------
        # Simulated transaction processing
        # --------------------------------------------------

        time.sleep(0.05)

        # --------------------------------------------------
        # Manual offset commit
        # --------------------------------------------------

        consumer.commit()


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    consumer.close()

    print("\n----------------------------------------")
    print("Consumer Summary")
    print("----------------------------------------")
    print(f"Messages processed : {processed_count}")
    print(f"Unique messages    : {len(unique_transactions)}")
    print(f"Duplicates         : {duplicate_count}")
    print("----------------------------------------")


