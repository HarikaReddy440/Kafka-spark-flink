from kafka import KafkaProducer
import json
import sys
import os


# ============================================================
# Import transaction generator
# ============================================================

generator_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "transaction_generator"
    )
)

sys.path.append(generator_path)

from transaction_generator import generate_transaction


# ============================================================
# Kafka configuration
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions"

NUMBER_OF_TRANSACTIONS = 20

# Transaction generator configuration
NUMBER_OF_CARDS = 10
FRAUD_RATIO = 0.05
AMOUNT_DISTRIBUTION = "uniform"


# ============================================================
# Create Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,

    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    key_serializer=lambda key: key.encode("utf-8"),

    # Reliability configuration
    acks="all",
    retries=5,
    enable_idempotence=True
)


# ============================================================
# Prepare transaction generator
# ============================================================

cards = [
    f"CARD{i:03d}"
    for i in range(1, NUMBER_OF_CARDS + 1)
]


# ============================================================
# Start producer
# ============================================================

print("Kafka Producer started...")
print(f"Topic                 : {TOPIC}")
print(f"Transactions          : {NUMBER_OF_TRANSACTIONS}")
print(f"Number of cards       : {NUMBER_OF_CARDS}")
print(f"Fraud ratio           : {FRAUD_RATIO:.2%}")
print(f"Amount distribution   : {AMOUNT_DISTRIBUTION}")
print(f"ACK configuration     : all")
print(f"Retries               : 5")
print(f"Idempotence           : enabled")
print()
print("Sending transactions...")
print()


successful = 0
failed = 0


# ============================================================
# Generate and send transactions
# ============================================================

try:

    for i in range(NUMBER_OF_TRANSACTIONS):

        # Generate a realistic transaction
        transaction = generate_transaction(
            cards=cards,
            fraud_ratio=FRAUD_RATIO,
            amount_distribution=AMOUNT_DISTRIBUTION,
        )

        # Use card_id as Kafka message key
        key = transaction["card_id"]

        try:

            # Send transaction to Kafka
            future = producer.send(
                TOPIC,
                key=key,
                value=transaction
            )

            # Wait for Kafka acknowledgement
            metadata = future.get(timeout=10)

            successful += 1

            print(
                f"[{i + 1}/{NUMBER_OF_TRANSACTIONS}] "
                f"Sent: {transaction['transaction_id']} | "
                f"Card: {transaction['card_id']} | "
                f"Amount: {transaction['amount']} | "
                f"Partition: {metadata.partition} | "
                f"Offset: {metadata.offset}"
            )

        except Exception as e:

            failed += 1

            print(
                f"FAILED: {transaction['transaction_id']} | "
                f"Error: {e}"
            )


    # ========================================================
    # Flush remaining messages
    # ========================================================

    producer.flush()


finally:

    # Close producer
    producer.close()


# ============================================================
# Final results
# ============================================================

print()
print("----------------------------------------")
print("Producer finished")
print("----------------------------------------")
print(f"Successful messages : {successful}")
print(f"Failed messages     : {failed}")
print(f"Total messages      : {NUMBER_OF_TRANSACTIONS}")
print("----------------------------------------")
