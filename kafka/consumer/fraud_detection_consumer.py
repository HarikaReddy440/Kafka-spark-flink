import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    group_id='fraud-detection-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("========================================")
print(" REAL-TIME FRAUD DETECTION CONSUMER")
print("========================================")
print("Topic       : transactions")
print("Consumer ID : fraud-detection-group")
print("\nWaiting for transactions...\n")


def detect_fraud(transaction):
    reasons = []

    amount = transaction.get("amount", 0)
    country = transaction.get("country", "IN")

    if amount > 8000:
        reasons.append("High transaction amount")

    if country != "IN":
        reasons.append("Foreign country")

    return reasons


for message in consumer:

    transaction = message.value

    transaction_id = transaction["transaction_id"]

    print("----------------------------------------")
    print(f"Transaction ID : {transaction_id}")
    print(f"Amount         : ₹{transaction['amount']}")
    print(f"Merchant       : {transaction['merchant_id']}")
    print(f"Device         : {transaction['device_type']}")
    print(f"Country        : {transaction['country']}")

    print("\nAnalyzing transaction...")

    fraud_reasons = detect_fraud(transaction)

    if fraud_reasons:
        print("\n🚨 FRAUD / SUSPICIOUS TRANSACTION DETECTED")
        print("Reasons:")

        for reason in fraud_reasons:
            print(f" - {reason}")

        fraud_status = "FRAUD"
    else:
        print("\n✅ Transaction appears normal")
        fraud_status = "NORMAL"

    print(f"\nFinal Status : {fraud_status}")

    consumer.commit()

    print(f"Offset {message.offset} COMMITTED.")
    print("----------------------------------------\n")
