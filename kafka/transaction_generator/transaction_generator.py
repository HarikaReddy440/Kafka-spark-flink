import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone


DEFAULT_DEVICES = ["mobile", "web", "tablet"]

DEFAULT_COUNTRIES = ["IN", "US", "UK", "SG", "AE"]

DEFAULT_MERCHANTS = [
    "M101",
    "M102",
    "M103",
    "M104",
    "M105",
]


def build_cards(number_of_cards):
    return [
        f"CARD{i:03d}"
        for i in range(1, number_of_cards + 1)
    ]


def generate_amount(distribution):
    if distribution == "uniform":
        return round(random.uniform(10, 10000), 2)

    if distribution == "normal":
        amount = random.gauss(3000, 1500)
        amount = max(10, min(10000, amount))
        return round(amount, 2)

    if distribution == "small":
        return round(random.uniform(10, 1000), 2)

    if distribution == "large":
        return round(random.uniform(1000, 10000), 2)

    raise ValueError(f"Unknown amount distribution: {distribution}")


def generate_transaction(
    cards,
    fraud_ratio,
    amount_distribution,
):
    transaction = {
        "transaction_id": "TX-" + str(uuid.uuid4())[:8],
        "card_id": random.choice(cards),
        "amount": generate_amount(amount_distribution),
        "merchant_id": random.choice(DEFAULT_MERCHANTS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": random.choice(DEFAULT_DEVICES),
        "country": random.choice(DEFAULT_COUNTRIES),
        "is_fraud": random.random() < fraud_ratio,
    }

    return transaction


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Configurable financial transaction generator"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of transactions to generate",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=1,
        help="Target transaction generation rate per second",
    )

    parser.add_argument(
        "--fraud-ratio",
        type=float,
        default=0.05,
        help="Fraud ratio between 0.0 and 1.0",
    )

    parser.add_argument(
        "--cards",
        type=int,
        default=10,
        help="Number of unique cards",
    )

    parser.add_argument(
        "--amount-distribution",
        choices=["uniform", "normal", "small", "large"],
        default="uniform",
        help="Amount distribution",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.count <= 0:
        raise ValueError("Count must be greater than 0")

    if args.rate <= 0:
        raise ValueError("Rate must be greater than 0")

    if not 0 <= args.fraud_ratio <= 1:
        raise ValueError("Fraud ratio must be between 0.0 and 1.0")

    if args.cards <= 0:
        raise ValueError("Number of cards must be greater than 0")

    cards = build_cards(args.cards)

    interval = 1 / args.rate

    print("=" * 70)
    print("CONFIGURABLE TRANSACTION GENERATOR")
    print("=" * 70)
    print(f"Transactions        : {args.count}")
    print(f"Target rate         : {args.rate} tx/sec")
    print(f"Fraud ratio         : {args.fraud_ratio:.2%}")
    print(f"Number of cards     : {args.cards}")
    print(f"Amount distribution : {args.amount_distribution}")
    print("=" * 70)
    print()

    next_send_time = time.time()

    for _ in range(args.count):
        now = time.time()

        if now < next_send_time:
            time.sleep(next_send_time - now)

        transaction = generate_transaction(
            cards=cards,
            fraud_ratio=args.fraud_ratio,
            amount_distribution=args.amount_distribution,
        )

        print(json.dumps(transaction))

        next_send_time += interval


if __name__ == "__main__":
    main()
