import json
import os

BASE_DIR = "/Users/dharaniarumugam/PycharmProjects/Payment_risks/data/raw"

def load_json_lines(path):
    rows = []
    with open(path) as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def validate_dataset(base_dir):

    print("Loading dataset...")

    users = load_json_lines(os.path.join(base_dir, "users", "users_001.json"))
    merchants = load_json_lines(os.path.join(base_dir, "merchants", "merchants_001.json"))
    methods = load_json_lines(os.path.join(base_dir, "payment_methods", "payment_methods_001.json"))
    transactions = load_json_lines(os.path.join(base_dir, "transactions", "transactions_001.json"))
    refunds = load_json_lines(os.path.join(base_dir, "refunds", "refunds_001.json"))
    chargebacks = load_json_lines(os.path.join(base_dir, "chargebacks", "chargebacks_001.json"))

    print("Running validation checks...\n")

    user_ids = {u["user_id"] for u in users}
    merchant_ids = {m["merchant_id"] for m in merchants}
    method_ids = {m["payment_method_id"] for m in methods}
    txn_ids = {t["transaction_id"] for t in transactions}

    # 1️⃣ Check FK: transaction.user_id
    bad_users = [t for t in transactions if t["user_id"] not in user_ids]
    print("Invalid user references:", len(bad_users))

    # 2️⃣ Check FK: transaction.merchant_id
    bad_merchants = [t for t in transactions if t["merchant_id"] not in merchant_ids]
    print("Invalid merchant references:", len(bad_merchants))

    # 3️⃣ Check FK: transaction.payment_method_id
    bad_methods = [t for t in transactions if t["payment_method_id"] not in method_ids]
    print("Invalid payment method references:", len(bad_methods))

    # 4️⃣ Refund FK
    bad_refunds = [r for r in refunds if r["transaction_id"] not in txn_ids]
    print("Invalid refund transaction references:", len(bad_refunds))

    # 5️⃣ Chargeback FK
    bad_chargebacks = [c for c in chargebacks if c["transaction_id"] not in txn_ids]
    print("Invalid chargeback transaction references:", len(bad_chargebacks))

    # 6️⃣ Check refund only from completed transactions
    txn_status = {t["transaction_id"]: t["status"] for t in transactions}

    invalid_refunds = [
        r for r in refunds
        if txn_status.get(r["transaction_id"]) != "completed"
    ]

    print("Refunds from non-completed txns:", len(invalid_refunds))

    # 7️⃣ Check settled_at logic
    bad_settled = [
        t for t in transactions
        if t["status"] != "completed" and t["settled_at"] is not None
    ]

    print("Invalid settled_at values:", len(bad_settled))

    print("\nValidation complete.")

if __name__ == "__main__":
    validate_dataset(BASE_DIR)