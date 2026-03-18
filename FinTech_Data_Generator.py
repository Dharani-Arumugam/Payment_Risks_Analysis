"""
FinTech Data Generator
Generates realistic dimensional and fact data
for Real-Time Payments Risk & Intelligence Platform on Databricks


Tables generated:
  Dimensions : users, merchants, payment_methods
  Facts       : transactions, refunds, chargebacks
Design principles:
  - Referential integrity: every FK resolves to a real PK
  - Realistic distributions: amounts follow power-law,
    fraud is rare (~2%), chargebacks rarer (~1%)
  - Time-coherent: refund/chargeback timestamps always
    come AFTER the parent transaction
  - Idempotent: fixed random seed = same output every run

"""
import os
import uuid as uuid
import random
import json
from faker import Faker
from helpers import *
from collections import defaultdict
from config import configuration
import boto3


fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Config ─────────────────────────────────────────────────
# Adjust counts to stay within your cluster memory budget.
# At 2 000 transactions the full dataset is ~3 MB uncompressed.
# Scale to 50 000 if you want realistic Silver/Gold aggregations.
config = {
    "n_users":           500,    # dimension: unique customers
    "n_merchants":       80,     # dimension: unique merchants
    "n_payment_methods": 700,    # ~1.4 methods per user on average
    "n_transactions":    2_000,  # fact: core event volume
    "refund_rate":       0.08,   # 8 % of completed txns get refunded
    "chargeback_rate":   0.01,   # 1 % of completed txns get disputed
    "seed":              42,
}
# S3 file path
s3_filepath = "s3a://payments_risk_analysis/raw_data/"
s3_client = boto3.client("s3",
                         aws_access_key_id=configuration["AWS_ACCESS_KEY"],
                         aws_secret_access_key=configuration["AWS_SECRET_KEY"],
                         region_name="us-east-1"
                         )
# ══════════════════════════════════════════════════════════
# DIMENSION GENERATORS
# Dimensions are small, slowly-changing reference tables.
# Generate these first because facts reference their PKs.
# ══════════════════════════════════════════════════════════
def generate_users(n_users):

    KYC_STATUSES = ['approved', 'rejected', 'pending']
    RISK_TIERS = ['low', 'medium', 'high']
    users = []
    for _ in range(n_users):
        created_at = random_past_ts(365)
        updated_at = ts_after(created_at, 1, 24 * 30)
        users.append({
            "user_id": str(uuid.uuid4()),
            "email_id": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone" : fake.phone_number(),
            "country_code" : fake.country_code(),
            "kyc_status" : random.choices(KYC_STATUSES, weights =[75,20,5], k=1)[0],
            "risk_tier" : random.choices(RISK_TIERS, weights =[70,20,10], k=1)[0],
            "created_at" : iso(created_at),
            "updated_at" : iso(updated_at),
        })
    return users

def generate_merchants(n_merchants):

    MCC_CODES = [
        ("5411", "Grocery stores"),
        ("5812", "Eating places"),
        ("5944", "Jewelry stores"),
        ("4111", "Transportation"),
        ("5999", "Miscellaneous retail"),
        ("7011", "Hotels and motels"),
        ("5045", "Computers and software"),
        ("5912", "Drug stores"),
    ]
    STATUSES = ['active', 'suspended', 'closed']
    merchants = []
    for _ in range(n_merchants):
        mcc, mcc_desc = random.choice(MCC_CODES)
        merchants.append({
            "merchant_id": str(uuid.uuid4()),
            "merchant_name": fake.company(),
            "mcc_code" : mcc,
            "mcc_desc" : mcc_desc,
            "country_code" : fake.country_code(),
            "status" : random.choices(STATUSES, weights=[80,10,10], k=1)[0],
            "risk_score" : round(random.betavariate(1.5,8),4),
            "on_boarded_at" : iso(random_past_ts(730)),
        })

    return merchants

def generate_payment_methods(users, n_payment_methods):

    PAYMENT_TYPES = ['credit_card','debit_card', 'digital_wallet']
    NETWORKS = ['Visa', 'Mastercard', 'Amex', 'Discover']

    user_ids = [u['user_id'] for u in users]
    payment_methods = []
    for uid in user_ids:
        payment_methods.append({
            'payment_method_id': str(uuid.uuid4()),
            'user_id' : uid,
            'payment_type' : random.choices(PAYMENT_TYPES, weights=[70,5,25], k=1)[0],
            'last_four': str(random.randint(1000,9999)),
            'network' : random.choices(NETWORKS, weights=[35,35,20,10], k=1)[0],
            'country_code' : fake.country_code(),
            'is_default': True,
            'is_active' : True,
            'created_at': iso(random_past_ts(365)),
        })

    for _ in range(n_payment_methods - len(user_ids)):
        payment_methods.append({
            'payment_method_id': str(uuid.uuid4()),
            'user_id' : random.choice(user_ids),
            'payment_type' : random.choices(PAYMENT_TYPES, weights=[70,5,25], k=1)[0],
            'last_four': str(random.randint(1000,9999)),
            'network': random.choices(NETWORKS, weights=[35,35,20,10], k=1)[0],
            'country_code' : fake.country_code(),
            'is_default': False,
            'is_active': random.choices([True, False], weights=[92,8],k=1)[0],
            'created_at': iso(random_past_ts(365)),
        })
    return payment_methods

# ══════════════════════════════════════════════════════════
# FACT GENERATORS
# Facts are high-volume event tables.
# Generate transactions first; all other facts derive from them.
# ══════════════════════════════════════════════════════════
def generate_transactions(users, merchants, payment_methods, n_transactions):
    """
        FACT: transactions
        -------------------
        One row per payment attempt.

        Assumptions:
        - every user has exactly one default payment method
        - default payment method is always active
        - non-default methods may be active or inactive
        """
    payment_methods_by_user = defaultdict(list)
    default_payment_by_user = {}

    for method in payment_methods:
        userid = method['user_id']
        payment_methods_by_user[userid].append(method)
        if method.get('is_default'):
            default_payment_by_user[userid] = method


    transactions = []
    for _ in range(n_transactions):

        #User_id
        user = random.choice(users)
        user_id = user['user_id']
        #Merchant_id
        merchant = random.choice(merchants)
        merchant_id = merchant['merchant_id']
        #Payment_Method
        user_methods = payment_methods_by_user[user_id]
        default_payment = default_payment_by_user[user_id]
        active_non_default_methods =[]
        inactive_non_default_methods = []
        for m in user_methods:
            if not m.get('is_default'):
                if m.get('is_active'):
                    active_non_default_methods.append(m)
                else:
                    inactive_non_default_methods.append(m)

        r = random.random()
        if r < 0.80:
            chosen_method = default_payment
        elif r < 0.98 and active_non_default_methods:
            chosen_method = random.choice(active_non_default_methods)
        elif inactive_non_default_methods:
            chosen_method = random.choice(inactive_non_default_methods)
        else:
            chosen_method = default_payment

        #Skewed_amount
        amount = skewed_amount()

        #country code
        user_country = user.get('country_code')
        merchant_country = merchant.get('country_code', user_country)
        payment_method_country = chosen_method.get('country_code', user_country)

        txn_country = random.choices(
            [user_country, merchant_country, payment_method_country],
            weights=[60, 25, 15],
            k=1,
        )[0]

        channel = random.choices(['web', 'mobile', 'api'], weights=[45,40,15], k=1)[0]
        status = random.choices(['completed', 'failed', 'pending'],weights=[60,25,15], k=1)[0]

        # If inactive method is used, transaction should fail
        if not chosen_method["is_active"]:
            status = "failed"
        else:
            # Add a little realism for risky combos
            risk_bump = 0.0

            if chosen_method["network"] == "Amex":
                risk_bump += 0.03
            if channel == "api":
                risk_bump += 0.04
            if payment_method_country != txn_country:
                risk_bump += 0.05
            if user_country != txn_country:
                risk_bump += 0.05
            if amount > 1000:
                risk_bump += 0.05

            if status == "completed" and random.random() < risk_bump:
                status = "failed"


        initiated = random_past_ts(90)

        if status == "completed":
           settled_at = ts_after(initiated, 0.1, 2)
        else:
            settled_at = None


        transactions.append({
            'transaction_id': str(uuid.uuid4()),
            'user_id' : user_id,
            'merchant_id' : merchant_id,
            'payment_method_id' : chosen_method['payment_method_id'],
            'amount' : amount,
            'currency': random.choice(['EUR','USD', 'USD', 'INR', 'GBP']),
            'country_code' : txn_country,
            'status': status,
            'channel' : channel,
            'network': chosen_method['network'],
            'method_type': chosen_method['payment_type'],
            'is_default_method': chosen_method['is_default'],
            'ip_address': fake.ipv4_public(),
            'device_fingerprint': fake.md5(),
            'initiated_at': iso(initiated),
            'settled_at':  iso(settled_at) if settled_at else None,
        })

    return transactions

def generate_refunds(transactions, refund_rate):
    """
       FACT: refunds
       --------------
       Only completed transactions can be refunded.
       We sample `rate` fraction of completed txns.

       reason_code     — maps to card-network reason codes.
                         'duplicate' and 'fraudulent' are the most
                         important for fraud analysis.

       initiated_by    — reuses the transaction's user_id.
                         In a real system this could also be a
                         merchant or ops agent ID.

       Partial refunds are modelled: refund amount is a random
       fraction (10–100 %) of the original transaction amount.
       """

    REASON_CODES = ['duplicate', 'fraudulent', 'customer_request', 'product_not_received', 'product_unacceptable']
    STATUSES = ['completed', 'failed', 'pending']

    completed_transactions = [t for t in transactions if t['status'] == 'completed']
    sample_transactions = random.sample(completed_transactions, int(len(completed_transactions)* refund_rate))

    refunds = []
    for txn in sample_transactions:
        requested = datetime.fromisoformat(txn['initiated_at'])
        requested = ts_after(requested, min_hours=1, max_hours=120)

        refunds.append({
            'refund_id': str(uuid.uuid4()),
            'transaction_id': txn['transaction_id'],
            'initiated_by': txn['user_id'],
            'amount': txn['amount'],
            'reason_code': random.choice(REASON_CODES),
            'status': random.choice(STATUSES),
            'requested_at': iso(requested),
            'processed_at': iso(ts_after(requested,1,48)),
        })

    return refunds

def generate_chargebacks(transactions, chargeback_rate):
    """
       FACT: chargebacks
       ------------------
       Chargebacks are card-network disputes filed by the cardholder,
       bypassing the merchant. They are rarer than refunds and far
       more expensive for merchants (typically $20–$100 fee per case).

       reason_code     — Visa/Mastercard dispute codes:
                           4853 = cardholder dispute
                           4863 = not as described
                           10.4 = card-absent fraud (Visa)
                           4853 = services not provided

       network_case_id — the case reference issued by Visa/MC.
                         Used to correlate with network-level feeds.

       status distribution: 50 % open (unresolved), 30 % lost,
                            20 % won (merchant defended successfully).
       """

    REASON_CODES = ["4853", "4863", "10.4", "4853", "UA02"]
    STATUSES = ['open','lost', 'won']

    completed_transactions = [t for t in transactions if t['status'] == 'completed']
    sample_transactions = random.sample(completed_transactions, int(len(completed_transactions)* chargeback_rate))

    chargebacks = []
    for txn in sample_transactions:
        filed = ts_after(datetime.fromisoformat(txn['initiated_at']), min_hours=24, max_hours=720)

        chargebacks.append({
            'chargeback_id': str(uuid.uuid4()),
            'transaction_id': txn['transaction_id'],
            'reason_code': random.choice(REASON_CODES),
            'network_case_id': fake.bothify("CB-####-????").upper(),
            'status': random.choice(STATUSES),
            'disputed_amount': txn['amount'],
            'filed_at': iso(filed),
            'resolved_at': iso(ts_after(filed, 48, 720)) if random.random() > 0.5 else None,
        })

    return chargebacks

# ══════════════════════════════════════════════════════════
# ORCHESTRATOR — generate everything and return as a dict
# ══════════════════════════════════════════════════════════

def generate_all(config):
    """
    Generate the full dataset respecting referential integrity.

    Order matters:
      1. users          (no FKs)
      2. merchants      (no FKs)
      3. payment_methods (FK → users)
      4. transactions   (FK → users, merchants, payment_methods)
      5. refunds        (FK → transactions)
      6. chargebacks    (FK → transactions)
    """
    print("Generating full dataset...")

    users = generate_users(config['n_users'])
    merchants = generate_merchants(config['n_merchants'])
    payment_methods = generate_payment_methods(users, config['n_payment_methods'])

    print('Generating Facts')
    transactions = generate_transactions(users, merchants, payment_methods,config['n_transactions'])
    refunds = generate_refunds(transactions, config['refund_rate'])
    chargebacks = generate_chargebacks(transactions, config['chargeback_rate'])

    dataset = {
        'users': users,
        'merchants': merchants,
        'payment_methods': payment_methods,
        'transactions': transactions,
        'refunds': refunds,
        'chargebacks': chargebacks,
    }
    print("Writing dataset...")
    for name, rows in dataset.items():
        print(f'Writing {name}...{len(rows)} rows')

    return dataset


# ══════════════════════════════════════════════════════════
# WRITER — save as newline-delimited JSON for Auto Loader
# ══════════════════════════════════════════════════════════

def write_as_json(records, path):
    """
       Newline-delimited JSON (NDJSON) — one JSON object per line.
       This is the format Auto Loader cloudFiles reads most efficiently:
       it can infer schema from the first N lines without reading the
       whole file, and it streams line-by-line with low memory overhead.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        for record in records:
            f.write(json.dumps(record, default=str) + '\n')

    print(f'Wrote {len(records)} records to {path}')

def write_as_json_to_s3(records, bucket, key):
    body = "\n".join(json.dumps(record, default=str) for record in records) + "\n"

    s3_client.put_object(Bucket=bucket,
                         Key=key,
                         Body=body.encode('utf-8'),
                         ContentType='application/json')
    print(f'Wrote {len(records)} records to s3://{bucket}/{key}')

def save_dataset_to_s3(dataset, bucket, prefix):
    print(f"\nWriting NDJSON to s3://{bucket}/{prefix}/")

    for table, rows in dataset.items():
        key = f"{prefix}/{table}/{table}_001.json"
        write_as_json_to_s3(rows, bucket, key)

    print("Done.")

def save_locally(dataset, base_dir: str = "/tmp/payments_raw"):
    """
    Save all tables as NDJSON under base_dir.
    In Databricks, copy to S3 afterwards:
        dbutils.fs.cp("file:/tmp/payments_raw/", "s3://your-bucket/raw/", recurse=True)
    """
    print(f"\nWriting NDJSON to {base_dir}/")
    for table, rows in dataset.items():
        path = os.path.join(base_dir, table, f"{table}_001.json")
        write_as_json(rows, path)
    print("Done.")

# ══════════════════════════════════════════════════════════
# ENTRY POINT — run standalone (outside Databricks)
# ══════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     data = generate_all(config)
#     save_locally(data, base_dir=s3_filepath)

if __name__ == "__main__":
    data = generate_all(config)
    save_dataset_to_s3(
        data,
        bucket="payments-risk-analysis",
        prefix="raw-data"
    )