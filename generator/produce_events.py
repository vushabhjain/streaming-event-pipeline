import json, os, random, time, uuid
from datetime import datetime, timezone, timedelta

def run_generator(total_batches=5, records_per_batch=2000, interval_seconds=2):
    output_dir = "/app/storage/landing"
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)

    merchants = ["MCH_001", "MCH_002", "MCH_003", "MCH_004", "MCH_005"]
    invalid_merchants = ["UNREGISTERED_INC", "ANON_CORP"]
    now = datetime.now(timezone.utc)

    print(f">>> Producing {total_batches} batches ({records_per_batch} events each) to {output_dir}", flush=True)

    for b in range(1, total_batches + 1):
        records = []
        for i in range(1, records_per_batch + 1):
            is_dup = (i % 50 == 0) and i > 1
            is_null = (i % 100 == 0)
            is_neg = (i % 80 == 0)
            is_bad_merchant = (i % 70 == 0)
            is_late = (i % 40 == 0)

            tx_id = "TX_STATIC_DUPLICATE_ID" if is_dup else (None if is_null else str(uuid.uuid4()))
            event_time = (now - timedelta(minutes=30)) if is_late else (now - timedelta(seconds=random.randint(1, 60)))
            amount = -50.0 if is_neg else round(random.uniform(5.0, 500.0), 2)
            merchant = random.choice(invalid_merchants) if is_bad_merchant else random.choice(merchants)

            records.append({
                "transaction_id": tx_id,
                "account_id": f"ACC_{random.randint(100, 150)}",
                "merchant_id": merchant,
                "amount": amount,
                "currency": "USD",
                "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            })

        file_path = os.path.join(output_dir, f"events_batch_{b}_{int(time.time())}.json")
        with open(file_path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        print(f"[Producer] Batch {b}/{total_batches} written: {file_path}", flush=True)
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_generator()
