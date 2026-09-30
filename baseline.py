# Part A - baseline processor, no fault tolerance.
# Validate -> Process -> Record. First failure stops the whole run.
from data import TRANSACTIONS, FAILING_ATTEMPTS


def validate(tx):
    if tx.amount <= 0:
        raise ValueError(f"{tx.id}: bad amount")


def process(tx, attempt=0):
    if attempt < FAILING_ATTEMPTS[tx.failure]:
        raise RuntimeError(f"{tx.id}: {tx.failure} failure")


def run_baseline():
    done = []
    attempted = 0
    for tx in TRANSACTIONS:
        attempted += 1
        validate(tx)
        try:
            process(tx)
        except RuntimeError as e:
            print("STOP:", e)
            break
        done.append(tx)  # record
        print(f"{tx.id} ok")

    total = sum(t.amount for t in TRANSACTIONS)
    processed = sum(t.amount for t in done)
    return {
        "attempted": attempted,
        "successful": len(done),
        "lost": len(TRANSACTIONS) - len(done),
        "processed_amount": processed,
        "lost_amount": total - processed,
    }


if __name__ == "__main__":
    r = run_baseline()
    print()
    for k, v in r.items():
        print(f"{k}: {v}")
