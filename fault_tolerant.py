# Parts B, C, D - fault tolerant processor.
# use_retry / use_checkpoints let me switch mechanisms on one by one for the analysis.
import copy
import csv
import json
from datetime import datetime

from data import TRANSACTIONS, FAILING_ATTEMPTS

MAX_RETRIES = 2
CHECKPOINT_EVERY = 5
LABELS = ["initial", "retry 1", "retry 2"]


class TransactionError(Exception):
    pass

class NetworkError(TransactionError):
    pass

class TransactionTimeout(TransactionError):
    pass

class DatabaseError(TransactionError):
    pass

ERRORS = {"Network": NetworkError, "Timeout": TransactionTimeout, "Database": DatabaseError}


class Processor:
    def __init__(self, use_retry=True, use_checkpoints=True, log=True):
        self.use_retry = use_retry
        self.use_checkpoints = use_checkpoints
        self.write_logs = log

        self.ledger = {}        # tx id -> {"amount", "status"}  (status OK or PARTIAL)
        self.journal = {}       # successful tx since last checkpoint (id -> amount)
        self.checkpoints = []   # saved states
        self.ok_count = 0
        self.failed = []
        self.retries = 0
        self.rollbacks = 0
        self.step = 0
        self.tx_log = []
        self.attempt_log = []
        self.exceptions = {}    # tx id -> exception class name (first failure)

    # ---------- logging ----------
    def _event(self, **kw):
        self.step += 1
        kw = {"step": self.step, "time": datetime.now().isoformat(timespec="milliseconds"), **kw}
        self.tx_log.append(kw)

    def _attempt(self, tx, n, action, result):
        self.step += 1
        self.attempt_log.append({
            "step": self.step,
            "time": datetime.now().isoformat(timespec="milliseconds"),
            "transaction": tx.id,
            "attempt": LABELS[n] if n is not None else "-",
            "failure": tx.failure,
            "action": action,
            "result": result,
        })

    # ---------- the three steps ----------
    def validate(self, tx):
        if tx.amount <= 0:
            raise ValueError("bad amount")

    def process(self, tx, n):
        if n < FAILING_ATTEMPTS[tx.failure]:
            if tx.failure == "Database":
                # db dies in the middle of the write -> half written record stays in the ledger
                self.ledger[tx.id] = {"amount": tx.amount, "status": "PARTIAL"}
            raise ERRORS[tx.failure](f"{tx.id} {tx.failure} failure on {LABELS[n]}")

    def record(self, tx):
        # idempotent: same id is never written twice
        self.ledger[tx.id] = {"amount": tx.amount, "status": "OK"}

    # ---------- checkpoint / rollback ----------
    def checkpoint(self, name):
        self.checkpoints.append({
            "name": name,
            "count": self.ok_count,
            "total": self.total(),
            "state": copy.deepcopy(self.ledger),
        })
        self.journal = {}
        self._event(event="checkpoint", name=name, successful=self.ok_count, total=self.total())

    def rollback(self, tx):
        self.rollbacks += 1
        if self.checkpoints:
            cp = self.checkpoints[-1]
            base = copy.deepcopy(cp["state"])
            name = cp["name"]
        else:
            base, name = {}, "start"
        self.ledger = base
        # transactions that succeeded after the checkpoint are replayed from the journal
        replayed = 0
        for tid, amount in self.journal.items():
            if tid not in self.ledger:
                self.ledger[tid] = {"amount": amount, "status": "OK"}
                replayed += 1
        self._event(event="rollback", transaction=tx.id, to=name, replayed=replayed)

    def total(self):
        return sum(v["amount"] for v in self.ledger.values() if v["status"] == "OK")

    # ---------- main loop ----------
    def handle(self, tx):
        if tx.id in self.ledger and self.ledger[tx.id]["status"] == "OK":
            self._event(event="duplicate_skipped", transaction=tx.id)
            return

        max_attempts = 1 + (MAX_RETRIES if self.use_retry else 0)
        for n in range(max_attempts):
            action = "process" if n == 0 else f"retry ({tx.failure.lower()})"
            if n > 0:
                self.retries += 1
            try:
                self.validate(tx)
                self.process(tx, n)
                self.record(tx)
            except TransactionError as e:
                self.exceptions.setdefault(tx.id, type(e).__name__)
                self._attempt(tx, n, action, "FAIL")
                self._event(event="error", transaction=tx.id, exception=type(e).__name__, attempt=LABELS[n])
                continue
            self._attempt(tx, n, action, "SUCCESS")
            self.ok_count += 1
            self.journal[tx.id] = tx.amount
            self._event(event="success", transaction=tx.id, attempt=LABELS[n], amount=tx.amount)
            if self.use_checkpoints and self.ok_count % CHECKPOINT_EVERY == 0:
                self.checkpoint(f"CP{len(self.checkpoints) + 1}")
            return

        # all attempts used up
        self.failed.append(tx)
        exc = self.exceptions[tx.id]
        if exc == "DatabaseError" and self.use_checkpoints:
            self._attempt(tx, None, "rollback", "ROLLED BACK")
            self.rollback(tx)
        self._event(event="failed", transaction=tx.id, exception=exc)

    def run(self):
        for tx in TRANSACTIONS:
            self.handle(tx)   # one failure never stops the loop
        if self.use_checkpoints and self.journal:
            self.checkpoint(f"CP{len(self.checkpoints) + 1}")  # final commit
        return self.summary()

    def summary(self):
        all_total = sum(t.amount for t in TRANSACTIONS)
        ok = self.total()
        partial = sum(1 for v in self.ledger.values() if v["status"] == "PARTIAL")
        return {
            "successful": self.ok_count,
            "failed": len(self.failed),
            "processed_amount": ok,
            "lost_amount": all_total - ok,
            "retries": self.retries,
            "rollbacks": self.rollbacks,
            "completion_rate": round(100 * self.ok_count / len(TRANSACTIONS), 2),
            "inconsistent_records": partial,
        }

    def save_logs(self):
        with open("transaction_log.jsonl", "w") as f:
            for row in self.tx_log:
                f.write(json.dumps(row) + "\n")
        with open("attempt_log.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(self.attempt_log[0].keys()))
            w.writeheader()
            w.writerows(self.attempt_log)
        with open("checkpoints.json", "w") as f:
            json.dump([{k: v for k, v in c.items() if k != "state"} | {"saved_ids": sorted(c["state"])}
                       for c in self.checkpoints], f, indent=2)


if __name__ == "__main__":
    stages = [
        ("B: exception handling only", dict(use_retry=False, use_checkpoints=False)),
        ("C: + retry", dict(use_retry=True, use_checkpoints=False)),
        ("D: + checkpoint/rollback (final)", dict(use_retry=True, use_checkpoints=True)),
    ]
    final = None
    for name, kw in stages:
        p = Processor(**kw)
        res = p.run()
        print(f"--- {name}")
        for k, v in res.items():
            print(f"  {k}: {v}")
        final = p
    final.save_logs()
    print("\nchecked idempotency: re-running T001 on final ledger")
    final.handle(TRANSACTIONS[0])
    print(" ", final.tx_log[-1])
