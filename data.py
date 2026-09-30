# synthetic data from the assignment
from dataclasses import dataclass

@dataclass
class Tx:
    id: str
    amount: int
    failure: str  # "None", "Network", "Timeout", "Database"

TRANSACTIONS = [
    Tx("T001", 12000, "None"),
    Tx("T002", 25000, "Network"),
    Tx("T003", 8000, "None"),
    Tx("T004", 45000, "Timeout"),
    Tx("T005", 13000, "None"),
    Tx("T006", 70000, "Database"),
    Tx("T007", 9000, "None"),
    Tx("T008", 31000, "Network"),
    Tx("T009", 15000, "None"),
    Tx("T010", 50000, "Timeout"),
    Tx("T011", 6000, "None"),
    Tx("T012", 80000, "Database"),
    Tx("T013", 11000, "None"),
    Tx("T014", 22000, "None"),
    Tx("T015", 40000, "Network"),
]

# how many attempts fail before the transaction can succeed
# Network: initial fails, retry 1 ok. Timeout: initial + retry 1 fail, retry 2 ok.
# Database: initial, retry 1, retry 2 all fail -> never succeeds
FAILING_ATTEMPTS = {"None": 0, "Network": 1, "Timeout": 2, "Database": 3}
