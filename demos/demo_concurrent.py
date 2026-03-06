"""
demo_concurrent.py

Simulates 3 parallel agent workers all trying to charge the same customer.
Without protection → customer charged 3×.
Ledger → exactly-once execution across all threads.

python demo_concurrent.py
"""

import threading
import time
from ledger import guard


# ─── Simulated Stripe API ─────────────────────────────────────────────────────

charges = []
charges_lock = threading.Lock()

def stripe_charge(customer_id: str, amount: float, worker_id: int = 0):
    time.sleep(0.05)   # slight delay increases chance of real race condition
    with charges_lock:
        charges.append(amount)
        total = sum(charges)
        count = len(charges)
    print(f"  💳 Worker {worker_id} → POST /v1/charges  customer={customer_id}  amount=${amount}  (total so far: ${total})")
    return {"charge_id": f"ch_{count:04d}", "amount": amount}


# ─── Without Ledger ───────────────────────────────────────────────────────────

def run_without_ledger():
    print("\n" + "─" * 56)
    print("  WITHOUT LEDGER — 3 parallel workers")
    print("─" * 56)
    print("  Each worker tries to charge cus_42 for $49.\n")

    threads = [
        threading.Thread(target=lambda i=i: stripe_charge("cus_42", 49, worker_id=i + 1))
        for i in range(3)
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    print(f"\n  ❌ Customer charged {len(charges)}×  =  ${sum(charges)}  (should be $49 once)\n")


# ─── With Ledger ──────────────────────────────────────────────────────────────

def run_with_ledger():
    print("\n" + "─" * 56)
    print("  WITH LEDGER — 3 parallel workers")
    print("─" * 56)
    print("  Each worker tries to charge cus_42 for $49.\n")

    guard.workflow("order-9981")

    threads = [
        threading.Thread(
            target=lambda i=i: guard(stripe_charge, "cus_42", 49, worker_id=i + 1)
        )
        for i in range(3)
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    print(f"\n  ✅ Customer charged {len(charges)}×  =  ${sum(charges)}  (correct)")
    print("\n  What Ledger saw:\n")
    guard.log()
    guard.workflow("default")


# ─── Run both ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 56)
print("  THE CONCURRENT WORKER PROBLEM — Stripe Charges")
print("=" * 56)

run_without_ledger()

charges.clear()
guard.reset()

run_with_ledger()
