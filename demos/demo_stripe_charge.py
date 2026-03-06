"""
demo_stripe_charge.py

Simulates an AI agent retrying a Stripe charge after timeouts.
Without protection → customer charged multiple times.
Ledger → exactly-once execution.

python demo_stripe_charge.py
"""

import random
import time
from ledger import guard


# ─── Simulated Stripe API ─────────────────────────────────────────────────────

total_charged = 0

def stripe_charge(customer_id: str, amount: float):
    global total_charged
    total_charged += amount
    print(f"  💳 POST /v1/charges  customer={customer_id}  amount=${amount}  (total so far: ${total_charged})")
    return {"charge_id": f"ch_{abs(hash(customer_id)) % 9999:04d}", "amount": amount}


# ─── Without Ledger ───────────────────────────────────────────────────────────

def run_without_ledger():
    print("\n" + "─" * 56)
    print("  WITHOUT LEDGER")
    print("─" * 56)
    print("  Agent charges customer cus_42 for $49.\n")

    # Agent retries the Stripe charge if the request times out.
    # If the charge actually succeeded, every retry charges again.
    for attempt in range(1, 6):
        try:
            if random.random() < 0.75:          # 75% chance of timeout
                stripe_charge("cus_42", 49)     # executes before timeout lands
                raise TimeoutError("timeout — retrying")
            else:
                stripe_charge("cus_42", 49)
                print(f"\n  Agent: confirmed on attempt {attempt}. Done.\n")
                break
        except TimeoutError as e:
            print(f"  Agent: {e}\n")


# ─── With Ledger ──────────────────────────────────────────────────────────────

def run_with_ledger():
    print("\n" + "─" * 56)
    print("  WITH LEDGER")
    print("─" * 56)
    print("  Agent charges customer cus_42 for $49.\n")

    # Agent retries the Stripe charge if the request times out.
    # If the charge actually succeeded, every retry charges again.
    for attempt in range(1, 6):
        try:
            if random.random() < 0.75:
                guard(stripe_charge, "cus_42", 49)   # ← one word change
                raise TimeoutError("timeout — retrying")
            else:
                guard(stripe_charge, "cus_42", 49)
                print(f"\n  Agent: confirmed on attempt {attempt}. Done.\n")
                break
        except TimeoutError as e:
            print(f"  Agent: {e}\n")


# ─── Run both ─────────────────────────────────────────────────────────────────

random.seed(42)   # same luck for both runs

print("\n" + "=" * 56)
print("  THE AGENT RETRY PROBLEM — Stripe Charges")
print("=" * 56)

run_without_ledger()
print(f"  ❌ Customer charged ${total_charged}  (should be $49)")

guard.reset()
total_charged = 0
random.seed(42)   # reset to same sequence

run_with_ledger()
print(f"  ✅ Customer charged ${total_charged}  (correct)")
print("\n  What Ledger saw:\n")
guard.log()
