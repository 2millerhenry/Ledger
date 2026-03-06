"""
demo_agent_loop.py

Simulates a runaway agent loop that keeps calling the payment tool.
Without protection → customer charged on every iteration.
Ledger → exactly-once, no matter how many times the loop fires.

python demo_agent_loop.py
"""

import random
import time
from ledger import guard


# ─── Simulated Stripe API ─────────────────────────────────────────────────────

total_charged = 0

def stripe_charge(customer_id: str, amount: float):
    global total_charged
    total_charged += amount
    print(f"  💳 POST /v1/charges  customer={customer_id}  amount=${amount}"
          f"  (total so far: ${total_charged})")
    return {"charge_id": f"ch_{abs(hash(customer_id)) % 9999:04d}", "amount": amount}


# ─── Without Ledger ───────────────────────────────────────────────────────────

def run_without_ledger():
    print("\n" + "─" * 56)
    print("  WITHOUT LEDGER — 12 agent loop iterations")
    print("─" * 56)
    print("  Agent wants to charge cus_42 for $49.\n")

    for i in range(12):
        print(f"  Agent iteration: {i + 1}")
        if random.random() < 0.8:    # agent fires the tool most iterations
            stripe_charge("cus_42", 49)
        time.sleep(0.02)

    print(f"\n  ❌ Customer charged ${total_charged}  (should be $49)\n")


# ─── With Ledger ──────────────────────────────────────────────────────────────

def run_with_ledger():
    print("\n" + "─" * 56)
    print("  WITH LEDGER — same 12 agent loop iterations")
    print("─" * 56)
    print("  Agent wants to charge cus_42 for $49.\n")

    for i in range(12):
        print(f"  Agent iteration: {i + 1}")
        if random.random() < 0.8:
            guard(stripe_charge, "cus_42", 49)   # ← one word change
        time.sleep(0.02)

    print(f"\n  ✅ Customer charged ${total_charged}  (correct)")
    print("\n  What Ledger saw:\n")
    guard.log()


# ─── Run both ─────────────────────────────────────────────────────────────────

random.seed(7)   # same luck for both runs

print("\n" + "=" * 56)
print("  THE AGENT LOOP PROBLEM — Stripe Charges")
print("=" * 56)

run_without_ledger()

total_charged = 0
guard.reset()

run_with_ledger()
