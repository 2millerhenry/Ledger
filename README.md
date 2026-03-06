# Ledger-Once

My AI agent charged a customer 5 times.

Retry storm → duplicate Stripe charges

![ledger demo](https://raw.githubusercontent.com/2millerhenry/Ledger/main/docs/demo.gif)

Exactly-once execution for AI agent tool calls.

AI agents retry tools constantly.

timeouts  
parallel workers  
LLM loops

Sometimes those retries hit **real side effects**:

- duplicate Stripe charges
- duplicate refunds
- duplicate emails
- duplicate database writes

Ledger guarantees a tool executes **once**, even if the agent calls it 10 times.

```
agent
  ↓
ledger guard
  ↓
tool
```

> Stripe added idempotency keys for APIs. Ledger brings the same guarantee to AI agents.

---

## Quickstart

```python
from ledger import guard

def charge_card(customer_id, amount):
    print(f"charging {customer_id} ${amount}")

guard(charge_card, "cus_42", 49)  # runs
guard(charge_card, "cus_42", 49)  # blocked
guard(charge_card, "cus_42", 49)  # blocked

guard.log()
# ✓ charge_card   attempts 3   executed 1   blocked 2   ← retried 2×
```

---

## The fix

```python
from ledger import guard

guard(charge_card, customer_id="cus_42", amount=49)  # runs
guard(charge_card, customer_id="cus_42", amount=49)  # blocked
guard(charge_card, customer_id="cus_42", amount=49)  # blocked

guard.log()
# ✓ charge_card   attempts 3   executed 1   blocked 2   ← retried 2×
```

```
Without Ledger                    With Ledger

agent                             agent
  ↓                                 ↓
charge_card()  ← executes         guard()
  ↓                                 ↓
charge_card()  ← executes         charge_card()  ← executes once
  ↓                                 ↓
charge_card()  ← executes         blocked
  ↓                                 ↓
charge_card()  ← executes         blocked

customer charged $245             customer charged $49
```

---

## See the failure in 10 seconds

```bash
git clone https://github.com/2millerhenry/Ledger
cd Ledger
python3 demos/demo_stripe_charge.py
```

```
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $49)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $98)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $147)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $245)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $245)

❌ Customer charged $245  (should be $49)
```

**With Ledger** — same agent, same retries:

```
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $49)

✅ Customer charged $49  (correct)

✓ stripe_charge   attempts 5   executed 1   blocked 4   ← retried 4×
```

Try the other failure modes:

```bash
python3 demos/demo_concurrent.py    # 3 workers race to charge the same customer
python3 demos/demo_agent_loop.py    # runaway agent fires the tool repeatedly
```

---

## Install

Copy [`ledger.py`](ledger.py) into your project:

```python
from ledger import guard
```

Or clone and run the demos:

```bash
git clone https://github.com/2millerhenry/Ledger
cd Ledger
python3 demos/demo_stripe_charge.py
```

---

## Fix it with one line

```python
# Before — retries cause duplicate charges
charge_card(customer_id="cus_42", amount=49)

# After — retries are safe
guard(charge_card, customer_id="cus_42", amount=49)
```

Same agent. Same retries. No duplicate side effects.

---

## Why this happens

LLM agents retry tool calls automatically.

If a tool times out, the agent can't tell whether it executed — so it retries.

If that tool has side effects (charges, refunds, emails), every retry executes again.

This is a classic distributed systems problem. Ledger restores **exactly-once execution**.

---

## Real failures this prevents

- **Duplicate Stripe charges** — customer billed twice for one order
- **Duplicate refunds** — $500 refund becomes $1,500
- **Duplicate emails** — welcome email sent 5 times
- **Duplicate database writes** — record created multiple times

If your agent calls external APIs, it has this bug. You just haven't seen it yet.

---

## The moment you see how bad it was

```python
guard.log()
```

```
  ✓ charge_card          attempts 5   executed 1   blocked 4   ← retried 4×
  ✓ send_email           attempts 6   executed 1   blocked 5   ← retried 5×
  ✓ refund_order         attempts 3   executed 1   blocked 2   ← retried 2×
```

Most teams have never seen these numbers. They're always higher than expected.

---

## Async, decorators, any framework

```python
# Async — same syntax, just await
result = await guard(post_webhook, url="https://...", payload=data)

# Decorator — protect every call at the source
@guard.once
def charge_card(card_id: str, amount: float):
    return stripe.charge(card_id, amount)

# Drop into any agent loop — OpenAI, LangChain, AutoGen, custom
for tool_call in response.tool_calls:
    result = guard(tools[tool_call.name], **tool_call.arguments)
```

---

## Per-tool rules

```python
guard.policy("search_web",      unlimited=True)          # reads: always run
guard.policy("charge_card",     once=True, replay=True)  # writes: once, return cached result on retry
guard.policy("send_sms",        max=2)                   # cap at 2
guard.policy("daily_report",    ttl=86400)               # once per day
```

---

## Custom idempotency key

```python
# Like Stripe's Idempotency-Key header — caller controls identity
guard(charge_card, amount=49, key=f"order-{order_id}")
guard(charge_card, amount=49, key=f"order-{order_id}")  # blocked — same key
```

---

## Survives restarts, crashes, and parallel workers

```python
guard.persist("ledger.db")   # one line at startup
```

SQLite-backed. Atomic claims via `INSERT OR IGNORE`. If your process crashes mid-execution, Ledger detects the stale record and allows a safe retry.

---

## CLI

```bash
ledger show  ledger.db      # print full history
ledger tail  ledger.db      # live-tail as your agent runs
ledger stats ledger.db      # summary + duplicate rate
ledger clear ledger.db      # wipe records
```

---

## Production checklist

| Concern | How Ledger handles it |
|---|---|
| Agent retries after timeout | fingerprint + block |
| Process crashes mid-execution | stale RUNNING detection → safe retry |
| Two workers run in parallel | atomic `INSERT OR IGNORE` claim |
| Args passed positionally vs keyword | normalized to same fingerprint |
| Float args drift from JSON parsing | rounded to 8 decimal places |
| Need result back on retry | `guard.policy("tool", replay=True)` |
| Survive restart | `guard.persist("ledger.db")` |
| Multi-node / Redis | implement the 4-method Store protocol |

---

## Repo structure

```
ledger/
├─ ledger.py          ← the whole library, one file
├─ README.md
├─ LICENSE
├─ pyproject.toml
├─ .gitignore
├─ docs/
│   └─ demo.gif
└─ demos/
    ├─ demo_stripe_charge.py   ← retry storm
    ├─ demo_concurrent.py      ← parallel workers
    └─ demo_agent_loop.py      ← runaway agent
```

---

## Design principles

- one file
- zero dependencies
- works with any agent framework
- safe across retries, crashes, and parallel workers

---

## Roadmap

Ledger currently guarantees exactly-once execution.

Future layers:

- workflow budgets
- agent kill switches
- execution policies
- full action audit logs

---

One word change.

**Exactly-once execution for AI agents.**

```bash
git clone https://github.com/2millerhenry/Ledger
cd Ledger
python3 demos/demo_stripe_charge.py
```
