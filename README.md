# Ledger-Once

```bash
pip install ledger-once
```

My AI agent charged a customer **5 times**.

Retry storm → duplicate Stripe charges

![ledger demo](https://raw.githubusercontent.com/2millerhenry/ledger-once/main/docs/demo.gif)

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

# Quickstart

```python
from ledger import guard

def charge_card(customer_id, amount):
    print(f"charging {customer_id} ${amount}")

guard(charge_card, "cus_42", 49)  # runs
guard(charge_card, "cus_42", 49)  # blocked
guard(charge_card, "cus_42", 49)  # blocked

guard.log()
# ✓ charge_card   attempts 3   executed 1   blocked 2
```

---

# The fix

```python
from ledger import guard

guard(charge_card, customer_id="cus_42", amount=49)  # runs
guard(charge_card, customer_id="cus_42", amount=49)  # blocked
guard(charge_card, customer_id="cus_42", amount=49)  # blocked

guard.log()
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

# See the failure in 10 seconds

```bash
git clone https://github.com/2millerhenry/ledger-once
cd ledger-once
python3 demos/demo_stripe_charge.py
```

```
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $49)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $98)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $147)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $196)
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $245)

❌ Customer charged $245  (should be $49)
```

**With Ledger — same agent, same retries**

```
💳 POST /v1/charges  customer=cus_42  amount=$49  (total so far: $49)

✅ Customer charged $49

✓ stripe_charge   attempts 5   executed 1   blocked 4
```

Try the other failure modes:

```bash
python3 demos/demo_concurrent.py
python3 demos/demo_agent_loop.py
```

---

# Fix it with one line

```python
# before
charge_card(customer_id="cus_42", amount=49)

# after
guard(charge_card, customer_id="cus_42", amount=49)
```

Same agent. Same retries. No duplicate side effects.

---

# Why this happens

LLM agents retry tool calls automatically.

If a tool times out, the agent can't tell whether it executed — so it retries.

If that tool has side effects (charges, refunds, emails), every retry executes again.

This is a classic distributed systems problem. Ledger restores **exactly-once execution**.

---

# Real failures this prevents

- duplicate Stripe charges
- duplicate refunds
- duplicate emails
- duplicate database writes

If your agent calls external APIs, it already has this bug. You just haven't seen it yet.

---

# The moment you see how bad it was

```python
guard.log()
```

```
✓ charge_card     attempts 5   executed 1   blocked 4
✓ send_email      attempts 6   executed 1   blocked 5
✓ refund_order    attempts 3   executed 1   blocked 2
```

Most teams have never seen these numbers. They're always higher than expected.

---

# Async, decorators, any framework

```python
# async
result = await guard(post_webhook, url="https://...", payload=data)

# decorator
@guard.once
def charge_card(card_id: str, amount: float):
    return stripe.charge(card_id, amount)

# agent loop
for tool_call in response.tool_calls:
    result = guard(tools[tool_call.name], **tool_call.arguments)
```

---

# Per-tool rules

```python
guard.policy("search_web", unlimited=True)
guard.policy("charge_card", once=True, replay=True)
guard.policy("send_sms", max=2)
guard.policy("daily_report", ttl=86400)
```

---

# Custom idempotency key

```python
guard(charge_card, amount=49, key=f"order-{order_id}")
guard(charge_card, amount=49, key=f"order-{order_id}")  # blocked
```

---

# Survives restarts and parallel workers

```python
guard.persist("ledger.db")
```

SQLite-backed with atomic claims.

---

# CLI

```bash
ledger show ledger.db
ledger tail ledger.db
ledger stats ledger.db
ledger clear ledger.db
```

---

# Design principles

- one file
- zero dependencies
- works with any agent framework
- safe across retries, crashes, and parallel workers

---

# Roadmap

Ledger currently guarantees exactly-once execution.

Future layers:

- workflow budgets
- agent kill switches
- execution policies
- full action audit logs

---

**Exactly-once execution for AI agents**

```bash
pip install ledger-once
```
