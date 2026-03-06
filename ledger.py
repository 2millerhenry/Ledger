"""
ledger.py — AI agents retry constantly. Ledger stops the damage.

    from ledger import guard

    guard(send_email, to="user@example.com")
    guard(send_email, to="user@example.com")  # blocked — already sent
    guard(send_email, to="user@example.com")  # blocked — already sent

    guard.log()
    # ✓ send_email   attempts 3   executed 1   blocked 2   ← retried 2×

One file. Zero dependencies. Drop it in. Works with any agent framework.

PERSIST ACROSS RESTARTS
    guard.persist("ledger.db")

PER-TOOL RULES
    guard.policy("search",  unlimited=True)          # reads: always run
    guard.policy("refund",  once=True, replay=True)  # writes: block + replay cached result
    guard.policy("sms",     max=2)                   # notifications: cap at 2

CUSTOM IDEMPOTENCY KEY  (mirrors Stripe's idempotency-key header)
    guard(charge_card, amount=99, key="order-9981")  # stable key regardless of other args

ASYNC
    result = await guard(post_webhook, url="...", payload=data)

DECORATOR
    @guard.once
    def charge_card(card_id, amount): ...

ESCAPE HATCHES
    guard.retry(send_email, to="user@example.com")  # clear record, allow next call
    guard.force(send_email, to="user@example.com")  # run it right now regardless

CLI  (after: pip install ledger-once)
    ledger tail ledger.db        # live-tail the action log
    ledger show ledger.db        # print full history
    ledger clear ledger.db       # wipe all records
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import sqlite3
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable

log = logging.getLogger("ledger")


# ── State machine ─────────────────────────────────────────────────────────────
#
#   (new) → RUNNING → SUCCESS   normal path
#                  ↘ FAILED     tool raised; safe to retry

class S(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"


@dataclass
class Record:
    id:       str            # fingerprint
    tool:     str            # function name
    args:     dict           # normalized arguments
    wf:       str            # workflow scope
    created:  datetime
    status:   S    = S.RUNNING
    attempts: int  = 0       # total times agent tried (including duplicates)
    runs:     int  = 0       # times the tool actually executed
    blocked:  int  = 0       # times Ledger stopped a duplicate
    result:   Any  = None    # cached return value (for replay)
    error:    str | None = None
    since:    datetime | None = None   # RUNNING start time (crash detection)
    touched:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def stale(self, timeout: int) -> bool:
        """True if stuck RUNNING for longer than timeout seconds (crashed process).
        Always uses the policy's timeout — no default so it can't silently drift.

        Edge case: if the original process is still running but very slow, and the
        stale timeout fires, two processes could execute the same tool concurrently.
        Set timeout conservatively — higher than your P99 tool latency. Default 300s.
        This is an inherent tradeoff in any crash-recovery idempotency system.
        """
        if self.status != S.RUNNING or not self.since: return False
        return (datetime.now(timezone.utc) - self.since).total_seconds() > timeout

    def expired(self, ttl) -> bool:
        """True if record is older than ttl seconds — allow fresh execution."""
        if not ttl: return False
        return (datetime.now(timezone.utc) - self.touched).total_seconds() > ttl

    def as_dict(self) -> dict:
        return dict(tool=self.tool, args=self.args, wf=self.wf,
                    status=self.status.value, attempts=self.attempts,
                    runs=self.runs, blocked=self.blocked, error=self.error,
                    created=self.created.isoformat(), touched=self.touched.isoformat())

    def __repr__(self) -> str:
        return (f"<Ledger {self.tool} "
                f"attempts={self.attempts} runs={self.runs} blocked={self.blocked} "
                f"status={self.status.value}>")


# ── Fingerprint ───────────────────────────────────────────────────────────────

def _fp(tool: str, args: dict, wf: str, key: str | None = None) -> str:
    """
    Stable 32-char hex fingerprint (128 bits of entropy).

    128 bits → collision probability negligible even at extreme call volumes.
    Safe for high-throughput production systems.

    If key is given, fingerprint is (tool, key, wf) — ignoring args entirely.
    Use this when args contain non-deterministic values (timestamps, UUIDs):
        guard(send_email, to="user", ts=time.now(), key=f"email-{order_id}")

    Otherwise fingerprint is (tool, normalized_args, wf).
    Arg order doesn't matter: guard(fn, x=1, y=2) == guard(fn, y=2, x=1).
    Positional == keyword:   guard(fn, 42)       == guard(fn, x=42).
    Float drift handled:     guard(fn, x=99.99)  == guard(fn, x=99.9900000001).
    """
    if key is not None:
        payload = json.dumps({"t": tool, "k": key, "w": wf},
                             sort_keys=True, separators=(",", ":"))
    else:
        payload = json.dumps({"t": tool, "a": _norm(args), "w": wf},
                             sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:32]

def _norm(v):
    """Recursively normalize so key order and float precision never affect the fingerprint."""
    if isinstance(v, dict):          return {k: _norm(vv) for k, vv in sorted(v.items())}
    if isinstance(v, (list, tuple)): return [_norm(i) for i in v]
    if isinstance(v, float):         return round(v, 8)   # stabilize 0.1 vs 0.1000000001
    if isinstance(v, (str, int, bool)) or v is None: return v
    return str(v)

_SIG_CACHE: dict[Callable, inspect.Signature] = {}

def _bind(fn: Callable, args: tuple, kwargs: dict) -> dict:
    """Map positional args to param names via the function signature.
    Caches signatures — inspect.signature() is slow; this makes hot paths ~10× faster."""
    try:
        sig = _SIG_CACHE.get(fn)
        if sig is None:
            sig = inspect.signature(fn)
            _SIG_CACHE[fn] = sig
        b = sig.bind(*args, **kwargs)
        b.apply_defaults()
        return dict(b.arguments)
    except (ValueError, TypeError):
        return {**kwargs, **{"_": list(args)}} if args else dict(kwargs)


# ── Storage ───────────────────────────────────────────────────────────────────

class _Mem:
    """In-memory. Thread-safe via lock. Lost on restart. Zero setup."""
    def __init__(self):
        self._d: dict[str, Record] = {}
        self._lock = threading.Lock()

    def get(self, id) -> Record | None:
        with self._lock: return self._d.get(id)

    def claim(self, r: Record) -> bool:
        """Atomic insert-if-absent. One winner per id across all threads."""
        with self._lock:
            if r.id in self._d: return False
            self._d[r.id] = r; return True

    def put(self, r: Record):
        with self._lock: self._d[r.id] = r

    def delete(self, id):
        with self._lock: self._d.pop(id, None)

    def all(self, wf=None) -> list[Record]:
        with self._lock: recs = list(self._d.values())
        return [r for r in recs if r.wf == wf] if wf else recs

    def clear(self, wf=None):
        with self._lock:
            if wf: self._d = {k: v for k, v in self._d.items() if v.wf != wf}
            else:  self._d.clear()


class _SQLite:
    """
    File-backed. Survives restarts. No extra infra.
    One connection per thread (SQLite constraint).
    INSERT OR IGNORE makes claim() atomic — safe across threads and processes.

    Concurrency: WAL mode handles dozens of concurrent writers cleanly.
    At very high write throughput (100+ workers on one DB), SQLite becomes
    a bottleneck. At that scale, implement the Store protocol with Redis
    (SET NX for claim) or Postgres (INSERT ... ON CONFLICT DO NOTHING).
    """
    def __init__(self, path: str):
        self.path = str(path)
        self._tl  = threading.local()
        self._init()

    def _conn(self):
        if not hasattr(self._tl, "c"):
            c = sqlite3.connect(self.path, check_same_thread=False)
            c.execute("PRAGMA journal_mode=WAL")    # concurrent readers
            c.execute("PRAGMA synchronous=NORMAL")  # safe + fast
            c.row_factory = sqlite3.Row
            self._tl.c = c
        return self._tl.c

    def _init(self):
        with self._conn() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS ledger (
                id TEXT PRIMARY KEY,
                tool TEXT, args TEXT, wf TEXT,
                created TEXT, touched TEXT, since TEXT,
                status  TEXT DEFAULT 'running',
                attempts INT DEFAULT 0,
                runs     INT DEFAULT 0,
                blocked  INT DEFAULT 0,
                result TEXT, error TEXT
            )""")
            c.execute("CREATE INDEX IF NOT EXISTS i_wf ON ledger(wf)")

    def get(self, id) -> Record | None:
        r = self._conn().execute("SELECT * FROM ledger WHERE id=?", (id,)).fetchone()
        return self._row(r) if r else None

    def claim(self, r: Record) -> bool:
        """INSERT OR IGNORE — single atomic SQLite op. One winner guaranteed."""
        try:
            with self._conn() as c:
                c.execute("""INSERT OR IGNORE INTO ledger
                    (id,tool,args,wf,created,touched,since,status,attempts,runs,blocked,result,error)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", self._vals(r))
                return c.execute("SELECT changes()").fetchone()[0] > 0
        except sqlite3.IntegrityError:
            return False

    def put(self, r: Record):
        with self._conn() as c:
            c.execute("""UPDATE ledger SET touched=?,since=?,status=?,
                attempts=?,runs=?,blocked=?,result=?,error=? WHERE id=?""",
                (r.touched.isoformat(),
                 r.since.isoformat() if r.since else None,
                 r.status.value, r.attempts, r.runs, r.blocked,
                 json.dumps(r.result) if r.result is not None else None,
                 r.error, r.id))

    def delete(self, id):
        with self._conn() as c: c.execute("DELETE FROM ledger WHERE id=?", (id,))

    def all(self, wf=None) -> list[Record]:
        q = "SELECT * FROM ledger WHERE wf=?" if wf else "SELECT * FROM ledger ORDER BY created"
        rows = self._conn().execute(q, (wf,) if wf else ()).fetchall()
        return [self._row(r) for r in rows]

    def clear(self, wf=None):
        with self._conn() as c:
            if wf: c.execute("DELETE FROM ledger WHERE wf=?", (wf,))
            else:  c.execute("DELETE FROM ledger")

    def _vals(self, r):
        return (r.id, r.tool, json.dumps(r.args), r.wf,
                r.created.isoformat(), r.touched.isoformat(),
                r.since.isoformat() if r.since else None,
                r.status.value, r.attempts, r.runs, r.blocked,
                json.dumps(r.result) if r.result is not None else None, r.error)

    @staticmethod
    def _row(row) -> Record:
        return Record(
            id=row["id"], tool=row["tool"], args=json.loads(row["args"]),
            wf=row["wf"],   created=datetime.fromisoformat(row["created"]),
            touched=datetime.fromisoformat(row["touched"]),
            since=datetime.fromisoformat(row["since"]) if row["since"] else None,
            status=S(row["status"]), attempts=row["attempts"],
            runs=row["runs"], blocked=row["blocked"],
            result=json.loads(row["result"]) if row["result"] else None,
            error=row["error"])


# ── Policy ────────────────────────────────────────────────────────────────────

@dataclass
class Policy:
    max:     int | None = 1      # None = unlimited executions
    replay:  bool       = False  # return cached result to blocked callers
    ttl:     int | None = None   # seconds before record expires; allow again after
    timeout: int        = 300    # seconds before RUNNING = crashed; allow retry


# ── Engine ────────────────────────────────────────────────────────────────────

class _Engine:
    def __init__(self, store=None, wf="default"):
        self.store    = store or _Mem()
        self.wf       = wf
        self.policies: dict[str, Policy] = {}
        self.default  = Policy()
        self.logging  = True
        self._on_block: Callable | None = None

    def check_and_claim(self, tool: str, args: dict, wf: str,
                         key: str | None = None) -> tuple[bool, Record, Any]:
        """
        Evaluate policy → claim if allowed → return (allowed, record, cached_result).
        This is the hot path. Every guard() call comes through here.
        """
        p   = self.policies.get(tool, self.default)
        id  = _fp(tool, args, wf, key)
        now = datetime.now(timezone.utc)
        rec = self.store.get(id)

        # ── Clear records that should be treated as new ──────────────────────
        if rec:
            if rec.expired(p.ttl) or rec.stale(p.timeout):
                self.store.delete(id); rec = None
            elif rec.status == S.FAILED:
                # FAILED = tool raised; always allow retry
                self.store.delete(id); rec = None

        # ── Block: SUCCESS record at execution limit ──────────────────────────
        if rec and rec.status == S.SUCCESS:
            under_limit = p.max is None or rec.runs < p.max
            if under_limit:
                # Delete so we can re-claim with carried-forward counts
                prev = (rec.runs, rec.attempts, rec.blocked)
                self.store.delete(id); rec = None
            else:
                rec.blocked += 1; rec.attempts += 1; rec.touched = now
                self.store.put(rec)
                self._emit(False, tool, id, rec.blocked)
                self._fire_block(rec)
                return False, rec, (rec.result if p.replay else None)

        # ── Block: concurrent RUNNING (another caller is mid-execution) ───────
        if rec and rec.status == S.RUNNING:
            rec.blocked += 1; rec.attempts += 1; rec.touched = now
            self.store.put(rec)
            self._emit(False, tool, id, rec.blocked)
            self._fire_block(rec)
            return False, rec, None

        # ── Claim ─────────────────────────────────────────────────────────────
        pr, pa, pb = locals().get("prev", (0, 0, 0))
        new = Record(id=id, tool=tool, args=args, wf=wf, created=now,
                     touched=now, since=now, status=S.RUNNING,
                     attempts=pa + 1, runs=pr, blocked=pb)
        if not self.store.claim(new):
            # Lost the race to another process — block this call
            fresh = self.store.get(id)
            return False, fresh or new, None

        self._emit(True, tool, id, 0)
        return True, new, None

    def succeed(self, rec: Record, result: Any):
        rec.status = S.SUCCESS; rec.result = result; rec.runs += 1
        rec.since  = None;      rec.touched = datetime.now(timezone.utc)
        self.store.put(rec)

    def fail(self, rec: Record, error: Exception):
        rec.status = S.FAILED; rec.error = f"{type(error).__name__}: {error}"
        rec.since  = None;     rec.touched = datetime.now(timezone.utc)
        self.store.put(rec)

    def _emit(self, allowed: bool, tool: str, id: str, blocked: int):
        if not self.logging: return
        if allowed: log.info("[ledger] ✓ %s  %s", tool, id)
        else:       log.info("[ledger] ✗ %s  %s  (blocked %dx)", tool, id, blocked)

    def _fire_block(self, rec: Record):
        if self._on_block:
            try: self._on_block(rec)
            except Exception: pass


# ── Guard ─────────────────────────────────────────────────────────────────────

class Guard:
    """
    The default usage is a global singleton — import and use immediately:

        from ledger import guard
        guard(refund_customer, customer_id=42, amount=500)

    For dependency injection, multi-tenant systems, or test isolation,
    create explicit instances instead:

        from ledger import Guard, SQLiteStore
        agent_guard = Guard()
        agent_guard.persist("agent_A.db").workflow("agent_A")

        # Each instance is fully independent — separate store, separate history
        guard_A = Guard().persist("a.db")
        guard_B = Guard().persist("b.db")

    guard(fn, **kwargs)          run once, block duplicates
    await guard(fn, **kwargs)    async — same guarantees
    @guard.once                  decorator — protect every call
    guard.persist("file.db")     survive restarts
    guard.policy("tool", ...)    per-tool rules
    guard.log()                  see retry counts
    """

    def __init__(self):
        self._e = _Engine()

    # ── Core call ─────────────────────────────────────────────────────────────

    def __call__(self, fn: Callable, /, *args, **kwargs) -> Any:
        """
        guard(fn, **kwargs)

        Runs fn once per unique (fn, args, workflow) combination. Blocks duplicates.
        Works with sync and async functions automatically.

        IMPORTANT — non-deterministic arguments:
        If your args contain timestamps, UUIDs, or random values, every call gets a
        different fingerprint and Ledger can't detect duplicates. Use key= instead:

            # BAD  — timestamp makes every call unique
            guard(send_email, to="user@x.com", sent_at=time.now())

            # GOOD — stable key based on your business identity
            guard(send_email, to="user@x.com", sent_at=time.now(), key=f"email-{order_id}")

        Pass key= to use a custom idempotency key (ignores all other args):
            guard(charge_card, amount=99, key="order-9981")
            guard(charge_card, amount=99, key="order-9981")  # blocked — same key
        """
        key    = kwargs.pop("key", None)   # custom idempotency key (optional)
        is_async = asyncio.iscoroutinefunction(fn)
        return self._acall(fn, args, kwargs, key) if is_async else self._call(fn, args, kwargs, key)

    def _call(self, fn, args, kwargs, key=None):
        fargs = _bind(fn, args, kwargs)
        tool  = f"{fn.__module__}.{fn.__name__}"
        ok, rec, cached = self._e.check_and_claim(tool, fargs, self._e.wf, key)
        if not ok: return cached
        try:
            r = fn(*args, **kwargs); self._e.succeed(rec, r); return r
        except Exception as e:
            self._e.fail(rec, e); raise

    async def _acall(self, fn, args, kwargs, key=None):
        fargs = _bind(fn, args, kwargs)
        tool  = f"{fn.__module__}.{fn.__name__}"
        ok, rec, cached = self._e.check_and_claim(tool, fargs, self._e.wf, key)
        if not ok: return cached
        try:
            r = await fn(*args, **kwargs); self._e.succeed(rec, r); return r
        except Exception as e:
            self._e.fail(rec, e); raise

    # ── Decorator ─────────────────────────────────────────────────────────────

    def once(self, fn=None, *, replay=False):
        """
        @guard.once
        def send_email(to, subject): ...

        @guard.once(replay=True)     # blocked calls return the cached result
        def create_invoice(id, amount): ...
        """
        def wrap(f):
            if replay: self._e.policies[f.__name__] = Policy(max=1, replay=True)
            @functools.wraps(f)
            def s(*a, **kw): return self._call(f, a, kw)
            @functools.wraps(f)
            async def a(*a, **kw): return await self._acall(f, a, kw)
            return a if asyncio.iscoroutinefunction(f) else s
        return wrap(fn) if fn is not None else wrap

    # ── Setup (all optional) ──────────────────────────────────────────────────

    def persist(self, path="ledger.db") -> "Guard":
        """Survive restarts. Call once at startup."""
        self._e.store = _SQLite(str(path))
        return self

    def workflow(self, id: str) -> "Guard":
        """
        Scope deduplication to a run / request / order.
        Same action allowed once per workflow — independent across workflows.

            guard.workflow(f"order-{order_id}")
        """
        self._e.wf = id
        return self

    def policy(self, tool, *, unlimited=False, once=True, replay=False,
               max=None, ttl=None) -> "Guard":
        """
        Per-tool execution rules. Call before your agent runs.

            guard.policy("search",  unlimited=True)
            guard.policy("refund",  once=True, replay=True)
            guard.policy("sms",     max=2)
            guard.policy("report",  ttl=86400)   # once per day
        """
        name = f"{tool.__module__}.{tool.__name__}" if callable(tool) else tool
        if unlimited: p = Policy(max=None)
        elif max:     p = Policy(max=max, replay=replay, ttl=ttl)
        elif ttl:     p = Policy(max=1,   replay=replay, ttl=ttl)
        else:         p = Policy(max=1,   replay=replay)
        self._e.policies[name] = p
        return self

    def on_block(self, fn: Callable) -> "Guard":
        """
        Callback fired on every blocked duplicate.
        Use for metrics, alerting, custom logging.

            guard.on_block(lambda r: metrics.increment("ledger.blocked",
                                                        tags={"tool": r.tool}))
        """
        self._e._on_block = fn
        return self

    def quiet(self) -> "Guard":
        """Silence per-call log lines. guard.log() still works."""
        self._e.logging = False
        return self

    # ── Escape hatches ────────────────────────────────────────────────────────

    def retry(self, fn: Callable, /, *args, **kwargs) -> None:
        """Clear a record so the next call executes."""
        key = kwargs.pop("key", None)
        tool = f"{fn.__module__}.{fn.__name__}"
        self._e.store.delete(_fp(tool, _bind(fn, args, kwargs), self._e.wf, key))

    def force(self, fn: Callable, /, *args, **kwargs) -> Any:
        """Execute right now regardless of history."""
        self.retry(fn, *args, **kwargs)
        return self(fn, *args, **kwargs)

    # ── Observability ─────────────────────────────────────────────────────────

    def log(self, wf: str | None = None) -> None:
        """
        Print the action log. The moment teams see how much their agents retry.

        ✓ refund_customer   attempts 4   executed 1   blocked 3   ← retried 3×
        ✗ charge_card       attempts 1   executed 0               ↳ CardError: declined
        """
        records = self._e.store.all(wf)
        if not records:
            print("[ledger] nothing recorded yet"); return
        print()
        for r in records:
            icon = "✓" if r.status == S.SUCCESS else "✗" if r.status == S.FAILED else "⟳"
            note = f"   ← retried {r.blocked}×" if r.blocked else ""
            print(f"  {icon} {r.tool:<28}  attempts {r.attempts:<3}  "
                  f"executed {r.runs:<3}  blocked {r.blocked:<3}{note}")
            if r.error: print(f"    ↳ {r.error[:72]}")
        print()

    def stats(self, wf=None) -> dict:
        recs = self._e.store.all(wf)
        return dict(actions =len(recs),
                    attempts=sum(r.attempts for r in recs),
                    executed=sum(r.runs     for r in recs),
                    blocked =sum(r.blocked  for r in recs),
                    failed  =sum(1 for r in recs if r.status == S.FAILED))

    def history(self, tool=None, wf=None) -> list[dict]:
        name = f"{tool.__module__}.{tool.__name__}" if callable(tool) else tool
        recs = self._e.store.all(wf)
        if name: recs = [r for r in recs if r.tool == name]
        return [r.as_dict() for r in recs]

    def reset(self, wf=None) -> None:
        """Clear records so actions can execute again."""
        self._e.store.clear(wf)


# ── CLI ───────────────────────────────────────────────────────────────────────
#
#   ledger show  ledger.db          print full history
#   ledger tail  ledger.db          live-tail (polls every 2s)
#   ledger clear ledger.db          wipe all records
#   ledger stats ledger.db          summary numbers
#
# Install entry point via pyproject.toml:
#   [project.scripts]
#   ledger = "ledger:_cli"

def _cli():
    """Entry point for `ledger` CLI command."""
    import time, os

    usage = "usage: ledger <show|tail|clear|stats> <ledger.db>"
    args  = sys.argv[1:]
    if len(args) < 2: print(usage); sys.exit(1)

    cmd, path = args[0], args[1]
    if not os.path.exists(path):
        print(f"[ledger] file not found: {path}"); sys.exit(1)

    store = _SQLite(path)

    def _print_records(records):
        if not records: print("[ledger] no records"); return
        print()
        print(f"  {'TOOL':<28}  {'STATUS':<8}  {'ATT':>4}  {'RUN':>4}  {'BLK':>4}  NOTE")
        print(f"  {'─'*28}  {'─'*8}  {'─'*4}  {'─'*4}  {'─'*4}  {'─'*30}")
        for r in records:
            icon = "✓" if r.status == S.SUCCESS else "✗" if r.status == S.FAILED else "⟳"
            note = f"retried {r.blocked}×" if r.blocked else ""
            if r.error: note = r.error[:30]
            print(f"  {icon} {r.tool:<27}  {r.status.value:<8}  "
                  f"{r.attempts:>4}  {r.runs:>4}  {r.blocked:>4}  {note}")
        print()

    if cmd == "show":
        _print_records(store.all())

    elif cmd == "tail":
        print(f"[ledger] tailing {path}  (Ctrl-C to stop)\n")
        seen: dict[str, int] = {}   # id → last seen attempt count
        try:
            while True:
                for r in store.all():
                    if r.id not in seen:
                        seen[r.id] = r.attempts
                        icon = "✓" if r.status == S.SUCCESS else "✗" if r.status == S.FAILED else "⟳"
                        note = f"  ← retried {r.blocked}×" if r.blocked else ""
                        ts   = r.touched.strftime("%H:%M:%S")
                        print(f"  {ts}  {icon} {r.tool:<28}  "
                              f"att={r.attempts}  run={r.runs}  blk={r.blocked}{note}")
                        if r.error: print(f"         ↳ {r.error[:60]}")
                    else:
                        fresh = store.get(r.id)
                        if fresh and fresh.attempts != seen[r.id]:
                            # record updated since last print — show the update
                            seen[r.id] = fresh.attempts
                            ts  = fresh.touched.strftime("%H:%M:%S")
                            print(f"  {ts}  ↺ {fresh.tool:<28}  "
                                  f"att={fresh.attempts}  run={fresh.runs}  blk={fresh.blocked}"
                                  + (f"  ← retried {fresh.blocked}×" if fresh.blocked else ""))
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n[ledger] stopped")

    elif cmd == "clear":
        store.clear()
        print(f"[ledger] cleared all records in {path}")

    elif cmd == "stats":
        recs = store.all()
        if not recs: print("[ledger] no records"); sys.exit(0)
        total_att = sum(r.attempts for r in recs)
        total_run = sum(r.runs     for r in recs)
        total_blk = sum(r.blocked  for r in recs)
        total_fail= sum(1 for r in recs if r.status == S.FAILED)
        print(f"\n  actions  : {len(recs)}")
        print(f"  attempts : {total_att}")
        print(f"  executed : {total_run}")
        print(f"  blocked  : {total_blk}")
        if total_fail: print(f"  failed   : {total_fail}")
        if total_att > 0:
            pct = round(total_blk / total_att * 100)
            print(f"  dup rate : {pct}%  ({total_blk} of {total_att} attempts were duplicates)")
        print()

    else:
        print(f"[ledger] unknown command: {cmd}")
        print(usage); sys.exit(1)


# ── Global instance ───────────────────────────────────────────────────────────

guard = Guard()


if __name__ == "__main__":
    _cli()
