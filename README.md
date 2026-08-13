# circuit-breaker-mesh

A shared resilience library for service-to-service calls: per-dependency circuit
breaking, bulkhead isolation, and opt-in decorrelated-jitter retry — plus a
chaos-testing harness that injects real latency/errors/timeouts into a real
HTTP server and asserts on the resulting breaker transitions and throughput,
instead of just asserting the library "looks right" against mocks.

Minimal-dependency, framework-agnostic, plain TypeScript. No runtime
dependencies. Works from any Node.js service — Express, Fastify, NestJS, a
worker script, whatever is making the outbound call.

## The problem

A service calling three downstream dependencies typically has one of two
failure modes when a dependency degrades:

1. **No isolation at all** — a slow or failing dependency exhausts the
   caller's event loop / connection pool / thread pool, and a problem in one
   dependency takes down calls to every *other* dependency sharing the
   process, even healthy ones.
2. **A circuit breaker bolted on per-call-site, badly** — ad hoc breaker
   instances scattered through the codebase with no consistent state
   machine, no shared bulkhead, and retry logic layered on top that
   actually makes things worse (synchronized retry storms against an
   already-struggling dependency).

`circuit-breaker-mesh` is the version of this that a service actually wants:
one `wrapCall(dependencyName, fn, options)` entry point, independent breaker
+ bulkhead state per dependency name, and retry that's opt-in, jittered, and
only applied to calls the caller has explicitly marked safe to retry.

## Architecture

```
wrapCall(dependencyName, fn, options)
        │
        ▼
  CircuitBreaker (per dependency)  ──canAttempt()──▶  reject fast (CircuitOpenError)
        │ allowed
        ▼
  Bulkhead (per dependency)        ──acquire()─────▶  queue or reject (BulkheadFullError)
        │ slot acquired
        ▼
  Bulkhead (global, shared)        ──acquire()─────▶  queue or reject
        │ slot acquired
        ▼
     fn() runs, optional timeoutMs race
        │
        ▼
  record success/failure on the breaker, release both bulkhead slots
        │
        ▼ (only if options.retry.retryable === true and attempts remain)
  decorrelatedJitterDelay(prevDelay, base, cap) → sleep → retry
```

### Design decisions

- **Per-dependency circuit breaker state, not global.** One misbehaving
  dependency shouldn't trip a breaker that also gates calls to unrelated,
  healthy dependencies. Each `dependencyName` gets its own
  `CircuitBreaker` and its own rolling window of outcomes, independent
  thresholds, and independent open/half-open timers.

- **Bulkhead per dependency, plus one optional shared global bulkhead.**
  Concurrency is capped per dependency (so a saturated dependency's queue
  can never itself starve a *different* dependency's concurrency budget),
  and optionally capped again globally (so the aggregate fan-out across
  *all* dependencies can't exceed a process-wide ceiling). Both are
  simple counting semaphores (`src/bulkhead.ts`) — no threads, no
  external dependency, just a queue of pending resolvers.

- **Decorrelated jitter, not exponential backoff or "full jitter."**
  `sleep = min(cap, random_between(base, prevSleep * 3))`. This is the
  AWS Architecture Blog formula. The key property: each client's next
  delay is derived from *its own previous delay*, not from a shared
  deterministic sequence (`base * 2^attempt`). Full jitter
  (`random_between(0, base * 2^attempt)`) still re-derives from the same
  fixed exponential ladder every attempt, so many clients that started
  retrying at the same moment stay loosely bucketed around the same
  handful of delay values. Decorrelated jitter's dependence on each
  client's own random history makes the delay sequences fan out over
  time instead — see the retry-storm numbers below for the measured
  difference.

  **Rejected: full jitter.** Simpler to reason about, but weaker
  anti-synchronization for exactly the reason above — it was the initial
  candidate but didn't hold up under `test/retry-storm.test.ts`'s
  arrival-distribution comparison as well as decorrelated jitter.

  **Rejected: naive exponential backoff (still shipped, but only as the
  deliberately-bad baseline).** Zero randomization means every client
  computes the identical delay sequence — the worst possible case,
  included in `src/retry.ts` specifically so the chaos harness has
  something concrete to compare against instead of an assertion in prose.

- **Retry is opt-in per call, never a default.** `wrapCall` never retries
  unless the caller passes `retry: { retryable: true, ... }`. Retrying a
  non-idempotent call (e.g. "charge this card") on a transient error can
  duplicate side effects; the type signature makes `retryable: true` a
  conscious choice rather than a config default someone forgets is on.

  **Rejected: a mesh-wide "retry everything transient" default.**
  Convenient, but silently unsafe for any non-idempotent call a future
  maintainer routes through the same dependency name.

- **Fault injection against a real `http` server, not mocked breaker
  logic.** `chaos/fakeServer.ts` is a real `node:http` server
  (`FaultInjectingServer`) with per-route configurable latency, error
  rate, and hang/timeout rate. The chaos tests make real HTTP calls
  through real sockets via `fetch`; nothing about the breaker, bulkhead,
  or retry code path is mocked. The only thing simulated is what a real
  downstream would do.

- **Aborted-socket hardening in the fake server.** Fault injection means
  the harness deliberately serves clients that hang or time out. Two
  failure modes had to be handled explicitly so tests terminate reliably
  instead of leaking file descriptors or timers across runs:
  - `res.on('error', () => {})` plus a `res.writableEnded` /
    `res.destroyed` guard before writing, so a `setTimeout`-scheduled
    response firing after the client already gave up doesn't throw an
    unhandled `ERR_STREAM_WRITE_AFTER_END`/socket-hangup error.
  - The injected-latency `setTimeout` itself is cleared on `res`'s
    `'close'` event. Without this, a client aborting before a large
    injected latency elapses (chaos scenarios configure latency far past
    the client's own timeout on purpose) leaves that timer pending for
    its full duration — with hundreds of aborted calls in a saturation
    test, that's hundreds of live timers holding the event loop open
    long after the test's assertions have already finished, turning a
    ~3 second test file into a 60+ second one.
  - `server.stop()` calls `closeAllConnections()` before `close()`, so
    idle keep-alive sockets left open by `fetch` don't hold the server
    alive until Node's default 5s `keepAliveTimeout`.

### Rejected: NestJS (or any framework) as a dependency

This library intentionally has **zero runtime dependencies** and does not
depend on NestJS, even though the natural first consumer is a NestJS
service. Making it framework-agnostic plain TypeScript means it's usable
from Express, Fastify, a bare `http` server, a queue worker, or a script —
anywhere a service makes an outbound call to another service. NestJS
consumption is shown below as an integration pattern, not baked in as a
dependency.

## Setup

```bash
git clone <this-repo-url>
cd circuit-breaker-mesh
npm install
npm run build       # emits dist/ (CJS-free ESM + .d.ts)
npm test             # full test suite, ~3-4s
npm run test:chaos   # just the chaos + bulkhead-isolation scenarios
npm run demo         # runnable end-to-end demo, prints real numbers
```

Requires Node.js >= 18 (uses global `fetch` and `AbortController`).

## Usage

```typescript
import { createMesh, CircuitOpenError, BulkheadFullError } from 'circuit-breaker-mesh';

const mesh = createMesh({
  globalMaxConcurrent: 200,
  defaultDependencyConfig: {
    breaker: { failureRateThreshold: 0.5, minimumCalls: 5, rollingWindowSize: 10, openDurationMs: 5000, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 },
    bulkhead: { maxConcurrent: 20 },
  },
});

// Optionally give a specific dependency its own thresholds:
mesh.configureDependency('billing-api', {
  breaker: { failureRateThreshold: 0.3, minimumCalls: 10, rollingWindowSize: 20, openDurationMs: 10_000, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 2 },
  bulkhead: { maxConcurrent: 10, maxQueue: 50 },
});

// A non-idempotent call: never retried, by default.
const charge = await mesh.wrapCall('billing-api', () => billingClient.charge(orderId));

// An idempotent GET: explicitly opted in to jittered retry.
const account = await mesh.wrapCall(
  'billing-api',
  () => billingClient.getAccount(accountId),
  { retry: { retryable: true, maxAttempts: 4, baseDelayMs: 50, capDelayMs: 2000 }, timeoutMs: 2000 },
);
```

### Illustrative NestJS consumption (not a dependency of this package)

```typescript
// billing.service.ts -- a plain injectable that owns a Mesh instance.
// circuit-breaker-mesh has no idea NestJS exists; this is just normal DI.
import { Injectable } from '@nestjs/common';
import { createMesh, type Mesh } from 'circuit-breaker-mesh';

@Injectable()
export class BillingService {
  private readonly mesh: Mesh = createMesh({
    defaultDependencyConfig: { bulkhead: { maxConcurrent: 20 } },
  });

  async getAccount(accountId: string) {
    return this.mesh.wrapCall(
      'billing-api',
      () => this.httpClient.get(`/accounts/${accountId}`),
      { retry: { retryable: true, maxAttempts: 3 } },
    );
  }
}
```

## Real output

All numbers below are from actually running `npm run demo` and
`npm test` in this repo — not illustrative/hand-written.

### `npm run demo`

```
=== 1. Circuit breaker: open on a failing dependency, recover once it heals ===
  attempt 1: failed  breaker=closed
  attempt 2: failed  breaker=closed
  attempt 3: failed  breaker=open
  breaker is now open -- further calls fail fast, without touching the dependency:
  attempt 4: rejected locally (CircuitOpenError), server calls stayed at 3 (was 3)
  dependency recovers; waiting for open->half_open...
  breaker=half_open
  half-open trial: succeeded  breaker=closed

=== 2. Bulkhead isolation: a saturated failing dependency must not starve a healthy one ===
  baseline healthy-svc throughput (alone): 405.0 calls/sec
  healthy-svc throughput while failing-svc is saturated in the background: 400.0 calls/sec
  retained 99% of baseline throughput

=== 3. Retry storm: decorrelated jitter vs. naive exponential backoff ===
  300 clients all fail simultaneously at t=0 and retry 4 times each:
  naive backoff:       worst 20ms window contains 300/300 clients (fully synchronized)
  decorrelated jitter: worst 20ms window contains 12/300 clients (spread out)

done.
```

### `npm test`

```
[bulkhead isolation] baseline healthy throughput:   433.3 calls/sec (520 completed)
[bulkhead isolation] concurrent healthy throughput: 391.7 calls/sec (470 completed)
[bulkhead isolation] flaky dependency attempts while saturated: 65, calls reaching bulkhead: 65
[retry-storm] naive backoff:       mean=750.0ms stddev=0.0ms peak-bucket=300/300 clients within a single 20ms window
[retry-storm] decorrelated jitter: mean=1024.3ms stddev=562.6ms peak-bucket=12/300 clients within a single 20ms window

ℹ tests 28
ℹ pass 28
ℹ fail 0
ℹ duration_ms 3155.1
```

The bulkhead-isolation test's core assertion: concurrent healthy-dependency
throughput stayed at ~90% of its standalone baseline while a separate
dependency was continuously saturating its own 5-slot bulkhead in the
background (65 calls processed through it, each hung for 120ms before
failing) — proof the isolation is real, not just that nothing crashed.

The retry-storm numbers: with 300 clients failing at the same instant,
naive backoff put all 300 in the exact same 20ms window on their 4th retry
(stddev of arrival time: 0ms — perfectly synchronized). Decorrelated jitter
spread the same 300 clients enough that the worst 20ms window only ever
held 12 of them (stddev: ~560ms).

## Testing

- `test/circuitBreaker.test.ts`, `test/bulkhead.test.ts`, `test/retry.test.ts`,
  `test/wrapCall.test.ts` — unit tests for each component in isolation.
- `test/chaos-scenarios.test.ts` — three chaos scenarios against the real
  fault-injecting HTTP server: a slow dependency (latency past the client
  timeout), an error-storm dependency (100% error rate), and an
  intermittent dependency (partial error rate), each asserting real breaker
  state transitions (`closed` → `open` → `half_open` → `closed`/`open`
  again) and that an open breaker stops sending the dependency any more
  traffic.
- `test/bulkhead-isolation.test.ts` — measures real throughput (calls/sec)
  to a healthy dependency with and without a separate failing dependency
  saturating its own bulkhead concurrently, asserting the healthy
  dependency's throughput stays within a generous margin of its baseline.
- `test/retry-storm.test.ts` — statistical comparison of decorrelated
  jitter vs. naive exponential backoff arrival-time distributions across
  300 simulated simultaneous clients (seeded PRNG, reproducible).

## License

MIT — see [LICENSE](./LICENSE).
