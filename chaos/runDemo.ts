import { createMesh, CircuitOpenError } from '../src/index.js';
import { decorrelatedJitterDelay, naiveExponentialBackoffDelay } from '../src/retry.js';
import { FaultInjectingServer, callRoute } from './fakeServer.js';

/**
 * Runnable, human-readable demo of the three chaos scenarios plus the
 * jitter comparison, using the real library against the real fake HTTP
 * server (no mocking of breaker/bulkhead/retry logic). This is what the
 * README's "usage example" output is taken from -- run it yourself with
 * `npm run demo`.
 */

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

async function demoBreakerRecovery(server: FaultInjectingServer): Promise<void> {
  console.log('\n=== 1. Circuit breaker: open on a failing dependency, recover once it heals ===');
  server.configureRoute('/flaky-svc', { errorRate: 1 });

  const mesh = createMesh({
    defaultDependencyConfig: {
      breaker: { failureRateThreshold: 0.5, minimumCalls: 3, rollingWindowSize: 6, openDurationMs: 300, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 },
    },
  });
  const call = () => mesh.wrapCall('payments-api', () => callRoute(server.baseUrl, '/flaky-svc', 200));

  for (let i = 0; i < 3; i++) {
    const ok = await call().then(() => true).catch(() => false);
    console.log(`  attempt ${i + 1}: ${ok ? 'succeeded' : 'failed'}  breaker=${mesh.getBreakerState('payments-api')}`);
  }

  console.log('  breaker is now open -- further calls fail fast, without touching the dependency:');
  const statsBefore = server.getStats('/flaky-svc').total;
  await call().catch((err) => console.log(`  attempt 4: rejected locally (${err instanceof CircuitOpenError ? 'CircuitOpenError' : err.constructor.name}), server calls stayed at ${server.getStats('/flaky-svc').total} (was ${statsBefore})`));

  console.log('  dependency recovers; waiting for open->half_open...');
  server.configureRoute('/flaky-svc', { errorRate: 0 });
  await sleep(330);
  console.log(`  breaker=${mesh.getBreakerState('payments-api')}`);
  const recovered = await call().then(() => true).catch(() => false);
  console.log(`  half-open trial: ${recovered ? 'succeeded' : 'failed'}  breaker=${mesh.getBreakerState('payments-api')}`);
}

async function demoBulkheadIsolation(server: FaultInjectingServer): Promise<void> {
  console.log('\n=== 2. Bulkhead isolation: a saturated failing dependency must not starve a healthy one ===');
  server.configureRoute('/healthy-svc', { latencyMs: 8 });
  server.configureRoute('/failing-svc', { latencyMs: 60_000 }); // hangs well past the client timeout below

  const mesh = createMesh({ globalMaxConcurrent: 1000 });
  const neverTrip = { minimumCalls: 1_000_000, failureRateThreshold: 1, rollingWindowSize: 10, openDurationMs: 1, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 };
  mesh.configureDependency('healthy-svc', { breaker: neverTrip, bulkhead: { maxConcurrent: 5 } });
  mesh.configureDependency('failing-svc', { breaker: neverTrip, bulkhead: { maxConcurrent: 5 } });

  const windowMs = 1000;
  async function drive(dep: string, path: string, timeoutMs: number, workers: number): Promise<number> {
    const end = Date.now() + windowMs;
    let completed = 0;
    await Promise.all(
      Array.from({ length: workers }, async () => {
        while (Date.now() < end) {
          try {
            await mesh.wrapCall(dep, () => callRoute(server.baseUrl, path, timeoutMs));
            completed++;
          } catch {
            /* ignore for throughput counting */
          }
        }
      }),
    );
    return completed;
  }

  const baseline = await drive('healthy-svc', '/healthy-svc', 1000, 20);
  const baselineThroughput = baseline / (windowMs / 1000);
  console.log(`  baseline healthy-svc throughput (alone): ${baselineThroughput.toFixed(1)} calls/sec`);

  const [concurrent] = await Promise.all([drive('healthy-svc', '/healthy-svc', 1000, 20), drive('failing-svc', '/failing-svc', 120, 20)]);
  const concurrentThroughput = concurrent / (windowMs / 1000);
  console.log(`  healthy-svc throughput while failing-svc is saturated in the background: ${concurrentThroughput.toFixed(1)} calls/sec`);
  console.log(`  retained ${((concurrentThroughput / baselineThroughput) * 100).toFixed(0)}% of baseline throughput`);
}

function demoJitterComparison(): void {
  console.log('\n=== 3. Retry storm: decorrelated jitter vs. naive exponential backoff ===');
  const CLIENT_COUNT = 300;
  const BASE = 50;
  const CAP = 2000;
  const ATTEMPTS = 4;
  const BUCKET_MS = 20;

  const naiveArrivals: number[] = [];
  const jitteredArrivals: number[] = [];
  for (let client = 0; client < CLIENT_COUNT; client++) {
    let naiveSum = 0;
    for (let a = 0; a < ATTEMPTS; a++) naiveSum += naiveExponentialBackoffDelay(a, BASE, CAP);
    naiveArrivals.push(naiveSum);

    const rand = mulberry32(client + 1);
    let prev = BASE;
    let jitteredSum = 0;
    for (let a = 0; a < ATTEMPTS; a++) {
      const d = decorrelatedJitterDelay(prev, BASE, CAP, rand);
      jitteredSum += d;
      prev = d;
    }
    jitteredArrivals.push(jitteredSum);
  }

  const peak = (values: number[]) => {
    const counts = new Map<number, number>();
    for (const v of values) counts.set(Math.floor(v / BUCKET_MS), (counts.get(Math.floor(v / BUCKET_MS)) ?? 0) + 1);
    return Math.max(...counts.values());
  };

  console.log(`  ${CLIENT_COUNT} clients all fail simultaneously at t=0 and retry ${ATTEMPTS} times each:`);
  console.log(`  naive backoff:       worst ${BUCKET_MS}ms window contains ${peak(naiveArrivals)}/${CLIENT_COUNT} clients (fully synchronized)`);
  console.log(`  decorrelated jitter: worst ${BUCKET_MS}ms window contains ${peak(jitteredArrivals)}/${CLIENT_COUNT} clients (spread out)`);
}

async function main(): Promise<void> {
  const server = new FaultInjectingServer();
  await server.start();
  try {
    await demoBreakerRecovery(server);
    await demoBulkheadIsolation(server);
    demoJitterComparison();
  } finally {
    await server.stop();
  }
  console.log('\ndone.');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
