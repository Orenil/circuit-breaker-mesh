import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createMesh } from '../src/index.js';
import { FaultInjectingServer, callRoute } from '../chaos/fakeServer.js';

/**
 * Proves the central bulkhead claim: a saturated, failing dependency must
 * not reduce throughput to a healthy dependency sharing the process, as
 * long as they have independent per-dependency bulkheads.
 *
 * Method: drive a fixed-duration burst of concurrent calls at the healthy
 * dependency alone (baseline throughput), then repeat the same burst while
 * a separate pool of workers simultaneously keeps the *failing* dependency
 * saturated for the whole window. Compare measured completions/sec.
 */

const WINDOW_MS = 1200;
const HEALTHY_BULKHEAD = 5;
const FLAKY_BULKHEAD = 5;
const FLAKY_CLIENT_TIMEOUT_MS = 120;

async function driveHealthyForWindow(
  mesh: ReturnType<typeof createMesh>,
  baseUrl: string,
  windowMs: number,
  workerCount: number,
): Promise<number> {
  const end = Date.now() + windowMs;
  let completed = 0;
  const workers = Array.from({ length: workerCount }, async () => {
    while (Date.now() < end) {
      try {
        await mesh.wrapCall('healthy-dep', () => callRoute(baseUrl, '/healthy', 1000));
        completed++;
      } catch {
        // ignore -- only counting successful completions towards throughput
      }
    }
  });
  await Promise.all(workers);
  return completed;
}

async function driveFlakyInBackground(
  mesh: ReturnType<typeof createMesh>,
  baseUrl: string,
  windowMs: number,
  workerCount: number,
): Promise<{ attempts: number }> {
  const end = Date.now() + windowMs;
  let attempts = 0;
  const workers = Array.from({ length: workerCount }, async () => {
    while (Date.now() < end) {
      attempts++;
      await mesh.wrapCall('flaky-dep', () => callRoute(baseUrl, '/flaky', FLAKY_CLIENT_TIMEOUT_MS)).catch(() => {});
    }
  });
  await Promise.all(workers);
  return { attempts };
}

test('bulkhead isolation: a saturated failing dependency does not reduce healthy-dependency throughput', async () => {
  const server = new FaultInjectingServer();
  await server.start();
  server.configureRoute('/healthy', { latencyMs: 8, errorRate: 0 });
  // Hangs well past its own client timeout -> every attempt occupies a bulkhead
  // slot for FLAKY_CLIENT_TIMEOUT_MS and then fails. Breaker for this dependency
  // is configured to never trip (huge minimumCalls) so the bulkhead itself --
  // not the breaker short-circuiting -- is what's under test here.
  server.configureRoute('/flaky', { latencyMs: 60_000 });

  const mesh = createMesh({ globalMaxConcurrent: 1000 });
  mesh.configureDependency('healthy-dep', {
    breaker: { minimumCalls: 1_000_000, failureRateThreshold: 1, rollingWindowSize: 10, openDurationMs: 1, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 },
    bulkhead: { maxConcurrent: HEALTHY_BULKHEAD },
  });
  mesh.configureDependency('flaky-dep', {
    breaker: { minimumCalls: 1_000_000, failureRateThreshold: 1, rollingWindowSize: 10, openDurationMs: 1, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 },
    bulkhead: { maxConcurrent: FLAKY_BULKHEAD },
  });

  // Baseline: healthy dependency alone, oversubscribed workers so it's bulkhead-bound.
  const baselineCompleted = await driveHealthyForWindow(mesh, server.baseUrl, WINDOW_MS, HEALTHY_BULKHEAD * 4);
  const baselineThroughput = baselineCompleted / (WINDOW_MS / 1000);

  // Reset stats-bearing dependencies for a clean second measurement window.
  mesh.configureDependency('healthy-dep', {
    breaker: { minimumCalls: 1_000_000, failureRateThreshold: 1, rollingWindowSize: 10, openDurationMs: 1, halfOpenMaxCalls: 1, halfOpenSuccessThreshold: 1 },
    bulkhead: { maxConcurrent: HEALTHY_BULKHEAD },
  });

  // Concurrent: flaky dependency kept saturated by its own oversubscribed worker pool,
  // at the same time as we re-measure healthy throughput.
  const [concurrentCompleted, flakyResult] = await Promise.all([
    driveHealthyForWindow(mesh, server.baseUrl, WINDOW_MS, HEALTHY_BULKHEAD * 4),
    driveFlakyInBackground(mesh, server.baseUrl, WINDOW_MS, FLAKY_BULKHEAD * 4),
  ]);
  const concurrentThroughput = concurrentCompleted / (WINDOW_MS / 1000);

  const flakyStats = mesh.getStats('flaky-dep');
  const healthyStats = mesh.getStats('healthy-dep');

  console.log(`[bulkhead isolation] baseline healthy throughput:   ${baselineThroughput.toFixed(1)} calls/sec (${baselineCompleted} completed)`);
  console.log(`[bulkhead isolation] concurrent healthy throughput: ${concurrentThroughput.toFixed(1)} calls/sec (${concurrentCompleted} completed)`);
  console.log(`[bulkhead isolation] flaky dependency attempts while saturated: ${flakyResult.attempts}, calls reaching bulkhead: ${flakyStats.calls}`);

  // The flaky pool must have actually saturated its own bulkhead (proof this was a
  // real concurrency-capped, slow-failing dependency, not a no-op).
  assert.ok(flakyStats.calls >= FLAKY_BULKHEAD * 3, `expected flaky dependency to process multiple full batches through its ${FLAKY_BULKHEAD}-slot bulkhead, got ${flakyStats.calls} calls`);

  // Core isolation assertion: healthy throughput under concurrent flaky saturation
  // must stay close to its standalone baseline (generous margin for scheduler jitter).
  assert.ok(
    concurrentThroughput >= baselineThroughput * 0.7,
    `expected concurrent healthy throughput (${concurrentThroughput.toFixed(1)}/s) to stay within 30% of baseline (${baselineThroughput.toFixed(1)}/s)`,
  );
  assert.equal(healthyStats.failures, 0, 'healthy dependency should not have failed any calls');

  await server.stop();
});
