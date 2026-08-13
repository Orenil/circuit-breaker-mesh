import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createMesh, CircuitOpenError } from '../src/index.js';
import { FaultInjectingServer, callRoute } from '../chaos/fakeServer.js';

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const BREAKER_CONFIG = {
  failureRateThreshold: 0.5,
  minimumCalls: 3,
  rollingWindowSize: 6,
  openDurationMs: 200,
  halfOpenMaxCalls: 1,
  halfOpenSuccessThreshold: 1,
};

// -- Scenario 1: slow dependency -----------------------------------------
// A dependency whose latency exceeds the caller's timeout looks, from the
// caller's perspective, exactly like a dependency that is down: every call
// times out. The breaker should open on that failure pattern, then close
// again once the dependency's real latency drops back below the timeout.
test('chaos: slow dependency trips the breaker, then recovers through half_open on real latency', async () => {
  const server = new FaultInjectingServer();
  await server.start();
  server.configureRoute('/slow', { latencyMs: 400 });

  const mesh = createMesh({ defaultDependencyConfig: { breaker: BREAKER_CONFIG } });
  const callTimeoutMs = 60;
  const call = () => mesh.wrapCall('slow-dep', () => callRoute(server.baseUrl, '/slow', callTimeoutMs));

  assert.equal(mesh.getBreakerState('slow-dep'), 'closed');

  // Every call times out client-side (latency 400ms > timeout 60ms) -> breaker should open.
  for (let i = 0; i < BREAKER_CONFIG.minimumCalls; i++) {
    await assert.rejects(call());
  }
  assert.equal(mesh.getBreakerState('slow-dep'), 'open');

  // While open, calls fail fast and never reach the server.
  const statsBefore = server.getStats('/slow');
  await assert.rejects(call(), CircuitOpenError);
  const statsAfter = server.getStats('/slow');
  assert.equal(statsAfter.total, statsBefore.total, 'open breaker must short-circuit before hitting the server');

  // Dependency recovers: latency drops back under the client timeout.
  server.configureRoute('/slow', { latencyMs: 5 });
  await sleep(BREAKER_CONFIG.openDurationMs + 30);
  assert.equal(mesh.getBreakerState('slow-dep'), 'half_open');

  const result = await call();
  assert.deepEqual(result, { ok: true });
  assert.equal(mesh.getBreakerState('slow-dep'), 'closed', 'a successful half-open trial should close the breaker');

  await server.stop();
});

// -- Scenario 2: error-storm dependency ----------------------------------
// A dependency returning errors on every request (e.g. crash-looping,
// or rejecting due to its own overload) should trip the breaker almost
// immediately, and rejected calls thereafter must not add load to the
// already-struggling dependency.
test('chaos: error-storm dependency (100% error rate) trips the breaker and stops sending it traffic', async () => {
  const server = new FaultInjectingServer();
  await server.start();
  server.configureRoute('/storm', { errorRate: 1, errorStatus: 503 });

  const mesh = createMesh({ defaultDependencyConfig: { breaker: BREAKER_CONFIG } });
  const call = () => mesh.wrapCall('storm-dep', () => callRoute(server.baseUrl, '/storm', 200));

  for (let i = 0; i < BREAKER_CONFIG.minimumCalls; i++) {
    await assert.rejects(call());
  }
  assert.equal(mesh.getBreakerState('storm-dep'), 'open');

  const totalBeforeExtra = server.getStats('/storm').total;
  for (let i = 0; i < 5; i++) {
    await assert.rejects(call(), CircuitOpenError);
  }
  const totalAfterExtra = server.getStats('/storm').total;
  assert.equal(totalAfterExtra, totalBeforeExtra, 'an open breaker must stop sending additional requests to a dependency already returning errors');

  const stats = mesh.getStats('storm-dep');
  assert.ok(stats.rejections >= 5);

  await server.stop();
});

// -- Scenario 3: intermittent dependency ---------------------------------
// A flaky dependency (partial error rate) should still trip once its
// failure rate crosses the threshold, and a half-open trial against a
// still-flaky dependency should reopen the breaker rather than close it.
test('chaos: intermittent dependency trips on failure rate, and a failed half-open trial reopens it', async () => {
  const server = new FaultInjectingServer();
  await server.start();
  server.configureRoute('/flaky', { errorRate: 0.85 });

  const mesh = createMesh({ defaultDependencyConfig: { breaker: BREAKER_CONFIG } });
  const call = () => mesh.wrapCall('flaky-dep', () => callRoute(server.baseUrl, '/flaky', 200));

  // Drive enough volume that a >=50% failure-rate threshold is crossed.
  let opened = false;
  for (let i = 0; i < 20 && !opened; i++) {
    await call().catch(() => {});
    opened = mesh.getBreakerState('flaky-dep') === 'open';
  }
  assert.equal(opened, true, 'breaker should have opened under sustained intermittent failures');

  // Dependency is still flaky when the half-open trial happens -> expect it can reopen.
  await sleep(BREAKER_CONFIG.openDurationMs + 30);
  assert.equal(mesh.getBreakerState('flaky-dep'), 'half_open');
  // Force the trial itself to fail regardless of random luck, to deterministically assert the transition.
  server.configureRoute('/flaky', { errorRate: 1 });
  await call().catch(() => {});
  assert.equal(mesh.getBreakerState('flaky-dep'), 'open', 'a failed half-open trial must reopen the breaker');

  // Now let it genuinely recover and confirm it closes.
  server.configureRoute('/flaky', { errorRate: 0 });
  await sleep(BREAKER_CONFIG.openDurationMs + 30);
  assert.equal(mesh.getBreakerState('flaky-dep'), 'half_open');
  const result = await call();
  assert.deepEqual(result, { ok: true });
  assert.equal(mesh.getBreakerState('flaky-dep'), 'closed');

  await server.stop();
});
