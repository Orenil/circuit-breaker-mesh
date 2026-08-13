import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createMesh, CircuitOpenError, CallTimeoutError } from '../src/index.js';

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('wrapCall returns the value on success and records stats', async () => {
  const mesh = createMesh();
  const result = await mesh.wrapCall('svc', async () => 42);
  assert.equal(result, 42);
  const stats = mesh.getStats('svc');
  assert.equal(stats.successes, 1);
  assert.equal(stats.failures, 0);
});

test('wrapCall never retries by default, even on failure', async () => {
  const mesh = createMesh();
  let calls = 0;
  await assert.rejects(
    mesh.wrapCall('svc', async () => {
      calls++;
      throw new Error('boom');
    }),
  );
  assert.equal(calls, 1, 'no retry option supplied -> exactly one attempt');
});

test('wrapCall retries only when explicitly marked retryable, using jittered delay', async () => {
  const mesh = createMesh();
  let calls = 0;
  const result = await mesh.wrapCall(
    'svc',
    async () => {
      calls++;
      if (calls < 3) throw new Error('transient');
      return 'ok';
    },
    { retry: { retryable: true, maxAttempts: 5, baseDelayMs: 5, capDelayMs: 50 } },
  );
  assert.equal(result, 'ok');
  assert.equal(calls, 3);
});

test('wrapCall gives up after maxAttempts and throws the last error', async () => {
  const mesh = createMesh();
  let calls = 0;
  await assert.rejects(
    mesh.wrapCall(
      'svc',
      async () => {
        calls++;
        throw new Error(`fail ${calls}`);
      },
      { retry: { retryable: true, maxAttempts: 3, baseDelayMs: 1, capDelayMs: 10 } },
    ),
    /fail 3/,
  );
  assert.equal(calls, 3);
});

test('isRetryableError predicate can stop a retry early', async () => {
  const mesh = createMesh();
  let calls = 0;
  await assert.rejects(
    mesh.wrapCall(
      'svc',
      async () => {
        calls++;
        throw new Error('not retryable');
      },
      {
        retry: { retryable: true, maxAttempts: 5, baseDelayMs: 1, isRetryableError: () => false },
      },
    ),
  );
  assert.equal(calls, 1);
});

test('open breaker rejects immediately with CircuitOpenError, without invoking fn', async () => {
  const mesh = createMesh({ defaultDependencyConfig: { breaker: { minimumCalls: 2, failureRateThreshold: 0.5 } } });
  let calls = 0;
  const failing = async () => {
    calls++;
    throw new Error('down');
  };
  await assert.rejects(mesh.wrapCall('svc', failing));
  await assert.rejects(mesh.wrapCall('svc', failing));
  assert.equal(mesh.getBreakerState('svc'), 'open');

  calls = 0;
  await assert.rejects(mesh.wrapCall('svc', failing), CircuitOpenError);
  assert.equal(calls, 0, 'breaker should short-circuit before fn is called');
});

test('per-dependency bulkhead caps concurrency independent of a busy dependency', async () => {
  const mesh = createMesh({
    defaultDependencyConfig: { bulkhead: { maxConcurrent: 2 } },
  });
  let peakConcurrent = 0;
  let current = 0;
  const slow = async () => {
    current++;
    peakConcurrent = Math.max(peakConcurrent, current);
    await sleep(30);
    current--;
    return 'ok';
  };
  await Promise.all([mesh.wrapCall('svc', slow), mesh.wrapCall('svc', slow), mesh.wrapCall('svc', slow), mesh.wrapCall('svc', slow)]);
  assert.ok(peakConcurrent <= 2, `expected peak concurrency <= 2, got ${peakConcurrent}`);
});

test('timeoutMs option rejects a call that runs too long', async () => {
  const mesh = createMesh();
  await assert.rejects(
    mesh.wrapCall(
      'svc',
      async () => {
        await sleep(200);
        return 'too late';
      },
      { timeoutMs: 20 },
    ),
    CallTimeoutError,
  );
});
