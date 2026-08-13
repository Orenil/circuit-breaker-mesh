import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decorrelatedJitterDelay, naiveExponentialBackoffDelay } from '../src/retry.js';

test('decorrelated jitter delay is always within [base, cap]', () => {
  let prev = 50;
  for (let i = 0; i < 200; i++) {
    const delay = decorrelatedJitterDelay(prev, 50, 2000);
    assert.ok(delay >= 50, `delay ${delay} should be >= base`);
    assert.ok(delay <= 2000, `delay ${delay} should be <= cap`);
    prev = delay;
  }
});

test('decorrelated jitter respects the cap even with a large previous delay', () => {
  const delay = decorrelatedJitterDelay(10_000, 50, 2000, () => 0.999999);
  assert.ok(delay <= 2000);
});

test('decorrelated jitter formula matches sleep = min(cap, random_between(base, prev*3))', () => {
  // rand() pinned to 0 -> lower bound of random_between == base
  assert.equal(decorrelatedJitterDelay(100, 50, 2000, () => 0), 50);
  // rand() pinned to 1 -> upper bound of random_between == prev*3 (below cap)
  assert.equal(decorrelatedJitterDelay(100, 50, 2000, () => 1), 300);
  // prev*3 below base -> upper bound floors at base
  assert.equal(decorrelatedJitterDelay(1, 50, 2000, () => 1), 50);
});

test('naive exponential backoff is deterministic and doubles per attempt, capped', () => {
  assert.equal(naiveExponentialBackoffDelay(0, 50, 10_000), 50);
  assert.equal(naiveExponentialBackoffDelay(1, 50, 10_000), 100);
  assert.equal(naiveExponentialBackoffDelay(2, 50, 10_000), 200);
  assert.equal(naiveExponentialBackoffDelay(3, 50, 10_000), 400);
  assert.equal(naiveExponentialBackoffDelay(10, 50, 10_000), 10_000, 'should be capped');
});

test('decorrelated jitter produces varied delays across independent callers (not synchronized)', () => {
  // Model 30 independent callers each computing their *first* retry delay
  // from the same starting state (prevDelayMs == baseDelayMs) -- this is
  // the realistic case, since a fresh wrapCall retry loop always starts
  // there. Note: this deliberately does NOT chain one continuously-growing
  // sequence for 30 iterations -- decorrelated jitter's upper bound grows
  // ~3x per step, so a long single chain saturates at capDelayMs within a
  // handful of steps and legitimately starts repeating the exact cap value,
  // which is correct clamping behavior, not a lack of variety.
  const delays = new Set<number>();
  for (let i = 0; i < 30; i++) {
    delays.add(decorrelatedJitterDelay(50, 50, 2000));
  }
  assert.ok(delays.size > 25, `expected high variety of delays, got ${delays.size} distinct values out of 30`);
});
