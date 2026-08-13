import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CircuitBreaker } from '../src/circuitBreaker.js';

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('stays closed while failure rate is below threshold', () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 4,
    rollingWindowSize: 10,
    openDurationMs: 1000,
    halfOpenMaxCalls: 1,
    halfOpenSuccessThreshold: 1,
  });
  breaker.recordSuccess();
  breaker.recordFailure();
  breaker.recordSuccess();
  breaker.recordSuccess();
  assert.equal(breaker.getState(), 'closed');
});

test('opens once failure rate reaches threshold with enough volume', () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 4,
    rollingWindowSize: 10,
    openDurationMs: 1000,
    halfOpenMaxCalls: 1,
    halfOpenSuccessThreshold: 1,
  });
  breaker.recordFailure();
  breaker.recordFailure();
  breaker.recordSuccess();
  assert.equal(breaker.getState(), 'closed', 'below minimumCalls, should not evaluate yet');
  breaker.recordFailure();
  assert.equal(breaker.getState(), 'open');
  assert.equal(breaker.canAttempt(), false);
});

test('does not open below minimumCalls even at 100% failure', () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 5,
    rollingWindowSize: 10,
    openDurationMs: 1000,
    halfOpenMaxCalls: 1,
    halfOpenSuccessThreshold: 1,
  });
  breaker.recordFailure();
  breaker.recordFailure();
  breaker.recordFailure();
  breaker.recordFailure();
  assert.equal(breaker.getState(), 'closed');
});

test('transitions open -> half_open after openDurationMs, then closes on trial success', async () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 2,
    rollingWindowSize: 10,
    openDurationMs: 60,
    halfOpenMaxCalls: 1,
    halfOpenSuccessThreshold: 1,
  });
  breaker.recordFailure();
  breaker.recordFailure();
  assert.equal(breaker.getState(), 'open');
  assert.equal(breaker.canAttempt(), false);

  await sleep(80);
  assert.equal(breaker.getState(), 'half_open');
  assert.equal(breaker.canAttempt(), true);

  breaker.onAttemptStart();
  breaker.recordSuccess();
  assert.equal(breaker.getState(), 'closed');
});

test('half_open trial failure reopens the breaker immediately', async () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 2,
    rollingWindowSize: 10,
    openDurationMs: 60,
    halfOpenMaxCalls: 1,
    halfOpenSuccessThreshold: 1,
  });
  breaker.recordFailure();
  breaker.recordFailure();
  await sleep(80);
  assert.equal(breaker.getState(), 'half_open');

  breaker.onAttemptStart();
  breaker.recordFailure();
  assert.equal(breaker.getState(), 'open');
  assert.equal(breaker.canAttempt(), false);
});

test('half_open only permits halfOpenMaxCalls concurrent trials', async () => {
  const breaker = new CircuitBreaker('svc', {
    failureRateThreshold: 0.5,
    minimumCalls: 2,
    rollingWindowSize: 10,
    openDurationMs: 30,
    halfOpenMaxCalls: 2,
    halfOpenSuccessThreshold: 3,
  });
  breaker.recordFailure();
  breaker.recordFailure();
  await sleep(50);

  assert.equal(breaker.canAttempt(), true);
  breaker.onAttemptStart();
  assert.equal(breaker.canAttempt(), true);
  breaker.onAttemptStart();
  assert.equal(breaker.canAttempt(), false, 'a 3rd concurrent trial should be refused');
});
