import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decorrelatedJitterDelay, naiveExponentialBackoffDelay } from '../src/retry.js';

/**
 * Retry-storm comparison: decorrelated jitter vs. naive (unjittered)
 * exponential backoff, measured as real arrival-time distributions rather
 * than asserting on the delay formula in isolation (that's covered by
 * retry.test.ts).
 *
 * Scenario: a downstream dependency blips and CLIENT_COUNT independent
 * callers all fail at the exact same instant, all beginning their retry
 * loop at t=0. For each strategy, compute every client's cumulative
 * arrival time (sum of its own delay sequence) at its TARGET_ATTEMPT-th
 * retry, then compare:
 *
 *  - stddev of arrival times across clients (spread)
 *  - the busiest single time bucket (the actual retry-storm spike a
 *    downstream would feel: how many clients hit it in the same instant)
 *
 * A seeded PRNG (mulberry32) is used instead of Math.random() so the
 * statistical assertions below are reproducible, not flaky.
 */

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

const CLIENT_COUNT = 300;
const BASE_DELAY_MS = 50;
const CAP_DELAY_MS = 2000;
const TARGET_ATTEMPT = 4;
const BUCKET_MS = 20;

function stddev(values: number[]): number {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  return Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length);
}

/** Largest number of arrivals that land in the same fixed-width time bucket -- the actual spike a downstream server would feel. */
function peakBucketCount(arrivals: number[], bucketMs: number): number {
  const counts = new Map<number, number>();
  for (const t of arrivals) {
    const bucket = Math.floor(t / bucketMs);
    counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
  }
  return Math.max(...counts.values());
}

test('retry-storm: decorrelated jitter spreads simultaneous retries out; naive backoff keeps every client synchronized', () => {
  const naiveArrivals: number[] = [];
  const jitteredArrivals: number[] = [];

  for (let client = 0; client < CLIENT_COUNT; client++) {
    // Naive exponential backoff (src/retry.ts) is fully deterministic --
    // base * 2^attempt with no randomization at all -- so every client
    // computes the bit-for-bit identical delay sequence.
    let naiveCumulative = 0;
    for (let attempt = 0; attempt < TARGET_ATTEMPT; attempt++) {
      naiveCumulative += naiveExponentialBackoffDelay(attempt, BASE_DELAY_MS, CAP_DELAY_MS);
    }
    naiveArrivals.push(naiveCumulative);

    // Decorrelated jitter: each client has its own independent random
    // draw history, feeding each delay back in as the next call's prevDelay.
    const rand = mulberry32(client + 1);
    let prev = BASE_DELAY_MS;
    let jitteredCumulative = 0;
    for (let attempt = 0; attempt < TARGET_ATTEMPT; attempt++) {
      const delay = decorrelatedJitterDelay(prev, BASE_DELAY_MS, CAP_DELAY_MS, rand);
      jitteredCumulative += delay;
      prev = delay;
    }
    jitteredArrivals.push(jitteredCumulative);
  }

  const naiveStd = stddev(naiveArrivals);
  const jitteredStd = stddev(jitteredArrivals);
  const naivePeak = peakBucketCount(naiveArrivals, BUCKET_MS);
  const jitteredPeak = peakBucketCount(jitteredArrivals, BUCKET_MS);
  const mean = (values: number[]) => values.reduce((a, b) => a + b, 0) / values.length;

  console.log(
    `[retry-storm] naive backoff:       mean=${mean(naiveArrivals).toFixed(1)}ms stddev=${naiveStd.toFixed(1)}ms ` +
      `peak-bucket=${naivePeak}/${CLIENT_COUNT} clients within a single ${BUCKET_MS}ms window`,
  );
  console.log(
    `[retry-storm] decorrelated jitter: mean=${mean(jitteredArrivals).toFixed(1)}ms stddev=${jitteredStd.toFixed(1)}ms ` +
      `peak-bucket=${jitteredPeak}/${CLIENT_COUNT} clients within a single ${BUCKET_MS}ms window`,
  );

  // Naive backoff has zero randomness, so every client's Nth-attempt arrival
  // time is identical -- literally the whole client population lands in one
  // bucket. This is the retry storm the decorrelated-jitter design exists to avoid.
  assert.equal(naiveStd, 0, 'naive backoff has zero randomness, so all clients should arrive at exactly the same instant');
  assert.equal(naivePeak, CLIENT_COUNT, 'naive backoff synchronizes every client into a single instant');

  // Decorrelated jitter must meaningfully spread the same client population out.
  assert.ok(jitteredStd > naiveStd, `expected jitter to add real spread, got stddev=${jitteredStd}`);
  assert.ok(
    jitteredPeak < CLIENT_COUNT * 0.1,
    `expected decorrelated jitter's worst single ${BUCKET_MS}ms bucket to hold well under 10% of clients, got ${jitteredPeak}/${CLIENT_COUNT}`,
  );
});
