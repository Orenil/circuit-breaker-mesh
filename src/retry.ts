/**
 * Retry-with-jitter, opt-in only: a caller must explicitly mark a call as
 * `retryable: true` because retries are only safe for idempotent /
 * safe-to-retry operations. wrapCall never retries by default.
 *
 * Uses "decorrelated jitter" (the AWS Architecture Blog formula), not
 * exponential backoff and not "full jitter". Full jitter and exponential
 * backoff both re-derive each delay from the fixed sequence base*2^n
 * (optionally randomized independently each time), which means many
 * clients that started retrying at the same moment stay loosely
 * synchronized around the same delay values -- a retry storm against
 * the downstream. Decorrelated jitter instead derives each delay from
 * the *previous* delay:
 *
 *   sleep = min(cap, random_between(base, prev_sleep * 3))
 *
 * Because each client's next delay depends on its own random history,
 * client delay sequences fan out over time instead of clustering,
 * which is exactly what retry-storm avoidance requires. See
 * `test/retry-storm.test.ts` for a real statistical comparison against
 * naive exponential backoff.
 */

export interface RetryOptions {
  /** Required opt-in: only set this for operations safe to retry (idempotent). */
  retryable: true;
  /** Total attempts including the first, default 3. */
  maxAttempts?: number;
  /** Base delay in ms, default 50. */
  baseDelayMs?: number;
  /** Delay cap in ms, default 2000. */
  capDelayMs?: number;
  /** Predicate deciding whether a given error should be retried. Defaults to always retry. */
  isRetryableError?: (err: unknown) => boolean;
}

/** Decorrelated jitter: sleep = min(cap, random_between(base, prevSleep * 3)). */
export function decorrelatedJitterDelay(
  prevDelayMs: number,
  baseDelayMs: number,
  capDelayMs: number,
  rand: () => number = Math.random,
): number {
  const upper = Math.max(baseDelayMs, prevDelayMs * 3);
  const jittered = baseDelayMs + rand() * (upper - baseDelayMs);
  return Math.min(capDelayMs, jittered);
}

/** Plain exponential backoff, no jitter at all: base * 2^attempt, capped. Deterministic on purpose --
 *  used only as the "naive" baseline that the retry-storm test contrasts against decorrelated jitter. */
export function naiveExponentialBackoffDelay(attemptIndex: number, baseDelayMs: number, capDelayMs: number): number {
  return Math.min(capDelayMs, baseDelayMs * 2 ** attemptIndex);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
