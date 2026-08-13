/**
 * Per-dependency circuit breaker.
 *
 * State machine: closed -> open -> half_open -> (closed | open)
 *
 * "closed": calls flow normally. Outcomes are recorded into a rolling
 *   window of the last `rollingWindowSize` calls. Once at least
 *   `minimumCalls` outcomes are in the window, if the failure rate is
 *   >= `failureRateThreshold` the breaker opens.
 *
 * "open": calls are rejected immediately (fail fast) without touching the
 *   downstream dependency, for `openDurationMs`.
 *
 * "half_open": after the open duration elapses, a limited number of trial
 *   calls (`halfOpenMaxCalls` concurrently) are allowed through to probe
 *   whether the dependency has recovered. Any failure during a trial call
 *   reopens the breaker immediately. Enough successes
 *   (`halfOpenSuccessThreshold`) closes it again.
 */

export type BreakerState = 'closed' | 'open' | 'half_open';

export interface BreakerConfig {
  /** Failure rate (0..1) within the rolling window that trips the breaker. */
  failureRateThreshold: number;
  /** Minimum number of calls in the window before the failure rate is evaluated. */
  minimumCalls: number;
  /** Size of the rolling window of recent call outcomes. */
  rollingWindowSize: number;
  /** How long the breaker stays open before allowing a half-open trial. */
  openDurationMs: number;
  /** Number of concurrent trial calls permitted while half-open. */
  halfOpenMaxCalls: number;
  /** Number of successful trial calls needed to close the breaker again. */
  halfOpenSuccessThreshold: number;
}

export const DEFAULT_BREAKER_CONFIG: BreakerConfig = {
  failureRateThreshold: 0.5,
  minimumCalls: 5,
  rollingWindowSize: 10,
  openDurationMs: 5000,
  halfOpenMaxCalls: 1,
  halfOpenSuccessThreshold: 1,
};

export class CircuitBreaker {
  private state: BreakerState = 'closed';
  private window: boolean[] = [];
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private halfOpenSuccesses = 0;

  constructor(
    public readonly name: string,
    private readonly config: BreakerConfig,
  ) {}

  /** Current state, lazily transitioning open -> half_open once the timer elapses. */
  getState(): BreakerState {
    this.maybeExpireOpen();
    return this.state;
  }

  /** Whether a new attempt is allowed to proceed right now. */
  canAttempt(): boolean {
    const state = this.getState();
    if (state === 'closed') return true;
    if (state === 'open') return false;
    return this.halfOpenInFlight < this.config.halfOpenMaxCalls;
  }

  /** Call once an attempt is granted (after canAttempt() returned true) and is about to run. */
  onAttemptStart(): void {
    if (this.state === 'half_open') this.halfOpenInFlight++;
  }

  /** Call if an attempt was aborted before reaching the dependency (e.g. bulkhead rejected it). */
  onAttemptAbort(): void {
    if (this.state === 'half_open') this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
  }

  recordSuccess(): void {
    if (this.state === 'half_open') {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.config.halfOpenSuccessThreshold) this.close();
      return;
    }
    this.pushOutcome(true);
    this.evaluateWindow();
  }

  recordFailure(): void {
    if (this.state === 'half_open') {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
      this.open();
      return;
    }
    this.pushOutcome(false);
    this.evaluateWindow();
  }

  private maybeExpireOpen(): void {
    if (this.state === 'open' && Date.now() - this.openedAt >= this.config.openDurationMs) {
      this.state = 'half_open';
      this.halfOpenInFlight = 0;
      this.halfOpenSuccesses = 0;
    }
  }

  private pushOutcome(success: boolean): void {
    this.window.push(success);
    if (this.window.length > this.config.rollingWindowSize) this.window.shift();
  }

  private evaluateWindow(): void {
    if (this.state !== 'closed') return;
    if (this.window.length < this.config.minimumCalls) return;
    const failures = this.window.filter((ok) => !ok).length;
    const rate = failures / this.window.length;
    if (rate >= this.config.failureRateThreshold) this.open();
  }

  private open(): void {
    this.state = 'open';
    this.openedAt = Date.now();
    this.window = [];
    this.halfOpenInFlight = 0;
    this.halfOpenSuccesses = 0;
  }

  private close(): void {
    this.state = 'closed';
    this.window = [];
    this.halfOpenInFlight = 0;
    this.halfOpenSuccesses = 0;
  }
}

export class CircuitOpenError extends Error {
  constructor(public readonly dependencyName: string) {
    super(`circuit open for dependency "${dependencyName}"`);
    this.name = 'CircuitOpenError';
  }
}
