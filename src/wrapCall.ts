import { Bulkhead, BulkheadConfig, BulkheadFullError } from './bulkhead.js';
import { BreakerConfig, CircuitBreaker, CircuitOpenError, DEFAULT_BREAKER_CONFIG, BreakerState } from './circuitBreaker.js';
import { RetryOptions, decorrelatedJitterDelay, sleep } from './retry.js';

export { CircuitOpenError, BulkheadFullError };
export type { BreakerState };

export class CallTimeoutError extends Error {
  constructor(public readonly timeoutMs: number) {
    super(`call timed out after ${timeoutMs}ms`);
    this.name = 'CallTimeoutError';
  }
}

export interface DependencyConfig {
  breaker?: Partial<BreakerConfig>;
  bulkhead?: BulkheadConfig;
}

export interface MeshConfig {
  /** Concurrency cap shared across *all* dependencies, independent of each dependency's own bulkhead. */
  globalMaxConcurrent?: number;
  /** Default per-dependency config applied to any dependency not explicitly configured. */
  defaultDependencyConfig?: DependencyConfig;
}

export interface CallOptions {
  /** Opt-in retry. Omit or pass `false` to never retry (the default -- safe for non-idempotent calls). */
  retry?: RetryOptions | false;
  /** Optional wall-clock timeout applied to each individual attempt. */
  timeoutMs?: number;
}

export interface DependencyStats {
  calls: number;
  successes: number;
  failures: number;
  /** Calls rejected without reaching the dependency: breaker open or bulkhead full. */
  rejections: number;
}

interface DependencyEntry {
  breaker: CircuitBreaker;
  bulkhead: Bulkhead;
  stats: DependencyStats;
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new CallTimeoutError(ms)), ms);
    promise.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

const DEFAULT_BULKHEAD_CONFIG: BulkheadConfig = { maxConcurrent: 10 };

/**
 * A Mesh owns independent circuit-breaker + bulkhead state per downstream
 * dependency name, plus one shared global bulkhead. Create one Mesh per
 * process (or per logical group of dependencies) and reuse it -- state is
 * keyed by dependencyName, so calling wrapCall with the same name reuses
 * the same breaker/bulkhead across calls.
 */
export class Mesh {
  private readonly dependencies = new Map<string, DependencyEntry>();
  private readonly globalBulkhead: Bulkhead;

  constructor(private readonly config: MeshConfig = {}) {
    this.globalBulkhead = new Bulkhead({ maxConcurrent: config.globalMaxConcurrent ?? Number.MAX_SAFE_INTEGER });
  }

  /** Explicitly configure (or reconfigure) a dependency's breaker/bulkhead thresholds. */
  configureDependency(dependencyName: string, config: DependencyConfig): void {
    this.dependencies.set(dependencyName, {
      breaker: new CircuitBreaker(dependencyName, { ...DEFAULT_BREAKER_CONFIG, ...config.breaker }),
      bulkhead: new Bulkhead(config.bulkhead ?? DEFAULT_BULKHEAD_CONFIG),
      stats: { calls: 0, successes: 0, failures: 0, rejections: 0 },
    });
  }

  private getOrCreateEntry(dependencyName: string): DependencyEntry {
    let entry = this.dependencies.get(dependencyName);
    if (!entry) {
      const defaults = this.config.defaultDependencyConfig ?? {};
      entry = {
        breaker: new CircuitBreaker(dependencyName, { ...DEFAULT_BREAKER_CONFIG, ...defaults.breaker }),
        bulkhead: new Bulkhead(defaults.bulkhead ?? DEFAULT_BULKHEAD_CONFIG),
        stats: { calls: 0, successes: 0, failures: 0, rejections: 0 },
      };
      this.dependencies.set(dependencyName, entry);
    }
    return entry;
  }

  getBreakerState(dependencyName: string): BreakerState {
    return this.getOrCreateEntry(dependencyName).breaker.getState();
  }

  getStats(dependencyName: string): DependencyStats {
    return { ...this.getOrCreateEntry(dependencyName).stats };
  }

  getBulkheadSnapshot(dependencyName: string): { activeCount: number; queuedCount: number } {
    const { bulkhead } = this.getOrCreateEntry(dependencyName);
    return { activeCount: bulkhead.activeCount, queuedCount: bulkhead.queuedCount };
  }

  /**
   * Run `fn` against `dependencyName` through the circuit breaker, both
   * bulkheads (per-dependency and global), and -- only if `options.retry`
   * is set -- decorrelated-jitter retry.
   */
  async wrapCall<T>(dependencyName: string, fn: () => Promise<T>, options: CallOptions = {}): Promise<T> {
    const entry = this.getOrCreateEntry(dependencyName);
    const retryOpts = options.retry || undefined;
    const maxAttempts = retryOpts ? Math.max(1, retryOpts.maxAttempts ?? 3) : 1;
    const baseDelayMs = retryOpts?.baseDelayMs ?? 50;
    const capDelayMs = retryOpts?.capDelayMs ?? 2000;
    let prevDelayMs = baseDelayMs;
    let lastError: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (!entry.breaker.canAttempt()) {
        entry.stats.rejections++;
        throw new CircuitOpenError(dependencyName);
      }
      entry.breaker.onAttemptStart();

      let releaseDep: (() => void) | undefined;
      let releaseGlobal: (() => void) | undefined;
      try {
        releaseDep = await entry.bulkhead.acquire();
        releaseGlobal = await this.globalBulkhead.acquire();
      } catch (err) {
        entry.breaker.onAttemptAbort();
        entry.stats.rejections++;
        releaseDep?.();
        throw err;
      }

      entry.stats.calls++;
      let outcome: { ok: true; value: T } | { ok: false; error: unknown };
      try {
        const value = options.timeoutMs ? await withTimeout(fn(), options.timeoutMs) : await fn();
        outcome = { ok: true, value };
      } catch (error) {
        outcome = { ok: false, error };
      } finally {
        releaseDep();
        releaseGlobal();
      }

      if (outcome.ok) {
        entry.breaker.recordSuccess();
        entry.stats.successes++;
        return outcome.value;
      }

      entry.breaker.recordFailure();
      entry.stats.failures++;
      lastError = outcome.error;

      const isLastAttempt = attempt >= maxAttempts;
      const errorIsRetryable = retryOpts ? (retryOpts.isRetryableError ? retryOpts.isRetryableError(outcome.error) : true) : false;
      if (!retryOpts || isLastAttempt || !errorIsRetryable) {
        throw outcome.error;
      }

      const delay = decorrelatedJitterDelay(prevDelayMs, baseDelayMs, capDelayMs);
      prevDelayMs = delay;
      await sleep(delay);
    }

    throw lastError;
  }
}

export function createMesh(config?: MeshConfig): Mesh {
  return new Mesh(config);
}
