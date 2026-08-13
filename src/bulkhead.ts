/**
 * A bulkhead is a counting semaphore that caps how many calls may be
 * in flight concurrently for a given scope (a single dependency, or the
 * whole mesh). Calls beyond the concurrency cap queue in FIFO order
 * unless `maxQueue` is set, in which case excess calls are rejected
 * immediately with BulkheadFullError instead of queueing indefinitely.
 *
 * This is what stops one saturated/failing dependency from starving
 * calls to a healthy dependency: each dependency gets its own Bulkhead
 * instance, so a backlog on one never consumes the concurrency budget
 * of another.
 */

export interface BulkheadConfig {
  /** Maximum number of concurrent in-flight calls permitted. */
  maxConcurrent: number;
  /** Maximum number of callers allowed to queue once maxConcurrent is reached. Unbounded if omitted. */
  maxQueue?: number;
}

export class BulkheadFullError extends Error {
  constructor(public readonly activeCount: number, public readonly queuedCount: number) {
    super(`bulkhead full: ${activeCount} active, ${queuedCount} queued`);
    this.name = 'BulkheadFullError';
  }
}

export type Release = () => void;

export class Bulkhead {
  private inFlight = 0;
  private readonly queue: Array<() => void> = [];

  constructor(private readonly config: BulkheadConfig) {}

  get activeCount(): number {
    return this.inFlight;
  }

  get queuedCount(): number {
    return this.queue.length;
  }

  /** Acquire a slot, resolving once one is available. Rejects immediately if maxQueue is exceeded. */
  acquire(): Promise<Release> {
    if (this.inFlight < this.config.maxConcurrent) {
      this.inFlight++;
      return Promise.resolve(() => this.release());
    }
    if (this.config.maxQueue !== undefined && this.queue.length >= this.config.maxQueue) {
      return Promise.reject(new BulkheadFullError(this.inFlight, this.queue.length));
    }
    return new Promise<Release>((resolve) => {
      this.queue.push(() => {
        this.inFlight++;
        resolve(() => this.release());
      });
    });
  }

  private release(): void {
    this.inFlight--;
    const next = this.queue.shift();
    if (next) next();
  }
}
