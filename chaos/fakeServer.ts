import http from 'node:http';

/**
 * A real HTTP server (Node's built-in `http` module, no dependencies)
 * that can inject latency, error responses, and hangs (timeouts) per
 * route, reconfigurable at runtime. This is what the chaos scenarios
 * make real network calls against -- there is no mocking of the
 * circuit breaker / bulkhead / retry logic itself, only the downstream
 * behavior is simulated.
 */
export interface RouteConfig {
  /** Fixed artificial latency added before responding, in ms. */
  latencyMs?: number;
  /** Probability (0..1) that a given request gets an error response. */
  errorRate?: number;
  /** Probability (0..1) that a given request hangs forever (never responds), forcing the client to time out. */
  timeoutRate?: number;
  /** HTTP status used for injected errors. Defaults to 500. */
  errorStatus?: number;
}

export interface RouteStats {
  total: number;
  errors: number;
  timeouts: number;
  successes: number;
}

function emptyStats(): RouteStats {
  return { total: 0, errors: 0, timeouts: 0, successes: 0 };
}

export class FaultInjectingServer {
  private readonly routes = new Map<string, RouteConfig>();
  private readonly stats = new Map<string, RouteStats>();
  private server: http.Server | null = null;
  private _port = 0;

  get port(): number {
    return this._port;
  }

  get baseUrl(): string {
    return `http://127.0.0.1:${this._port}`;
  }

  /** (Re)configure fault injection for a route. Takes effect on the next request. */
  configureRoute(path: string, config: RouteConfig): void {
    this.routes.set(path, config);
    if (!this.stats.has(path)) this.stats.set(path, emptyStats());
  }

  getStats(path: string): RouteStats {
    return { ...(this.stats.get(path) ?? emptyStats()) };
  }

  resetStats(path: string): void {
    this.stats.set(path, emptyStats());
  }

  start(port = 0): Promise<number> {
    return new Promise((resolve) => {
      this.server = http.createServer((req, res) => this.handle(req, res));
      this.server.listen(port, '127.0.0.1', () => {
        const addr = this.server!.address();
        this._port = typeof addr === 'object' && addr ? addr.port : port;
        resolve(this._port);
      });
    });
  }

  stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.server) {
        resolve();
        return;
      }
      // Without this, idle keep-alive sockets left open by the fetch client
      // keep the server alive until Node's default 5s keepAliveTimeout, which
      // makes every test that stops a server pay a multi-second tax.
      this.server.closeAllConnections();
      this.server.close((err) => (err ? reject(err) : resolve()));
    });
  }

  private handle(req: http.IncomingMessage, res: http.ServerResponse): void {
    // Swallow write-after-abort errors: a client that gave up (its own timeout,
    // or a chaos scenario intentionally hanging past what the client will wait
    // for) can cause a still-pending setTimeout(respond) to fire against an
    // already-closed socket. That's expected under fault injection, not a bug.
    res.on('error', () => {});

    const path = (req.url ?? '/').split('?')[0];
    const config = this.routes.get(path);
    const stat = this.stats.get(path) ?? emptyStats();
    this.stats.set(path, stat);
    stat.total++;

    if (!config) {
      res.writeHead(404, { 'content-type': 'application/json' }).end(JSON.stringify({ ok: false, error: 'no route configured' }));
      return;
    }

    if (config.timeoutRate && Math.random() < config.timeoutRate) {
      stat.timeouts++;
      // Deliberately never respond -- the client-side timeout must fire.
      return;
    }

    const respond = () => {
      if (res.writableEnded || res.destroyed) return;
      if (config.errorRate && Math.random() < config.errorRate) {
        stat.errors++;
        res.writeHead(config.errorStatus ?? 500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: false, injected: true }));
        return;
      }
      stat.successes++;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    };

    if (config.latencyMs && config.latencyMs > 0) {
      const timer = setTimeout(respond, config.latencyMs);
      // If the client gives up (its own timeout aborts the socket) before the
      // injected latency elapses, the underlying connection closes early and
      // this timer would otherwise sit pending for the rest of latencyMs --
      // chaos scenarios deliberately configure latency far beyond the client
      // timeout (e.g. 60s), so left unchecked every aborted attempt holds a
      // live timer keeping the event loop (and the whole test process) alive
      // long after the test's assertions have finished. Clear it on abort.
      res.on('close', () => clearTimeout(timer));
    } else {
      respond();
    }
  }
}

/**
 * Client helper: calls a route on a FaultInjectingServer with its own
 * client-side timeout (via AbortController), so a hung/slow route
 * actually surfaces as a rejected promise instead of hanging the test
 * process.
 */
export async function callRoute(baseUrl: string, path: string, timeoutMs = 1000): Promise<{ ok: true }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('client-side timeout')), timeoutMs);
  try {
    const res = await fetch(`${baseUrl}${path}`, { signal: controller.signal });
    if (!res.ok) {
      throw new Error(`downstream returned HTTP ${res.status}`);
    }
    return (await res.json()) as { ok: true };
  } finally {
    clearTimeout(timer);
  }
}
