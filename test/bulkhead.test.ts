import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Bulkhead, BulkheadFullError } from '../src/bulkhead.js';

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('caps concurrent holders at maxConcurrent', async () => {
  const bulkhead = new Bulkhead({ maxConcurrent: 2 });
  const r1 = await bulkhead.acquire();
  const r2 = await bulkhead.acquire();
  assert.equal(bulkhead.activeCount, 2);

  let thirdAcquired = false;
  const p3 = bulkhead.acquire().then((release) => {
    thirdAcquired = true;
    release();
  });

  await sleep(20);
  assert.equal(thirdAcquired, false, 'third caller should still be queued');
  assert.equal(bulkhead.queuedCount, 1);

  r1();
  await p3;
  assert.equal(thirdAcquired, true, 'third caller unblocks once a slot frees up');
  r2();
});

test('queued acquires are granted in FIFO order', async () => {
  const bulkhead = new Bulkhead({ maxConcurrent: 1 });
  const release0 = await bulkhead.acquire();
  const order: number[] = [];

  const p1 = bulkhead.acquire().then((r) => {
    order.push(1);
    r();
  });
  const p2 = bulkhead.acquire().then((r) => {
    order.push(2);
    r();
  });
  const p3 = bulkhead.acquire().then((r) => {
    order.push(3);
    r();
  });

  release0();
  await Promise.all([p1, p2, p3]);
  assert.deepEqual(order, [1, 2, 3]);
});

test('rejects with BulkheadFullError once maxQueue is exceeded', async () => {
  const bulkhead = new Bulkhead({ maxConcurrent: 1, maxQueue: 1 });
  const release0 = await bulkhead.acquire();
  const queued = bulkhead.acquire(); // fills the 1-slot queue
  await assert.rejects(() => bulkhead.acquire(), BulkheadFullError);
  release0();
  const r = await queued;
  r();
});

test('a saturated bulkhead does not affect an independent bulkhead instance', async () => {
  const busy = new Bulkhead({ maxConcurrent: 1 });
  const free = new Bulkhead({ maxConcurrent: 1 });

  const releaseBusy = await busy.acquire();
  void busy.acquire(); // queues, keeps `busy` saturated

  const start = Date.now();
  const releaseFree = await free.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 20, `acquiring the independent bulkhead should be immediate, took ${elapsed}ms`);

  releaseFree();
  releaseBusy();
});
