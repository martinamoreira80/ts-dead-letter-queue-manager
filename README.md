# ts-dead-letter-queue-manager

A small in-memory dead letter queue that stores failed events with retry metadata and lets you inspect and manually replay them.

## Usage

```js
import { DeadLetterQueue } from './src/index.js';

const queue = new DeadLetterQueue({
  // Optional; defaults to Date.now. Injecting a clock keeps tests deterministic.
  clock: () => 1700000000000,
  maxRetries: 3,
});

const entry = queue.add({ orderId: 42 }, { reason: 'timeout' });

console.log(queue.list());        // all entries, frozen
console.log(queue.get(entry.id)); // one entry by id

await queue.replay(entry.id, async (payload, e) => {
  // do the work again; throw or reject to keep the entry queued
  return 'replayed';
});

queue.remove(entry.id);           // manual removal
console.log(queue.size());
```

The exported name is `DeadLetterQueue`. Its methods are `add`, `list`, `get`, `remove`, `size`, `replay`, and `replayAll`.

## Why this exists

When an event handler fails, the event is often lost. A dead letter queue keeps the failed event plus metadata (attempts, retry budget, reason) so an operator can inspect it and decide whether to replay it. The trade-off here is that the queue is in-memory and single-process: it is deliberately small and has no persistence layer, no background workers, and no scheduling. That makes it easy to embed and easy to test, but it means you are responsible for durability and for calling `replay` at the right time.

## Edge you will hit

`replay` never removes an entry when the handler fails; it increments `attempts` instead. That is the point of a dead letter queue, but it means a handler that throws forever will keep the entry around until you remove it or its `maxRetries` budget is exhausted and `replayAll` starts skipping it. `replayAll` only processes the entries that existed when the call started, so entries added by the handler during the pass wait for the next call.

## Performance

The window keeps a bounded buffer, so `push` is constant time and memory does not
grow with the length of the stream. `peak` and `trough` are linear in the window
size, which is the trade that keeps `push` cheap.

## Limitations

Values are coerced to floats, so very large integers lose precision. If you need
exact integer aggregates over a window, this is the wrong tool.

