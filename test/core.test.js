import test from 'node:test';
import assert from 'node:assert/strict';

import { DeadLetterQueue } from '../src/index.js';

// A deterministic clock: each call advances by 1000ms.
function fakeClock() {
  let t = 0;
  return () => {
    t += 1000;
    return t;
  };
}

test('add stores an entry with defaults and a frozen shape', () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add({ kind: 'order' }, { reason: 'timeout' });
  assert.equal(entry.id, 1);
  assert.deepEqual(entry.payload, { kind: 'order' });
  assert.equal(entry.attempts, 1);
  assert.equal(entry.maxRetries, 3);
  assert.equal(entry.reason, 'timeout');
  assert.equal(entry.enqueuedAt, 1000);
  assert.ok(Object.isFrozen(entry));
  assert.equal(q.size(), 1);
});

test('add rejects invalid attempts and maxRetries', () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  assert.throws(() => q.add('x', { attempts: -1 }), TypeError);
  assert.throws(() => q.add('x', { attempts: 1.5 }), TypeError);
  assert.throws(() => q.add('x', { maxRetries: -1 }), TypeError);
  assert.throws(() => q.add('x', { maxRetries: 2.5 }), TypeError);
});

test('constructor rejects invalid options', () => {
  assert.throws(() => new DeadLetterQueue({ clock: 42 }), TypeError);
  assert.throws(() => new DeadLetterQueue({ maxRetries: -1 }), TypeError);
  assert.throws(() => new DeadLetterQueue({ maxRetries: 1.5 }), TypeError);
});

test('list returns a copy and preserves insertion order', () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const a = q.add('a');
  const b = q.add('b');
  const listed = q.list();
  assert.equal(listed.length, 2);
  assert.equal(listed[0].id, a.id);
  assert.equal(listed[1].id, b.id);
  // Mutating the returned array must not affect the queue.
  listed.pop();
  assert.equal(q.size(), 2);
});

test('get returns the entry or undefined', () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add('payload');
  assert.equal(q.get(entry.id), entry);
  assert.equal(q.get(999), undefined);
});

test('remove returns true only when an entry existed', () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add('payload');
  assert.equal(q.remove(entry.id), true);
  assert.equal(q.remove(entry.id), false);
  assert.equal(q.size(), 0);
});

test('replay removes the entry on success and returns the handler result', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add('payload');
  const result = await q.replay(entry.id, (payload, e) => {
    assert.equal(payload, 'payload');
    assert.equal(e.id, entry.id);
    return 'ok';
  });
  assert.equal(result, 'ok');
  assert.equal(q.size(), 0);
});

test('replay keeps the entry and bumps attempts on failure', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add('payload', { attempts: 1 });
  await assert.rejects(
    q.replay(entry.id, () => {
      throw new Error('boom');
    }),
    /boom/,
  );
  const after = q.get(entry.id);
  assert.ok(after);
  assert.equal(after.attempts, 2);
  assert.equal(q.size(), 1);
  // The original reference must not have been mutated.
  assert.equal(entry.attempts, 1);
});

test('replay propagates async rejections and still bumps attempts', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const entry = q.add('payload');
  await assert.rejects(
    q.replay(entry.id, async () => {
      throw new Error('async boom');
    }),
    /async boom/,
  );
  assert.equal(q.get(entry.id).attempts, 2);
});

test('replay rejects for unknown id and non-function handler', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  await assert.rejects(q.replay(1, () => {}), /no entry with id 1/);
  q.add('x');
  await assert.rejects(q.replay(1, 'not a function'), TypeError);
});

test('replayAll drains eligible entries and reports failed and skipped', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  const ok = q.add('ok', { maxRetries: 2 });
  const bad = q.add('bad', { maxRetries: 2 });
  const exhausted = q.add('exhausted', { attempts: 3, maxRetries: 3 });

  const report = await q.replayAll((payload) => {
    if (payload === 'bad') throw new Error('nope');
    return payload;
  });

  assert.deepEqual(report.replayed, [ok.id]);
  assert.equal(report.failed.length, 1);
  assert.equal(report.failed[0].id, bad.id);
  assert.match(report.failed[0].error.message, /nope/);
  assert.deepEqual(report.skipped, [exhausted.id]);

  // ok removed, bad still present with bumped attempts, exhausted untouched.
  assert.equal(q.get(ok.id), undefined);
  assert.equal(q.get(bad.id).attempts, 2);
  assert.equal(q.get(exhausted.id).attempts, 3);
});

test('replayAll does not process entries added during the pass', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  q.add('first');
  const report = await q.replayAll(() => {
    q.add('late');
  });
  assert.equal(report.replayed.length, 1);
  assert.equal(q.size(), 1);
  assert.equal(q.list()[0].payload, 'late');
});

test('replayAll rejects a non-function handler', async () => {
  const q = new DeadLetterQueue({ clock: fakeClock() });
  await assert.rejects(q.replayAll(null), TypeError);
});
