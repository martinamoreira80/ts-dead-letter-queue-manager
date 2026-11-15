// Core implementation of the dead letter queue manager.
//
// Design decisions:
// - A dead letter queue stores failed events together with retry metadata so
//   they can be inspected and manually replayed. We keep the queue as an
//   append-only array of entries. Removal happens only through explicit
//   "remove" or "replay" operations; inspection never mutates state.
// - Time is injected via a `clock` function (defaulting to `Date.now`). This
//   keeps tests deterministic and avoids any dependency on wall-clock time.
// - Each entry is frozen on creation so callers cannot mutate retry metadata
//   by accident. Re-queueing an entry creates a new frozen entry instead.
// - The `replay` method accepts a handler that may be synchronous or return a
//   promise. It always returns a promise so callers have a single contract.

export class DeadLetterQueue {
  /**
   * @param {Object} [options]
   * @param {() => number} [options.clock] Function returning current time in ms.
   *   Injected so tests never depend on the real clock.
   * @param {number} [options.maxRetries] Default retry budget for entries.
   */
  constructor(options = {}) {
    const { clock = Date.now, maxRetries = 3 } = options;
    if (typeof clock !== 'function') {
      throw new TypeError('clock must be a function');
    }
    if (!Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new TypeError('maxRetries must be a non-negative integer');
    }
    this._clock = clock;
    this._maxRetries = maxRetries;
    this._entries = [];
    this._nextId = 1;
  }

  /**
   * Add a failed event to the queue.
   *
   * @param {*} payload The event payload. Stored as-is; callers own its shape.
   * @param {Object} [meta]
   * @param {number} [meta.attempts] How many times the event has already been
   *   attempted. Defaults to 1 because the event just failed once.
   * @param {number} [meta.maxRetries] Per-entry retry budget. Falls back to the
   *   queue default.
   * @param {string} [meta.reason] Human-readable failure reason.
   * @returns {Readonly<Object>} The stored entry.
   */
  add(payload, meta = {}) {
    const {
      attempts = 1,
      maxRetries = this._maxRetries,
      reason = '',
    } = meta;
    if (!Number.isInteger(attempts) || attempts < 0) {
      throw new TypeError('attempts must be a non-negative integer');
    }
    if (!Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new TypeError('maxRetries must be a non-negative integer');
    }
    const entry = Object.freeze({
      id: this._nextId++,
      payload,
      attempts,
      maxRetries,
      reason,
      enqueuedAt: this._clock(),
    });
    this._entries.push(entry);
    return entry;
  }

  /**
   * Return a shallow copy of all entries in insertion order. The entries
   * themselves are frozen, so the copy is safe to hand out.
   *
   * @returns {Readonly<Object>[]}
   */
  list() {
    return this._entries.slice();
  }

  /**
   * Find a single entry by id.
   *
   * @param {number} id
   * @returns {Readonly<Object>|undefined}
   */
  get(id) {
    return this._entries.find((e) => e.id === id);
  }

  /**
   * Remove an entry by id. Returns true if an entry was removed.
   *
   * @param {number} id
   * @returns {boolean}
   */
  remove(id) {
    const index = this._entries.findIndex((e) => e.id === id);
    if (index === -1) return false;
    this._entries.splice(index, 1);
    return true;
  }

  /**
   * Number of entries currently queued.
   *
   * @returns {number}
   */
  size() {
    return this._entries.length;
  }

  /**
   * Manually replay an entry through a handler.
   *
   * The handler receives the entry's payload and the entry itself. If it
   * resolves, the entry is removed from the queue and the result is returned.
   * If it rejects (or throws), the entry stays in the queue and its `attempts`
   * counter is incremented by one; the rejection reason is propagated.
   *
   * This method never removes an entry on failure, which is the whole point of
   * a dead letter queue: failed replays remain inspectable.
   *
   * @param {number} id
   * @param {(payload: *, entry: Readonly<Object>) => any} handler
   * @returns {Promise<*>}
   */
  async replay(id, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('handler must be a function');
    }
    const index = this._entries.findIndex((e) => e.id === id);
    if (index === -1) {
      throw new Error(`no entry with id ${id}`);
    }
    const entry = this._entries[index];
    try {
      const result = await handler(entry.payload, entry);
      this._entries.splice(index, 1);
      return result;
    } catch (err) {
      // Replace the entry with an updated frozen copy so callers holding a
      // reference to the old entry do not see mutated state.
      const updated = Object.freeze({
        ...entry,
        attempts: entry.attempts + 1,
      });
      this._entries[index] = updated;
      throw err;
    }
  }

  /**
   * Replay every entry that still has retry budget left, in insertion order.
   *
   * Entries whose `attempts` already meet or exceed `maxRetries` are skipped
   * and reported in `skipped`. This gives callers a single call to drain the
   * queue without blowing past per-entry budgets.
   *
   * @param {(payload: *, entry: Readonly<Object>) => any} handler
   * @returns {Promise<{replayed: number[], failed: {id: number, error: *}[],
   *   skipped: number[]}>}
   */
  async replayAll(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('handler must be a function');
    }
    const replayed = [];
    const failed = [];
    const skipped = [];
    // Snapshot ids so that entries added during replay are not processed in
    // the same pass. This keeps the operation bounded and predictable.
    const ids = this._entries.map((e) => e.id);
    for (const id of ids) {
      const entry = this.get(id);
      if (!entry) continue;
      if (entry.attempts >= entry.maxRetries) {
        skipped.push(id);
        continue;
      }
      try {
        await this.replay(id, handler);
        replayed.push(id);
      } catch (error) {
        failed.push({ id, error });
      }
    }
    return { replayed, failed, skipped };
  }
}
