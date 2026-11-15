// Public entry point for ts-dead-letter-queue-manager.
//
// Only the queue class is exported. Keeping the surface small makes the
// contract easy to test and easy to reason about.

export { DeadLetterQueue } from './core.js';
