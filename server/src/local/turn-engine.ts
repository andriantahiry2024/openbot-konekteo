/**
 * The platform client a headless turn needs, built from this deployment's own parts.
 *
 * `toolkit/run-turn.ts` drives every turn nobody is watching — a routine firing, an approval
 * continuing, a Bot handing work to another Bot, the responsibility engine, the proactive pass. It
 * asks for exactly five methods (`IntelligenceLike`) and nothing else, which is why a deployment
 * with no platform can still run those turns: this adapter answers all five from the runner and the
 * lock the deployment already owns.
 *
 * WHAT IS HONEST HERE.
 *
 * - `getOrCreateThread` cannot fail because there is nothing to create: a local thread exists from
 *   the moment it has a run, and `run-turn` only reads the answer to know the thread was admitted.
 * - `getThreadMessages` answers with what the runner holds. The in-memory runner holds nothing
 *   across a restart; the Postgres one will, and then a hop reads the same history a person does.
 * - The lock is the local one, which is the same lock the browser's runs take. A turn taken while
 *   somebody is typing in that conversation is refused with the code `run-turn` already recognises
 *   as "wait and try again" rather than as a failure.
 */
import { ThreadAlreadyRunningError } from "@copilotkit/runtime/v2";
import type { IntelligenceLike } from "@openbot/toolkit/run-turn";
import type { ThreadLock } from "@openbot/toolkit/ports";

/** What the adapter reads a thread's messages through, when the runner can answer at all. */
type MessageReader = {
  getThreadMessages?: (
    threadId: string,
    scope: { ownerId: string },
  ) => { messages: unknown[] } | unknown[];
};

export function createLocalTurnEngine(options: {
  runner: MessageReader;
  lock: ThreadLock;
}): IntelligenceLike {
  const { runner, lock } = options;

  return {
    async getOrCreateThread({ threadId }) {
      /* A local thread is created by its first run, not by this call: there is no row to insert and
         no owner to record before something runs. Answered rather than refused because the caller
         treats a refusal as "this turn cannot start". */
      return { id: threadId };
    },

    async getThreadMessages({ threadId, userId }) {
      const read = runner.getThreadMessages?.(threadId, { ownerId: userId });
      const messages = Array.isArray(read) ? read : (read as { messages?: unknown[] } | undefined)?.messages ?? [];
      return { messages: messages as never };
    },

    async ɵacquireThreadLock({ threadId, runId, userId, agentId }) {
      const held = await lock.acquire({ threadId, runId, userId, agentId });
      /* The same class the runtime throws, so a caller matching on `code` treats this exactly as it
         treats the platform's own refusal. A plain Error here would read as a failure and be
         retried as one. */
      if (!held) throw new ThreadAlreadyRunningError();
      return { runId: held.runId };
    },

    async ɵrenewThreadLock({ threadId, runId }) {
      await lock.renew({ threadId, runId });
    },

    async ɵcleanupThreadLock({ threadId, runId }) {
      await lock.release({ threadId, runId });
    },
  };
}
