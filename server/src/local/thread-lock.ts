/**
 * The conversation's run lock, kept by this deployment instead of by the platform.
 *
 * The port is `ThreadLock` in `@openbot/toolkit`. The platform's version asks Intelligence to issue
 * the lock and hands back the run id it named; with no platform there is nobody to ask, so this
 * module is the whole of the mechanism: one run per conversation, taken NX, expiring on its own.
 *
 * WHY IN THIS PROCESS AND NOT IN THE DATABASE, FOR NOW. A single-replica deployment cannot have two
 * runs on one conversation without this, which is the property that matters. It cannot promise what
 * a shared store promises, though: two replicas would each hand out a lock for the same
 * conversation. The Postgres-backed lock (`pg_advisory_xact_lock`, or a row with a TTL) replaces
 * this class when the runner moves to Postgres; the port does not change, and neither does any
 * caller.
 *
 * THE RUN ID IS THE CALLER'S. The platform handed back its own id because its gateway checked every
 * streamed event against it. Nothing downstream of this lock speaks to a gateway, so the id the
 * caller minted is the id the run keeps — and `acquire` still returns one, because the port says so
 * and a caller that ignored it would be right to.
 */
import type { ThreadLock } from "@openbot/toolkit/ports";

type Held = { runId: string; expiresAt: number };

/** How long a lock is held before it lapses on its own, matching the platform's own default. */
export const LOCAL_THREAD_LOCK_TTL_MS = 120_000;

export function createLocalThreadLock(options: {
  ttlMs?: number;
  now?: () => number;
} = {}): ThreadLock {
  const ttlMs = options.ttlMs ?? LOCAL_THREAD_LOCK_TTL_MS;
  const now = options.now ?? Date.now;
  const held = new Map<string, Held>();
  /* Every thread this replica is running in, so the map cannot grow without bound on a deployment
     that has seen one conversation per customer and never released one. */
  const sweep = (at: number) => {
    for (const [threadId, entry] of held) {
      if (entry.expiresAt <= at) held.delete(threadId);
    }
  };

  return {
    async acquire({ threadId, runId }) {
      const at = now();
      sweep(at);
      const current = held.get(threadId);
      if (current && current.expiresAt > at) return null;
      held.set(threadId, { runId, expiresAt: at + ttlMs });
      return { runId };
    },
    async renew({ threadId, runId }) {
      const current = held.get(threadId);
      /* A lock this replica no longer holds is not renewed and not re-taken: the run it belonged to
         has already been told it was running, and a renewed lock would hide that from the next
         caller. */
      if (!current || current.runId !== runId) return;
      held.set(threadId, { runId, expiresAt: now() + ttlMs });
    },
    async release({ threadId, runId }) {
      const current = held.get(threadId);
      if (current && current.runId === runId) held.delete(threadId);
    },
  };
}
