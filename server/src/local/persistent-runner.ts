/**
 * Threads that survive a restart, with no platform behind them.
 *
 * WHAT THIS IS. `CopilotKit`'s open runtime asks a runner for four live operations (`run`,
 * `connect`, `isRunning`, `stop`) and — when the runner declares `ɵsupportsLocalThreadEndpoints` —
 * for six synchronous reads the HTTP layer serves as `GET /threads` and friends. The live four are
 * delegated to the in-memory runner, which already implements them correctly (interruption,
 * replay, concurrency). The durable part is here: every run's events and messages are written to a
 * SQLite file as they finish, and the six reads answer from it.
 *
 * WHY SQLITE AND NOT POSTGRES. The endpoint interface is synchronous — `listThreads` returns an
 * array, not a promise — and `bun:sqlite` is the one durable store with a synchronous API in this
 * process. OpenBot's own tables stay in Postgres; this file is only the runtime's thread store, and
 * a single file on a volume is enough for one replica.
 *
 * WHAT IT DOES NOT DO. Two replicas sharing one file would be two writers on one SQLite file; a
 * multi-replica deployment needs the Postgres runner this replaces, behind the same interface.
 */
import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { BaseEvent, Message } from "@ag-ui/client";
import { Observable } from "rxjs";
import {
  AgentRunner,
  type AgentRunnerConnectRequest,
  type AgentRunnerIsRunningRequest,
  type AgentRunnerRunRequest,
  type AgentRunnerStopRequest,
  InMemoryAgentRunner,
  type LocalThreadEndpointRecord,
  type LocalThreadEndpointRunner,
  type LocalThreadEndpointScope,
  ɵSERVER_THREAD_OWNER,
} from "@copilotkit/runtime/v2";

type ThreadRow = {
  id: string;
  name: string | null;
  agent_id: string;
  owner_id: string;
  archived: number;
  created_at: string;
  updated_at: string;
};

/** The event the run handler always emits first, and the one that carries the user's messages. */
type RunStartedEvent = {
  type: "RUN_STARTED";
  threadId: string;
  runId: string;
  input?: { messages?: Message[] };
  [key: string]: unknown;
};

export function createPersistentRunner(options: {
  /** The SQLite file. Its directory is created if missing. */
  path: string;
  /** The live runner. In-memory by default, and the only part of this file that runs agents. */
  live?: AgentRunner;
}): AgentRunner & LocalThreadEndpointRunner {
  mkdirSync(dirname(options.path), { recursive: true });
  const db = new Database(options.path, { create: true });
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec(`
    CREATE TABLE IF NOT EXISTS threads (
      id TEXT PRIMARY KEY,
      name TEXT,
      agent_id TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      archived INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
      thread_id TEXT NOT NULL,
      id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      json TEXT NOT NULL,
      PRIMARY KEY (thread_id, id)
    );
    CREATE TABLE IF NOT EXISTS events (
      thread_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      json TEXT NOT NULL,
      PRIMARY KEY (thread_id, run_id, seq)
    );
    CREATE TABLE IF NOT EXISTS runs (
      thread_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      state TEXT,
      created_at TEXT NOT NULL,
      PRIMARY KEY (thread_id, run_id)
    );
    CREATE INDEX IF NOT EXISTS messages_by_thread ON messages (thread_id, seq);
  `);

  const live = options.live ?? new InMemoryAgentRunner();
  /* The same object, typed for the reads it may or may not implement. */
  const liveRead = live as unknown as {
    listThreads?: (scope?: LocalThreadEndpointScope) => LocalThreadEndpointRecord[];
    getThreadMessages?: (threadId: string, scope?: LocalThreadEndpointScope) => Message[];
    getThreadEvents?: (threadId: string, scope?: LocalThreadEndpointScope) => BaseEvent[];
  };

  const ownerOf = (ownerId: string | undefined) => ɵresolveOwner(ownerId);

  const threadOf = (threadId: string): ThreadRow | undefined =>
    db
      .query<ThreadRow, [string]>("SELECT * FROM threads WHERE id = ?")
      .get(threadId) ?? undefined;

  const touchThread = (input: {
    threadId: string;
    agentId: string;
    ownerId: string;
  }): ThreadRow => {
    const now = new Date().toISOString();
    const existing = threadOf(input.threadId);
    if (existing) {
      db.run("UPDATE threads SET updated_at = ?, agent_id = ? WHERE id = ?", [
        now,
        input.agentId,
        input.threadId,
      ]);
      return { ...existing, updated_at: now, agent_id: input.agentId };
    }
    const row: ThreadRow = {
      id: input.threadId,
      name: null,
      agent_id: input.agentId,
      owner_id: input.ownerId,
      archived: 0,
      created_at: now,
      updated_at: now,
    };
    db.run(
      "INSERT INTO threads (id, name, agent_id, owner_id, archived, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [
        row.id,
        row.name,
        row.agent_id,
        row.owner_id,
        row.archived,
        row.created_at,
        row.updated_at,
      ],
    );
    return row;
  };

  /**
   * Persist one finished run: its events, the messages it carried and produced, and its state.
   *
   * WRITTEN ONCE, AT THE END. A run that is interrupted or refused writes what it had: a turn that
   * died mid-stream left the person's question in the thread, which is the part they would
   * otherwise have to type again.
   */
  const persist = (input: {
    threadId: string;
    runId: string;
    agentId: string;
    ownerId: string;
    events: BaseEvent[];
    state: unknown;
  }) => {
    const started = input.events.find(
      (event) => (event as { type?: string }).type === "RUN_STARTED",
    ) as RunStartedEvent | undefined;
    const history = started?.input?.messages ?? [];

    touchThread({
      threadId: input.threadId,
      agentId: input.agentId,
      ownerId: input.ownerId,
    });

    const insertEvent = db.prepare(
      "INSERT OR REPLACE INTO events (thread_id, run_id, seq, json) VALUES (?, ?, ?, ?)",
    );
    input.events.forEach((event, index) => {
      insertEvent.run([
        input.threadId,
        input.runId,
        index,
        JSON.stringify(event),
      ] as never);
    });

    const insertMessage = db.prepare(
      "INSERT OR REPLACE INTO messages (thread_id, id, seq, json) VALUES (?, ?, ?, ?)",
    );
    const nextSeq = (): number => {
      const row = db
        .query<{ n: number | null }, [string]>(
          "SELECT MAX(seq) AS n FROM messages WHERE thread_id = ?",
        )
        .get(input.threadId);
      return (row?.n ?? -1) + 1;
    };
    for (const message of history) {
      if (!message?.id) continue;
      insertMessage.run([
        input.threadId,
        message.id,
        nextSeq(),
        JSON.stringify(message),
      ] as never);
    }
    /* The assistant's own words, rebuilt from the stream. Nothing here invents content: a message
       exists because TEXT_MESSAGE_START opened it and its deltas were appended to it. */
    const assistant: { id: string; text: string }[] = [];
    for (const event of input.events) {
      const typed = event as {
        type?: string;
        messageId?: string;
        delta?: string;
      };
      if (typed.type === "TEXT_MESSAGE_START" && typed.messageId) {
        assistant.push({ id: typed.messageId, text: "" });
      }
      if (typed.type === "TEXT_MESSAGE_CONTENT" && typed.messageId) {
        const current = assistant.find((entry) => entry.id === typed.messageId);
        if (current) current.text += typed.delta ?? "";
      }
    }
    for (const message of assistant) {
      insertMessage.run([
        input.threadId,
        message.id,
        nextSeq(),
        JSON.stringify({ id: message.id, role: "assistant", content: message.text }),
      ] as never);
    }

    db.run(
      "INSERT OR REPLACE INTO runs (thread_id, run_id, state, created_at) VALUES (?, ?, ?, ?)",
      [
        input.threadId,
        input.runId,
        input.state === undefined ? null : JSON.stringify(input.state),
        new Date().toISOString(),
      ],
    );
  };

  const runner = {
    ɵsupportsLocalThreadEndpoints: true as const,

    run(request: AgentRunnerRunRequest): Observable<BaseEvent> {
      /* A run always has an id: the handler sends one, and a direct caller without one gets a fresh
         one rather than a row keyed on `undefined`. */
      const runId = request.input.runId ?? crypto.randomUUID();
      return new Observable<BaseEvent>((subscriber) => {
        const events: BaseEvent[] = [];
        /*
         * WRITTEN WHEN THE RUN SAYS IT IS OVER, NOT WHEN THE SUBJECT COMPLETES.
         *
         * The in-memory runner hands back a replay subject that stays open after the run ends, so a
         * completion handler here never fires. The signal that matters is the event: `RUN_FINISHED`
         * or `RUN_ERROR` is the run saying it is done, and that is when the thread is written. The
         * teardown covers the other ending — a browser that left mid-stream — with whatever the run
         * had produced by then.
         */
        let persisted = false;
        const flush = () => {
          if (persisted) return;
          persisted = true;
          persist({
            threadId: request.threadId,
            runId,
            agentId: request.agent.agentId ?? "unknown",
            ownerId: ownerOf(request.ownerId),
            events,
            state: request.input.state,
          });
        };
        const subscription = live.run(request).subscribe({
          next: (event) => {
            events.push(event);
            subscriber.next(event);
            const type = (event as { type?: string }).type;
            if (type === "RUN_FINISHED" || type === "RUN_ERROR") flush();
          },
          error: (error) => {
            flush();
            subscriber.error(error);
          },
          complete: () => {
            flush();
            subscriber.complete();
          },
        });
        return () => {
          flush();
          subscription.unsubscribe();
        };
      });
    },

    connect(request: AgentRunnerConnectRequest): Observable<BaseEvent> {
      const stored = runner.getThreadEvents(request.threadId, {
        ownerId: ownerOf(request.ownerId),
      });
      if (stored.length > 0) return new Observable((s) => { for (const e of stored) s.next(e); s.complete(); });
      return live.connect(request);
    },

    isRunning(request: AgentRunnerIsRunningRequest): Promise<boolean> {
      return live.isRunning(request);
    },

    stop(request: AgentRunnerStopRequest): Promise<boolean | undefined> {
      return live.stop(request);
    },

    listThreads(scope?: LocalThreadEndpointScope): LocalThreadEndpointRecord[] {
      const owner = ownerOf(scope?.ownerId);
      const rows = db
        .query<ThreadRow, [string]>(
          "SELECT * FROM threads WHERE owner_id = ? AND archived = 0 ORDER BY updated_at DESC",
        )
        .all(owner);
      /* A thread whose first run is still going has not been written yet; it comes from the live
         runner so the list does not lose a conversation in the seconds after somebody opens it. */
      const liveThreads = liveRead.listThreads?.(scope) ?? [];
      const known = new Set(rows.map((row) => row.id));
      const merged = [
        ...liveThreads.filter((thread) => !known.has(thread.id)),
        ...rows.map((row) => ({
        id: row.id,
        name: row.name,
        agentId: row.agent_id,
        organizationId: "",
        createdById: row.owner_id,
        archived: row.archived === 1,
          createdAt: row.created_at,
          updatedAt: row.updated_at,
        })),
      ];
      return merged;
    },

    getThreadMessages(
      threadId: string,
      scope?: LocalThreadEndpointScope,
    ): Message[] {
      if (!runner.isThreadAccessible(threadId, { ownerId: ownerOf(scope?.ownerId) }))
        return [];
      /*
       * LIVE FIRST, THEN THE FILE. A run in flight holds its messages in the runner, and anything
       * reading history mid-run — the approval resume is the one that matters — needs to see the
       * tool call it is being asked about. Reading only the file was a real refusal: the call had
       * not been written yet, so the resume answered "that unanswered tool call is not saved".
       */
      const liveMessages = liveRead.getThreadMessages?.(threadId, scope) ?? [];
      if (liveMessages.length > 0) return liveMessages;
      const rows = db
        .query<{ json: string }, [string]>(
          "SELECT json FROM messages WHERE thread_id = ? ORDER BY seq ASC",
        )
        .all(threadId);
      return rows.map((row) => JSON.parse(row.json) as Message);
    },

    getThreadEvents(threadId: string, scope?: LocalThreadEndpointScope): BaseEvent[] {
      if (!runner.isThreadAccessible(threadId, { ownerId: ownerOf(scope?.ownerId) }))
        return [];
      const liveEvents = liveRead.getThreadEvents?.(threadId, scope) ?? [];
      if (liveEvents.length > 0) return liveEvents;
      const rows = db
        .query<{ json: string }, [string]>(
          "SELECT json FROM events WHERE thread_id = ? ORDER BY run_id ASC, seq ASC",
        )
        .all(threadId);
      return rows.map((row) => JSON.parse(row.json) as BaseEvent);
    },

    getThreadState(
      threadId: string,
      scope?: LocalThreadEndpointScope,
    ): Record<string, unknown> | null {
      if (!runner.isThreadAccessible(threadId, { ownerId: ownerOf(scope?.ownerId) }))
        return null;
      const row = db
        .query<{ state: string | null }, [string]>(
          "SELECT state FROM runs WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .get(threadId);
      return row?.state ? (JSON.parse(row.state) as Record<string, unknown>) : null;
    },

    clearThreads(scope?: LocalThreadEndpointScope): void {
      const owner = ownerOf(scope?.ownerId);
      /* The live runner holds the conversations this process ran; clearing only the file would
         leave them listed and readable until the next restart. */
      (live as unknown as { clearThreads?: (scope?: LocalThreadEndpointScope) => void })
        .clearThreads?.(scope);
      const threads = runner.listThreads({ ownerId: owner }).map((row) => row.id);
      for (const threadId of threads) {
        db.run("DELETE FROM messages WHERE thread_id = ?", [threadId]);
        db.run("DELETE FROM events WHERE thread_id = ?", [threadId]);
        db.run("DELETE FROM runs WHERE thread_id = ?", [threadId]);
        db.run("DELETE FROM threads WHERE id = ?", [threadId]);
      }
    },

    isThreadAccessible(threadId: string, scope: LocalThreadEndpointScope): boolean {
      const row = threadOf(threadId);
      /* A thread nobody has written yet is free to claim: the run is what creates it. */
      if (!row) return true;
      return row.owner_id === ownerOf(scope.ownerId);
    },

    /** The file this runner writes, for the boot line. */
    get storePath(): string {
      return options.path;
    },
  };

  return runner as unknown as AgentRunner & LocalThreadEndpointRunner;
}

/** The owner a call acts as, with the runtime's own rule for "no caller". */
function ɵresolveOwner(ownerId: string | undefined): string {
  return ownerId ?? ɵSERVER_THREAD_OWNER;
}
