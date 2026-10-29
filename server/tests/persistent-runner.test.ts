import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Observable } from "rxjs";
import { InMemoryAgentRunner } from "@copilotkit/runtime/v2";
import { createPersistentRunner } from "../src/local/persistent-runner";

const dir = mkdtempSync(join(tmpdir(), "openbot-threads-"));
const file = join(dir, "threads.sqlite");

afterAll(() => rmSync(dir, { recursive: true, force: true }));

/** An agent that streams one answer and stops, as a real one does. */
const fakeAgent = {
  agentId: "general-assistant",
  messages: [],
  state: {},
  clone() {
    return fakeAgent;
  },
  connect() {
    return new Observable(() => {});
  },
  abortRun() {},
  addMessage() {},
  setMessages() {},
  subscribe() {
    return { unsubscribe() {} };
  },
  async runAgent(input: { threadId: string; runId: string }, { onEvent }: { onEvent: (e: { event: unknown }) => void }) {
    for (const event of [
      { type: "RUN_STARTED", threadId: input.threadId, runId: input.runId },
      { type: "TEXT_MESSAGE_START", messageId: "a1", role: "assistant" },
      {
        type: "TEXT_MESSAGE_CONTENT",
        messageId: "a1",
        delta: "bonjour du futur",
      },
      { type: "TEXT_MESSAGE_END", messageId: "a1" },
      { type: "RUN_FINISHED", threadId: input.threadId, runId: input.runId },
    ]) {
      onEvent({ event });
    }
  },
} as never;

function drain(runner: ReturnType<typeof createPersistentRunner>, threadId: string) {
  const seen: string[] = [];
  return new Promise<void>((resolve, reject) => {
    runner
      .run({
        threadId,
        ownerId: "user-1",
        agent: fakeAgent,
        input: {
          threadId,
          runId: "run-1",
          messages: [{ id: "u1", role: "user", content: "ecris bonjour" }],
          tools: [],
          context: [],
          state: {},
        },
      } as never)
      .subscribe({
        next: (event) => seen.push((event as { type: string }).type),
        error: reject,
        complete: () => resolve(),
      });
    // The in-memory runner's subject stays open; the run is over when RUN_FINISHED arrived.
    setTimeout(resolve, 50);
  }).then(() => seen);
}

describe("the persistent runner", () => {
  test("writes the thread, its messages and its events when a run finishes", async () => {
    const runner = createPersistentRunner({ path: file });
    const owner = { ownerId: "user-1" };

    const seen = await drain(runner, "thread-1");
    expect(seen).toContain("RUN_FINISHED");

    const threads = runner.listThreads(owner);
    expect(threads.map((t) => t.id)).toContain("thread-1");

    const messages = runner.getThreadMessages("thread-1", owner);
    expect(messages.map((m) => m.role)).toEqual(["user", "assistant"]);
    expect(messages[1]?.content).toBe("bonjour du futur");

    expect(runner.getThreadEvents("thread-1", owner).length).toBeGreaterThan(0);
  });

  test("a second process — a restart — sees the same thread", async () => {
    const restarted = createPersistentRunner({ path: file });
    const owner = { ownerId: "user-1" };

    const threads = restarted.listThreads(owner);
    expect(threads.map((t) => t.id)).toContain("thread-1");

    const messages = restarted.getThreadMessages("thread-1", owner);
    expect(messages[1]?.content).toBe("bonjour du futur");
  });

  test("another person does not see it, and cannot read it", async () => {
    const runner = createPersistentRunner({ path: file });
    const stranger = { ownerId: "user-2" };

    expect(runner.listThreads(stranger)).toHaveLength(0);
    expect(runner.getThreadMessages("thread-1", stranger)).toHaveLength(0);
    expect(runner.isThreadAccessible("thread-1", stranger)).toBe(false);
  });

  test("clearing removes only that person's threads", async () => {
    const runner = createPersistentRunner({ path: file });
    runner.clearThreads({ ownerId: "user-1" });

    expect(runner.listThreads({ ownerId: "user-1" })).toHaveLength(0);
  });
});
