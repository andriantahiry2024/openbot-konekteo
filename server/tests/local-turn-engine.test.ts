import { describe, expect, test } from "bun:test";
import { createLocalThreadLock } from "../src/local/thread-lock";
import { createLocalTurnEngine } from "../src/local/turn-engine";

const noRunner = {};

describe("the local turn engine", () => {
  test("a second turn in the same conversation is refused with the code run-turn matches on", async () => {
    const engine = createLocalTurnEngine({
      runner: noRunner,
      lock: createLocalThreadLock(),
    });

    await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r1",
      userId: "u1",
      agentId: "b1",
    });

    const refusal = engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r2",
      userId: "u1",
      agentId: "b1",
    });

    await expect(refusal).rejects.toMatchObject({
      code: "THREAD_ALREADY_RUNNING",
    });
  });

  test("the lock is handed back, so the next turn is admitted", async () => {
    const lock = createLocalThreadLock();
    const engine = createLocalTurnEngine({ runner: noRunner, lock });

    await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r1",
      userId: "u1",
      agentId: "b1",
    });
    await engine.ɵcleanupThreadLock({ threadId: "t1", runId: "r1" });

    const held = await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r2",
      userId: "u1",
      agentId: "b1",
    });

    expect(held).toEqual({ runId: "r2" });
  });

  test("a lock lapses on its own, so a process that died mid-run does not wedge the conversation", async () => {
    let now = 1_000;
    const lock = createLocalThreadLock({ ttlMs: 120_000, now: () => now });
    const engine = createLocalTurnEngine({ runner: noRunner, lock });

    await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r1",
      userId: "u1",
      agentId: "b1",
    });

    now += 120_001;

    const held = await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r2",
      userId: "u1",
      agentId: "b1",
    });

    expect(held).toEqual({ runId: "r2" });
  });

  test("a thread the runner cannot read answers with no messages rather than failing the turn", async () => {
    const engine = createLocalTurnEngine({
      runner: noRunner,
      lock: createLocalThreadLock(),
    });

    await expect(
      engine.getThreadMessages({ threadId: "unknown", userId: "u1" }),
    ).resolves.toEqual({ messages: [] });
  });

  test("a thread the runner can read answers with its messages", async () => {
    const engine = createLocalTurnEngine({
      runner: {
        getThreadMessages: () => [{ id: "m1", role: "user", content: "salut" }],
      },
      lock: createLocalThreadLock(),
    });

    const read = await engine.getThreadMessages({ threadId: "t1", userId: "u1" });

    expect(read.messages).toHaveLength(1);
  });

  test("renewing a lock this replica does not hold changes nothing", async () => {
    const lock = createLocalThreadLock();
    const engine = createLocalTurnEngine({ runner: noRunner, lock });

    await engine.ɵrenewThreadLock({ threadId: "t1", runId: "ghost", ttlSeconds: 120 });

    const held = await engine.ɵacquireThreadLock({
      threadId: "t1",
      runId: "r1",
      userId: "u1",
      agentId: "b1",
    });

    expect(held).toEqual({ runId: "r1" });
  });
});
