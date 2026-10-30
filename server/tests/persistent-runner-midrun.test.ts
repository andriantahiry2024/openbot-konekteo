import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Observable } from "rxjs";
import { createPersistentRunner } from "../src/local/persistent-runner";

/**
 * LE DÉFAUT QUE CE FICHIER EMPÊCHE DE REVENIR.
 *
 * Une action de bot qui demande une approbation est reprise par le serveur, et cette reprise relit
 * l'historique PENDANT le tour pour retrouver l'appel d'outil concerné (`savedToolCall`). Ni le
 * runner mémoire ni le fichier ne publiaient les messages avant la fin du run : la reprise ne
 * trouvait rien, et répondait « that unanswered tool call is not saved in this conversation ».
 * Cinq clics refusés d'affilée, sans que rien ne parle du navigateur.
 */
const dir = mkdtempSync(join(tmpdir(), "openbot-midrun-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe("le runner persistant pendant un run", () => {
  test("l'appel d'outil est lisible au milieu du tour, dans la forme que l'approbation attend", async () => {
    const runner = createPersistentRunner({ path: join(dir, "threads.sqlite") });
    let callsMidRun: { id: string; name: string; args: string }[] = [];
    let answered: boolean | undefined;

    const agent = {
      agentId: "general-assistant",
      messages: [],
      state: {},
      clone() {
        return agent;
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
      async runAgent(
        input: { threadId: string; runId: string },
        { onEvent }: { onEvent: (e: { event: unknown }) => void },
      ) {
        onEvent({
          event: { type: "RUN_STARTED", threadId: input.threadId, runId: input.runId },
        });
        onEvent({
          event: {
            type: "TOOL_CALL_START",
            toolCallId: "call-1",
            toolCallName: "computer_click",
            parentMessageId: "msg-1",
          },
        });
        onEvent({
          event: { type: "TOOL_CALL_ARGS", toolCallId: "call-1", delta: '{"x":1}' },
        });
        onEvent({ event: { type: "TOOL_CALL_END", toolCallId: "call-1" } });
        /* Laisser le runner relayer les événements avant de lire, comme le fait un vrai run. */
        await new Promise((r) => setTimeout(r, 60));

        /* C'est ici que la reprise d'approbation lit : au milieu du run, pas à la fin. */
        const history = runner.getThreadMessages("thread-midrun", {
          ownerId: "user-1",
        });
        callsMidRun = history.flatMap(
          (message) => (message as { toolCalls?: unknown[] }).toolCalls ?? [],
        ) as never;
        answered = history.some(
          (message) =>
            message.role === "tool" &&
            (message as { toolCallId?: string }).toolCallId === "call-1",
        );

        onEvent({
          event: { type: "RUN_FINISHED", threadId: input.threadId, runId: input.runId },
        });
      },
    } as never;

    await new Promise<void>((resolve) => {
      runner
        .run({
          threadId: "thread-midrun",
          ownerId: "user-1",
          agent,
          input: {
            threadId: "thread-midrun",
            runId: "run-1",
            messages: [{ id: "u1", role: "user", content: "clique sur le bouton" }],
            tools: [],
            context: [],
            state: {},
          },
        } as never)
        .subscribe({ next: () => {}, error: () => resolve() });
      setTimeout(resolve, 100);
    });

    expect(callsMidRun).toContainEqual({
      id: "call-1",
      name: "computer_click",
      args: '{"x":1}',
    });
    expect(answered).toBe(false);
  });
});
