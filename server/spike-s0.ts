/**
 * Spike S0 — le point de bascule existe-t-il vraiment dans la version installée ?
 *
 * Construit un CopilotRuntime SANS Intelligence (runner en mémoire) et vérifie :
 *   1. que le runtime accepte `runner` au lieu de `intelligence` ;
 *   2. qu'il rapporte le mode "sse" ;
 *   3. qu'un run AG-UI minimal traversé par ce runner produit des événements.
 *
 * Ce fichier est jetable : il ne rentre pas dans le fork, il prouve la couture.
 */
import { Observable } from "rxjs";
import { CopilotRuntime, InMemoryAgentRunner } from "@copilotkit/runtime/v2";

const events: unknown[] = [];

const fakeAgent = {
  // Le contrat AgentRunner appelle `agent.run(input)` et attend un Observable d'événements AG-UI.
  run: () =>
    new Observable((observer) => {
      observer.next({ type: "RUN_STARTED", threadId: "t1", runId: "r1" });
      observer.next({ type: "TEXT_MESSAGE_START", messageId: "m1", role: "assistant" });
      observer.next({ type: "TEXT_MESSAGE_CONTENT", messageId: "m1", delta: "bonjour" });
      observer.next({ type: "TEXT_MESSAGE_END", messageId: "m1" });
      observer.next({ type: "RUN_FINISHED", threadId: "t1", runId: "r1" });
      observer.complete();
    }),
  clone: () => fakeAgent,
  connect: () => new Observable(() => {}),
  abortRun: () => {},
  addMessage: () => {},
  setMessages: () => {},
  messages: [],
  state: {},
  agentId: "spike",
  description: "",
  subscribe: () => ({ unsubscribe: () => {} }),
};

const runtime = new CopilotRuntime({
  agents: { spike: fakeAgent as never },
  runner: new InMemoryAgentRunner(),
  threads: { access: "open" },
});

console.log("mode =", runtime.mode);
console.log("intelligence =", runtime.intelligence);
console.log("runner =", runtime.runner.constructor.name);

const runner = runtime.runner;
const stream = runner.run({
  threadId: "thread-spike",
  agent: fakeAgent as never,
  input: { threadId: "thread-spike", runId: "r1", messages: [], tools: [], context: [], state: {} } as never,
});

await new Promise<void>((resolve, reject) => {
  stream.subscribe({
    next: (event) => events.push((event as { type: string }).type),
    error: reject,
    complete: resolve,
  });
});

console.log("événements =", events.join(" → "));

if (typeof runner.listThreads === "function") {
  console.log("threads visibles =", runner.listThreads({ ownerId: "" }).length);
} else {
  console.log("pas d'endpoint threads local sur ce runner");
}
