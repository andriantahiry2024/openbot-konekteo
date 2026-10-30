/**
 * Serveur MCP Facebook pour OpenBot.
 *
 * POURQUOI CE SERVEUR EXISTE. OpenBot parle MCP, et le fork ne dépend plus du broker de connecteurs
 * de CopilotKit. Un serveur MCP autonome est donc la façon la moins invasive de rendre à un bot
 * l'accès à Facebook : il vit à côté du produit, il a son propre secret, et on peut l'arrêter sans
 * toucher au reste.
 *
 * CE QU'IL NE FAIT PAS. Il ne navigue pas sur Facebook avec une session humaine. Facebook refuse
 * les connexions depuis une IP de datacenter et un navigateur automatisé, et contourner ça serait
 * jouer contre eux. Il parle à l'API Graph avec un jeton de page : publier, lire, répondre.
 *
 * TRANSPORT. Streamable HTTP sur `/mcp`, parce que c'est le seul que le client d'OpenBot sait
 * parler (`plugins/mcp.ts`). Chaque requête porte un `Authorization: Bearer` : le serveur refuse
 * tout ce qui n'a pas le jeton partagé, sinon n'importe qui sur internet publierait sur la page.
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { z } from "zod";

const GRAPH = "https://graph.facebook.com/v21.0";
const PORT = Number(process.env.PORT ?? 8090);
const SHARED_TOKEN = process.env.MCP_AUTH_TOKEN?.trim();
const PAGE_TOKEN = process.env.FB_PAGE_TOKEN?.trim();
const PAGE_ID = process.env.FB_PAGE_ID?.trim();

if (!SHARED_TOKEN) {
  throw new Error(
    "MCP_AUTH_TOKEN est requis : c'est le jeton que le bot présente, et sans lui le serveur serait ouvert à tout internet.",
  );
}
if (!PAGE_TOKEN) {
  throw new Error(
    "FB_PAGE_TOKEN est requis : un jeton de page de longue durée, pas un jeton d'utilisateur.",
  );
}

/** Appelle Graph et rend ce que la page peut lire, ou une erreur lisible par le modèle. */
async function graph(
  path: string,
  params: Record<string, string> = {},
): Promise<{ ok: true; data: unknown } | { ok: false; error: string }> {
  const url = new URL(`${GRAPH}${path}`);
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
  url.searchParams.set("access_token", PAGE_TOKEN!);
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(20_000) });
    const body = (await response.json()) as Record<string, unknown>;
    if (!response.ok) {
      const err = body.error as { message?: string; code?: number } | undefined;
      return {
        ok: false,
        error: `Facebook a refusé (${response.status}${err?.code ? `, code ${err.code}` : ""}) : ${err?.message ?? "sans message"}`,
      };
    }
    return { ok: true, data: body };
  } catch (error) {
    return {
      ok: false,
      error: `Facebook est injoignable : ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

const text = (value: string) => ({ content: [{ type: "text" as const, text: value }] });

function buildServer(): McpServer {
  const server = new McpServer({ name: "facebook", version: "0.1.0" });

  server.registerTool(
    "fb_page_info",
    {
      title: "Informations de la page Facebook",
      description:
        "Nom, identifiant, nombre d'abonnés et catégorie de la page Facebook connectée. À appeler avant toute publication pour vérifier sur quelle page on écrit.",
    },
    async () => {
      const pageId = PAGE_ID ?? "me";
      const result = await graph(`/${pageId}`, {
        fields: "id,name,followers_count,fan_count,category,link",
      });
      if (!result.ok) return text(result.error);
      const data = result.data as Record<string, unknown>;
      return text(
        `Page : ${data.name ?? "(sans nom)"} (id ${data.id ?? pageId})\n` +
          `Abonnés : ${data.followers_count ?? data.fan_count ?? "inconnu"}\n` +
          `Catégorie : ${data.category ?? "inconnue"}\n` +
          `Lien : ${data.link ?? "inconnu"}`,
      );
    },
  );

  server.registerTool(
    "fb_publish_post",
    {
      title: "Publier sur la page Facebook",
      description:
        "Publie un message texte (et éventuellement un lien) sur la page connectée. Rend l'identifiant du post. Ne publie jamais sans que le texte ait été demandé explicitement par la personne.",
      inputSchema: {
        message: z.string().min(1).max(5000).describe("Le texte du post"),
        link: z.string().url().optional().describe("Lien à joindre, optionnel"),
      },
    },
    async ({ message, link }) => {
      const pageId = PAGE_ID ?? "me";
      const result = await graph(`/${pageId}/feed`, {
        message,
        ...(link ? { link } : {}),
      });
      if (!result.ok) return text(result.error);
      const data = result.data as { id?: string };
      return text(`Publié. Identifiant du post : ${data.id ?? "inconnu"}`);
    },
  );

  server.registerTool(
    "fb_list_posts",
    {
      title: "Lister les publications récentes",
      description:
        "Les dernières publications de la page, avec leur texte, leur date et leurs compteurs de réactions et de commentaires.",
      inputSchema: {
        limit: z.number().int().min(1).max(25).default(10).describe("Nombre de posts à lire"),
      },
    },
    async ({ limit }) => {
      const pageId = PAGE_ID ?? "me";
      const result = await graph(`/${pageId}/posts`, {
        fields: "id,message,created_time,permalink_url,reactions.summary(true),comments.summary(true)",
        limit: String(limit),
      });
      if (!result.ok) return text(result.error);
      const posts = ((result.data as { data?: Record<string, unknown>[] }).data ?? []).map(
        (post, index) =>
          `${index + 1}. ${post.created_time ?? "?"} — ${String(post.message ?? "(sans texte)").slice(0, 200)}\n` +
          `   réactions : ${(post.reactions as { summary?: { total_count?: number } })?.summary?.total_count ?? 0}, ` +
          `commentaires : ${(post.comments as { summary?: { total_count?: number } })?.summary?.total_count ?? 0}\n` +
          `   ${post.permalink_url ?? ""}`,
      );
      return text(posts.length ? posts.join("\n\n") : "Cette page n'a aucune publication.");
    },
  );

  server.registerTool(
    "fb_list_comments",
    {
      title: "Lire les commentaires d'un post",
      description:
        "Les commentaires d'une publication, avec leur auteur et leur texte. À utiliser pour trier ce qui mérite une réponse.",
      inputSchema: {
        post_id: z.string().min(1).describe("Identifiant du post, tel que rendu par fb_list_posts"),
        limit: z.number().int().min(1).max(50).default(25),
      },
    },
    async ({ post_id, limit }) => {
      const result = await graph(`/${post_id}/comments`, {
        fields: "id,from,message,created_time,like_count",
        limit: String(limit),
      });
      if (!result.ok) return text(result.error);
      const comments = ((result.data as { data?: Record<string, unknown>[] }).data ?? []).map(
        (comment, index) =>
          `${index + 1}. ${(comment.from as { name?: string })?.name ?? "inconnu"} (${comment.created_time ?? "?"}) : ` +
          `${String(comment.message ?? "").slice(0, 300)}  [id ${comment.id}]`,
      );
      return text(comments.length ? comments.join("\n") : "Aucun commentaire.");
    },
  );

  server.registerTool(
    "fb_reply_comment",
    {
      title: "Répondre à un commentaire",
      description:
        "Répond publiquement à un commentaire, sous le nom de la page.",
      inputSchema: {
        comment_id: z.string().min(1).describe("Identifiant du commentaire, tel que rendu par fb_list_comments"),
        message: z.string().min(1).max(2000),
      },
    },
    async ({ comment_id, message }) => {
      const result = await graph(`/${comment_id}/comments`, { message });
      if (!result.ok) return text(result.error);
      const data = result.data as { id?: string };
      return text(`Réponse publiée. Identifiant : ${data.id ?? "inconnu"}`);
    },
  );

  return server;
}

Bun.serve({
  port: PORT,
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname !== "/mcp") return new Response("not found", { status: 404 });

    /* A bearer check in front of every MCP call: an open MCP server on the internet is a publishing
       permission handed to whoever finds it. */
    const presented = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
    if (presented !== SHARED_TOKEN) {
      return new Response(JSON.stringify({ error: "unauthorized" }), {
        status: 401,
        headers: { "content-type": "application/json" },
      });
    }

    /* One server and one transport per request: this deployment keeps no session, the same choice
       `plugins/mcp.ts` makes on the client side. */
    const server = buildServer();
    const transport = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(transport);
    return transport.handleRequest(request);
  },
});

console.log(`mcp-facebook à l'écoute sur :${PORT}/mcp`);
