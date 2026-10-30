# mcp-facebook

Serveur MCP (Streamable HTTP, `/mcp`) qui donne à un bot OpenBot l'accès à une page Facebook par
l'API Graph : informations de la page, publication, lecture des posts, lecture et réponse aux
commentaires.

## Pourquoi un serveur MCP et pas le navigateur du bot

Facebook refuse les connexions depuis une IP de datacenter et un navigateur automatisé. Un bot qui
« se connecte à Facebook » dans son Chromium sera bloqué quoi qu'on fasse. L'API Graph est la voie
prévue, et elle ne dépend pas de la session de quelqu'un.

## Variables

| Variable | Rôle |
|---|---|
| `FB_PAGE_TOKEN` | jeton de page de longue durée (pas un jeton utilisateur) |
| `FB_PAGE_ID` | identifiant de la page ; à défaut, `me` |
| `MCP_AUTH_TOKEN` | jeton partagé exigé en `Authorization: Bearer` sur chaque requête |
| `PORT` | 8090 par défaut |

## Outils exposés

- `fb_page_info` — nom, abonnés, catégorie de la page
- `fb_publish_post` — publie un texte (+ lien optionnel)
- `fb_list_posts` — derniers posts avec réactions et commentaires
- `fb_list_comments` — commentaires d'un post
- `fb_reply_comment` — répond sous le nom de la page

## Vérifier

```
curl -s -X POST http://localhost:8090/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "Authorization: Bearer $MCP_AUTH_TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```
