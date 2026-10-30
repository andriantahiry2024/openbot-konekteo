# Serveur MCP Facebook, en un conteneur.
#
# Rien à compiler : Bun exécute le TypeScript directement. L'image ne contient que les dépendances
# et le fichier du serveur, parce qu'un serveur qui publie sur une page Facebook n'a aucune raison
# d'embarquer quoi que ce soit d'autre.
FROM oven/bun:1.4.4-slim

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY server.ts ./

ENV NODE_ENV=production
ENV PORT=8090
EXPOSE 8090
USER bun
CMD ["bun", "run", "server.ts"]
