FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

# Se precisar do ffmpeg, adicione:  apk add --no-cache ffmpeg
RUN apk add --no-cache ca-certificates && update-ca-certificates

# Instala as deps no build (sem rodar nada em loop depois)
COPY package*.json ./
# usa ci se houver lock; senão cai pro install
RUN npm ci --omit=dev --no-audit --no-fund || \
    npm install --omit=dev --no-audit --no-fund --legacy-peer-deps

# Copia o resto do código
COPY . .

# Executa direto o supervisor
CMD ["node", "master-workers.js"]
