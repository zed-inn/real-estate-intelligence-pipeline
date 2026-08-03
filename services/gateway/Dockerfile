FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install && npm cache clean --force

COPY . .

EXPOSE 3000

CMD ["sh", "-c", "npm run db:push && npm run start"]
