# Dockerfile for Node.js backend
FROM node:18

WORKDIR /app

COPY backend/package*.json ./
RUN npm install

COPY backend/ ./backend/

WORKDIR /app/backend
CMD ["node", "server.js"]