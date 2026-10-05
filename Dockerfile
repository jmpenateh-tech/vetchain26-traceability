# syntax=docker/dockerfile:1
# Node 22 LTS: misma versión mayor en todos los servicios (Node 20 llegó a fin de soporte).
FROM node:22-alpine

WORKDIR /app

# Instala solo dependencias de produccion usando el lockfile (build reproducible)
COPY package*.json ./
RUN npm ci --omit=dev

# Copia el resto del codigo del servicio (.dockerignore excluye node_modules y .env)
COPY . .

EXPOSE 3004

CMD ["npm", "start"]
