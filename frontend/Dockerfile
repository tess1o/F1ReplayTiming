FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY . .

ARG VITE_API_URL=
ENV VITE_API_URL=$VITE_API_URL

RUN npm run build

FROM nginx:alpine AS runner
WORKDIR /usr/share/nginx/html

ENV BACKEND_INTERNAL_URL=http://f1-backend:8000
ENV VITE_API_URL=

COPY --from=builder /app/dist/ ./
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
COPY nginx/40-generate-config.sh /docker-entrypoint.d/40-generate-config.sh

RUN chmod +x /docker-entrypoint.d/40-generate-config.sh

EXPOSE 3000
