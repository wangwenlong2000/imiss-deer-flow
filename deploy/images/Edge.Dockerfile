FROM node:22-alpine AS builder
WORKDIR /src
COPY apps/onecityagent/web/package.json apps/onecityagent/web/package-lock.json ./
RUN npm ci --ignore-scripts=false
COPY apps/onecityagent/web/ ./
ARG ONECITY_BACKEND_URL=http://api:7606
ARG ONECITY_COORDINATOR_URL=http://coordinator:7010
ENV ONECITY_BACKEND_URL=$ONECITY_BACKEND_URL ONECITY_COORDINATOR_URL=$ONECITY_COORDINATOR_URL
RUN npm run build
FROM nginx:1.29-alpine
COPY --from=builder /src/dist /usr/share/nginx/html
EXPOSE 8080
