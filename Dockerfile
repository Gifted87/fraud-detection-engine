# --- STAGE 1: Build ---
FROM node:18-alpine AS builder

WORKDIR /usr/src/app

# Install build dependencies for native modules if any (bcrypt, etc)
RUN apk add --no-cache python3 make g++

# Copy package files first for better caching
COPY package*.json ./
RUN npm ci

# Copy source and config
COPY . .

# Build the application
RUN npm run build

# --- STAGE 2: Runtime ---
FROM node:18-alpine

# Set high-performance Node.js environment variables
ENV NODE_ENV=production
ENV NODE_OPTIONS="--max-old-space-size=2048 --dns-result-order=ipv4first"

WORKDIR /usr/src/app

# Only copy necessary files for runtime to keep image slim
COPY package*.json ./
RUN npm ci --only=production && npm cache clean --force

# Copy compiled artifacts from builder stage
COPY --from=builder /usr/src/app/dist ./dist
# Copy migrations for startup execution if needed
COPY --from=builder /usr/src/app/src/store/event_store/postgres_impl/migrations ./dist/store/event_store/postgres_impl/migrations
COPY --from=builder /usr/src/app/knexfile.ts ./dist/knexfile.js

# Create a non-root user for security (Least Privilege Principle)
RUN addgroup -S fraudgroup && adduser -S frauduser -G fraudgroup
USER frauduser

# Expose metrics port (mapped via SystemConfiguration)
EXPOSE 9090

# Entry point triggers the compiled bootstrap
CMD ["node", "dist/index.js"]
