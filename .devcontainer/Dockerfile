FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends libopus-dev pkg-config nodejs npm \
    && corepack enable \
    && corepack prepare pnpm@10.0.0 --activate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER vscode

WORKDIR /workspaces/ecorun-logger-server
