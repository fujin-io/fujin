# syntax=docker/dockerfile:1

ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS builder

ARG FUJIN_CONFIGURATORS=github.com/fujin-io/fujin/public/plugins/configurator/all
ARG FUJIN_CONNECTORS=github.com/fujin-io/fujin/public/plugins/connector/all
ARG FUJIN_BIND_MIDDLEWARES
ARG FUJIN_CONNECTOR_MIDDLEWARES
ARG FUJIN_GO_TAGS=fujin,grpc

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN chmod +x build.sh

RUN FUJIN_CONFIGURATORS="${FUJIN_CONFIGURATORS:-}" \
    FUJIN_CONNECTORS="${FUJIN_CONNECTORS:-}" \
    FUJIN_BIND_MIDDLEWARES="${FUJIN_BIND_MIDDLEWARES:-}" \
    FUJIN_CONNECTOR_MIDDLEWARES="${FUJIN_CONNECTOR_MIDDLEWARES:-}" \
    FUJIN_GO_TAGS="${FUJIN_GO_TAGS}" \
    ./build.sh

FROM scratch AS runtime

WORKDIR /

COPY --from=builder /app/bin/fujin /fujin

STOPSIGNAL SIGTERM

EXPOSE 8080

ENTRYPOINT ["/fujin"]