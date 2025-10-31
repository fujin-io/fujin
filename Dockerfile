ARG GO_VERSION=1.24

FROM golang:${GO_VERSION}-alpine AS builder

ARG GO_BUILD_TAGS=kafka,nats_core,amqp091,amqp10,resp_pubsub,resp_streams,mqtt,nsq

WORKDIR /app

COPY . .

RUN apk add git make

RUN cd server && go mod download

RUN make build GO_BUILD_TAGS=${GO_BUILD_TAGS}

FROM scratch

WORKDIR /

COPY --from=builder app/bin/fujin /fujin

STOPSIGNAL SIGTERM

ENTRYPOINT ["/fujin"]