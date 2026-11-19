FROM golang:1.27.4-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/agentos ./cmd/agentos

FROM scratch
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/agentos /agentos
COPY LICENSE /LICENSE
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/agentos"]
CMD ["--config=/etc/agentos/config.json", "--db=/var/lib/agentos/state/runs.db", "--listen=:8080"]
