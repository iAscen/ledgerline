# syntax=docker/dockerfile:1.7

# ---- build ------------------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=auto

# Dependencies first, so source edits do not invalidate the module cache layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ledgerline ./cmd/ledgerline

# ---- runtime ----------------------------------------------------------------
# distroless/static: no shell, no package manager, CA certs + tzdata only.
# The :nonroot tag runs as UID/GID 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledgerline /usr/local/bin/ledgerline
USER 65532:65532
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/ledgerline", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/ledgerline"]
CMD ["serve"]
