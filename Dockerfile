# syntax=docker/dockerfile:1
FROM golang:1.26.10@sha256:cfa27959837144bf4649802ba38e59daffd0dd2a7994aa83daaf113285b0f551 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
# the image's Go builds (GOTOOLCHAIN=local): pinned to go.mod's toolchain above, which govulncheck checked
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/aperiodic-io/topicsync/internal/cli.Version=${VERSION}" -o /topicsync ./cmd/topicsync

# distroless static, non-root: nothing in the image but the binary and CA certificates
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /topicsync /usr/local/bin/topicsync
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/topicsync"]
