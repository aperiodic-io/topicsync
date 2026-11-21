# syntax=docker/dockerfile:1
FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
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
