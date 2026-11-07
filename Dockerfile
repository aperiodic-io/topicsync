# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/aperiodic-io/topicsync/internal/cli.Version=${VERSION}" -o /topicsync ./cmd/topicsync

# distroless static, non-root: nothing in the image but the binary and CA certificates
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /topicsync /usr/local/bin/topicsync
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/topicsync"]
