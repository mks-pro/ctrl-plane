# syntax=docker/dockerfile:1

# Build image is also used by the Compose test service (`go test -race` needs cgo).
FROM golang:1.26.2 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/api ./cmd/api \
    && go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/worker ./cmd/worker \
    && go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app app

WORKDIR /app
COPY --from=build /out/ /app/

USER 10001:10001
EXPOSE 8090

CMD ["/app/api"]
