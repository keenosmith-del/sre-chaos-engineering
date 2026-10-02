FROM golang:1.24.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal internal
COPY services services
RUN --mount=type=cache,target=/root/.cache/go-build mkdir /out && for service in gateway ordering inventory payments worker control; do CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -trimpath -o /out/$service ./services/$service; done
FROM alpine:3.21.3
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
ARG SERVICE
COPY --from=build /out/${SERVICE} /service
USER app
EXPOSE 8080
ENTRYPOINT ["/service"]
