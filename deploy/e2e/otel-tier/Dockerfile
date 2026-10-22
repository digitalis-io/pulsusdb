FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /build
COPY go.mod main.go ./
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go mod tidy && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -o /out/server .

FROM alpine:latest
COPY --from=build /out/server /server
ENTRYPOINT ["/server"]
