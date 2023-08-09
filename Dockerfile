FROM golang:alpine as builder
ENV CGO_ENABLED=0
RUN mkdir -p $GOPATH/src/github.com/vx-labs
WORKDIR $GOPATH/src/github.com/vx-labs/nest
COPY go.* ./
RUN go mod download
COPY . ./
RUN go test ./...
ARG BUILT_VERSION="snapshot"
RUN go build -buildmode=exe -ldflags="-s -w -X github.com/vx-labs/nest/cmd/nest/version.BuiltVersion=${BUILT_VERSION}" \
       -a -o /bin/nest ./cmd/nest

FROM alpine:3.18.3 as prod
ENTRYPOINT ["/usr/bin/nest"]
RUN apk -U add ca-certificates && \
    rm -rf /var/cache/apk/*
COPY --from=builder /bin/nest /usr/bin/nest
