FROM golang:1.17 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/mxcoder/benthos-plugin/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/mxcoder/benthos-plugin/
RUN go mod download

# Build
COPY . /go/src/github.com/mxcoder/benthos-plugin/
RUN go build -o bin/benthos

# Pack
FROM busybox

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd

COPY --from=build /go/src/github.com/mxcoder/benthos-plugin/bin/benthos .
COPY ./protos protos/
COPY ./config.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]
