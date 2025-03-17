FROM golang:1.23-alpine3.19 as builder
RUN apk add --no-cache dpkg gcc git musl-dev openssh

WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -v -installsuffix cgo -o app ./cmd/app
RUN CGO_ENABLED=0 GOOS=linux go build -a -v -installsuffix cgo -o relay ./cmd/relay
RUN CGO_ENABLED=0 GOOS=linux go build -a -v -installsuffix cgo -o worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -v -installsuffix cgo -o worker2 ./cmd/worker2


FROM alpine:latest

COPY --from=builder /app/app ./
COPY --from=builder /app/relay ./
COPY --from=builder /app/worker ./
COPY --from=builder /app/worker2 ./

CMD ["./app"]