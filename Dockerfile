# Development image: Go + Air for live-reload
FROM golang:1.24-bookworm

ENV CGO_ENABLED=1
RUN go install github.com/air-verse/air@v1.52.3

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Air runs inside the container and rebuilds on file changes (volume mount)
EXPOSE 8080
CMD ["air"]
