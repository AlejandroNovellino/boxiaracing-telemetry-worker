.PHONY: build test vet check docker-build

build:
	go build ./cmd/worker

test:
	go test ./...

vet:
	go vet ./...

check: test vet

docker-build:
	docker build --tag boxiaracing-telemetry-worker:local .
