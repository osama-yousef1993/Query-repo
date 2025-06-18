SHELL := /usr/local/bin/zsh
PROJECT_ID=digital-assets-301018

tools:
	go install github.com/securego/gosec/v2/cmd/gosec@latest;
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest;

test: 
	golangci-lint run ./...
	gosec ./...
	go test -v ./...

run:
	$(shell cat .env | tr "\n" " ") go run .