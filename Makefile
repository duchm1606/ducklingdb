.PHONY: proto test

proto:
	protoc \
		--proto_path=. \
		--go_out=. --go_opt=module=github.com/duchm1606/ducklingdb \
		--go-grpc_out=. --go-grpc_opt=module=github.com/duchm1606/ducklingdb \
		internal/proto/data.proto \
		internal/proto/api.proto \
		internal/proto/metadata.proto \
		internal/proto/service.proto

test:
	go test ./...
