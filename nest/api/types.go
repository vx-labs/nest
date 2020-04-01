package api

//go:generate protoc -I ${GOPATH}/src/github.com/vx-labs/nest/vendor -I ${GOPATH}/src/github.com/vx-labs/nest/vendor/github.com/gogo/protobuf/ -I ${GOPATH}/src/github.com/vx-labs/nest/nest/api api.proto --go_out=plugins=grpc:.
