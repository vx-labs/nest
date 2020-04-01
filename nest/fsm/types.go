package fsm

//go:generate protoc -I${GOPATH}/src -I ${GOPATH}/src/github.com/vx-labs/nest/vendor -I ${GOPATH}/src/github.com/vx-labs/nest/vendor/github.com/gogo/protobuf/  -I${GOPATH}/src/github.com/vx-labs/nest/nest/fsm/ --go_out=plugins=grpc:. types.proto
