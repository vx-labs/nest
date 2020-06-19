package main

import (
	"context"

	"github.com/vx-labs/nest/nest"
	"github.com/vx-labs/wasp/cluster"
	"go.uber.org/zap"
)

func newStream(ctx context.Context, id uint64, name string, shardCount int, datadir string, clusterMultiNode cluster.MultiNode, server nest.StreamsServer, raftConfig cluster.RaftConfig, logger *zap.Logger) (nest.Controller, error) {
	return nest.NewController(ctx, id, name, shardCount, datadir, clusterMultiNode, server, raftConfig, logger)
}
