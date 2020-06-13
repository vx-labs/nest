package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"

	"github.com/vx-labs/nest/nest"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/cluster"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func handleSnapshot(ctx context.Context, id uint64, snapshot *raftpb.Snapshot, messageLog nest.MessageLog, clusterNode cluster.Node) error {
	snapshotDescription := &Snapshot{}
	err := json.Unmarshal(snapshot.Data, snapshotDescription)
	if err != nil {
		nest.L(ctx).Debug("failed to decode state snapshot", zap.Error(err), zap.Int("snapshot_size", snapshot.Size()))
	} else {
		if id == snapshotDescription.Remote {
			return nil
		}
		nest.L(ctx).Info("loading snapshot", zap.Uint64("remote_node", snapshotDescription.Remote), zap.Uint64("current_log_offset", messageLog.CurrentOffset()), zap.Uint64("snapshot_log_offset", snapshotDescription.MessagesOffset))
		file, err := ioutil.TempFile("", "sst-incoming.*.nest")
		if err != nil {
			nest.L(ctx).Fatal("failed to create tmp file to receive snapshot", zap.Error(err))
		}
		defer os.Remove(file.Name())
		defer file.Close()
		err = clusterNode.Call(snapshotDescription.Remote, func(c *grpc.ClientConn) error {
			stream, err := api.NewMessagesClient(c).SST(ctx, &api.SSTRequest{
				ToOffset: snapshotDescription.MessagesOffset,
			})
			if err != nil {
				return err
			}

			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_, err = file.Write(chunk.Chunk)
				if err != nil {
					return err
				}
			}
			return file.Sync()
		})
		if err != nil {
			nest.L(ctx).Fatal("failed to receive snapshot", zap.Error(err))
		}
		file.Seek(0, io.SeekStart)
		err = messageLog.LoadState(snapshotDescription.StateOffset, file)
		if err != nil {
			nest.L(ctx).Fatal("failed to load snapshot", zap.Error(err))
		}
		nest.L(ctx).Info("loaded snapshot", zap.Uint64("current_log_offset", messageLog.CurrentOffset()))
	}
	return nil
}
