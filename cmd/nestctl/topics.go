package main

import (
	"context"
	"io"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const topicMetadataTemplate = `{{ .Name | bytesToString }}: {{ .MessageCount }} messages`

func Topics(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "topics",
	}
	get := (&cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			out, err := api.NewMessagesClient(conn).ListTopics(ctx, &api.ListTopicsRequest{
				Pattern: []byte(config.GetString("pattern")),
			})
			if err != nil {
				l.Fatal("failed to start stream", zap.Error(err))
			}
			tpl := ParseTemplate(config.GetString("format"))

			if err != nil {
				l.Error("failed to stream records", zap.Error(err))
			} else {
				for _, topic := range out.TopicMetadatas {
					tpl.Execute(cmd.OutOrStdout(), topic)
				}
			}
		},
	})
	get.Flags().String("format", topicMetadataTemplate, "Format each record using Golang template format.")
	get.Flags().StringP("pattern", "p", "#", "Filter topics using this pattern.")
	cmd.AddCommand(get)

	reindex := (&cobra.Command{
		Use: "reindex",
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			stream, err := api.NewMessagesClient(conn).ReindexTopics(ctx, &api.ReindexTopicsRequest{})
			if err != nil {
				l.Fatal("failed to start stream", zap.Error(err))
			}
			bar := pb.StartNew(100)
			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					bar.SetCurrent(100)
					break
				}
				if err != nil {
					l.Error("indexation failed", zap.Error(err))
					return
				}
				bar.SetCurrent(int64(msg.Progress))
			}
			bar.Finish()
		},
	})
	cmd.AddCommand(reindex)

	return cmd
}
