package main

import (
	"context"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const topicMetadataTemplate = `• {{ .Name | bytesToString | green }}
  Storage: {{ .MessageCount | bold }} messages - {{ .SizeInBytes | bold }} stored bytes
  Detected content type: "{{ .GuessedContentType | bold }}"
{{- if eq .GuessedContentType "text/plain; charset=utf-8" }}
  Last Record: {{ .LastRecord.Payload | bytesToString | bold }}
{{- end }}`

func Topics(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "topics",
	}
	list := (&cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			if len(args) == 0 {
				patterns = append(patterns, []byte("#"))
			}
			for _, pattern := range patterns {
				out, err := api.NewMessagesClient(conn).ListTopics(ctx, &api.ListTopicsRequest{
					Pattern: pattern,
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
			}
		},
	})
	list.Flags().String("format", topicMetadataTemplate, "Format each record using Golang template format.")
	cmd.AddCommand(list)

	get := (&cobra.Command{
		Use: "get",
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			if len(args) == 0 {
				patterns = append(patterns, []byte("#"))
			}
			timestamp := config.GetInt64("from-timestamp")
			since := config.GetDuration("since")
			if since > 0 {
				timestamp = time.Now().Add(-since).UnixNano()
			}
			for _, pattern := range patterns {
				stream, err := api.NewMessagesClient(conn).GetTopics(ctx, &api.GetTopicsRequest{
					Pattern:       pattern,
					FromOffset:    config.GetInt64("from-offset"),
					FromTimestamp: timestamp,
					Watch:         config.GetBool("watch"),
				})
				if err != nil {
					l.Fatal("failed to start stream", zap.Error(err))
				}
				tpl := ParseTemplate(config.GetString("format"))

				for {
					msg, err := stream.Recv()
					if err != nil {
						if err == io.EOF {
							return
						}
						l.Fatal("failed to stream", zap.Error(err))
					}
					for _, record := range msg.Records {
						tpl.Execute(cmd.OutOrStdout(), record)
					}
				}
			}
		},
	})
	get.Flags().String("format", recordTemplate, "Format each record using Golang template format.")
	get.Flags().BoolP("watch", "w", false, "Watch for new records")
	get.Flags().Int64P("from-timestamp", "", 0, "Fetch records written after the given timestamp.")
	get.Flags().Int64P("from-offset", "", 0, "Fetch records written after the given offset.")
	get.Flags().Duration("since", 0, "Fetch records written since the given time expression.")
	cmd.AddCommand(get)

	return cmd
}
