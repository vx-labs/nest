package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const eventTemplate = `[{{ .Timestamp | parseDate | yellow | faint }}] {{ .Kind | faint}} {{ range $attr := .Attributes }} {{ $attr.Key }}={{ $attr.Value }} {{ end }}`

func Events(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "events",
	}

	get := (&cobra.Command{
		Use: "get",
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			timestamp := config.GetInt64("from-timestamp")
			since := config.GetDuration("since")
			if since > 0 {
				timestamp = time.Now().Add(-since).UnixNano()
			}

			stream, err := api.NewEventsClient(conn).GetEvents(ctx, &api.GetEventRequest{
				FromOffset:    config.GetInt64("from-offset"),
				FromTimestamp: timestamp,
				Watch:         config.GetBool("watch"),
				Tenant:        config.GetString("tenant"),
			})
			if err != nil {
				l.Fatal("failed to start stream", zap.Error(err))
			}
			tpl := ParseTemplate(config.GetString("format"))
			count := 0
			for {
				records, err := stream.Recv()
				if err != nil {
					break
				}
				for _, elt := range records.Events {
					tpl.Execute(cmd.OutOrStdout(), elt)
					count++
				}
			}
			if err != io.EOF && err != nil {
				l.Error("failed to stream records", zap.Error(err))
			} else {
				fmt.Fprintf(cmd.ErrOrStderr(), "%d messages\n\n", count)
			}
		},
	})
	get.Flags().StringP("tenant", "t", "", "Fetch all events for the given tenant.")
	get.MarkFlagRequired("tenant")
	get.Flags().String("format", eventTemplate, "Format each event using Golang template format.")
	get.Flags().Int64P("from-timestamp", "", 0, "Fetch events written after the given timestamp.")
	get.Flags().Int64P("from-offset", "", 0, "Fetch events written after the given offset.")
	get.Flags().Duration("since", 0, "Fetch records written since the given time expression.")
	get.Flags().BoolP("watch", "w", false, "Watch for new events")
	backupCommand := &cobra.Command{
		Use:  "backup",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			stream, err := api.NewStreamsClient(conn).Dump(ctx, &api.DumpRequest{
				Stream:         "events",
				Shard:          0,
				DestinationURL: config.GetString("destination-url"),
			})
			if err != nil {
				l.Fatal("failed to start backup", zap.Error(err))
			}
			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					l.Fatal("failed to run backup", zap.Error(err))
				}
				log.Printf("Backup in progress: %d/%d\n", msg.ProgressBytes, msg.TotalBytes)
			}
			log.Printf("Backup done")
		},
	}
	backupCommand.Flags().StringP("destination-url", "t", "", "Backup destination URL (file URLs are resolved server-side.)")
	backupCommand.MarkFlagRequired("destination-url")
	cmd.AddCommand(backupCommand)
	restoreCommand := &cobra.Command{
		Use:  "restore",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			stream, err := api.NewStreamsClient(conn).Load(ctx, &api.LoadRequest{
				Stream:    "events",
				Shard:     0,
				SourceURL: config.GetString("source-url"),
			})
			if err != nil {
				l.Fatal("failed to start restore", zap.Error(err))
			}
			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					l.Fatal("failed to run restore", zap.Error(err))
				}
				log.Printf("Restore in progress: %d/%d\n", msg.ProgressBytes, msg.TotalBytes)
			}
			log.Printf("Restore done")
		},
	}
	restoreCommand.Flags().StringP("source-url", "f", "", "Backup source URL (file URLs are resolved server-side.)")
	restoreCommand.MarkFlagRequired("source-url")
	cmd.AddCommand(restoreCommand)
	cmd.AddCommand(get)

	return cmd
}
