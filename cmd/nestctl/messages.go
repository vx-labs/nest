package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const recordTemplate = `{{ .Timestamp }} {{ .Topic }} {{ .Payload }}`

type record struct {
	Timestamp int64
	Topic     string
	Payload   string
}

func Messages(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "messages",
	}
	cmd.AddCommand(&cobra.Command{
		Use: "put",
		Run: func(cmd *cobra.Command, _ []string) {
			conn, l := mustDial(ctx, cmd, config)
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			_, err := api.NewMessagesClient(conn).PutRecords(ctx, &api.PutRecordsRequest{
				Records: []*api.Record{
					&api.Record{Timestamp: time.Now().UnixNano(),
						Payload: []byte("test"),
					},
				},
			})
			if err != nil {
				l.Fatal("failed to put record", zap.Error(err))
			}
			cancel()
		},
	})
	get := (&cobra.Command{
		Use:  "get",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			stream, err := api.NewMessagesClient(conn).GetRecords(ctx, &api.GetRecordsRequest{
				Patterns: patterns,
			})
			if err != nil {
				l.Fatal("failed to start stream", zap.Error(err))
			}
			out := []*api.Record{}
			for {
				record, err := stream.Recv()
				if err != nil {
					break
				}
				out = append(out, record.Records...)
			}
			if err != io.EOF && err != nil {
				l.Error("failed to stream records", zap.Error(err))
			} else {
				tpl := ParseTemplate(config.GetString("format"))
				for _, elt := range out {
					r := record{Timestamp: time.Unix(0, elt.Timestamp).Unix(), Topic: string(elt.Topic), Payload: string(elt.Payload)}
					tpl.Execute(cmd.OutOrStdout(), r)
				}
				fmt.Fprintf(cmd.ErrOrStderr(), "\nPattern %q: %d messages\n\n", strings.Join(args, ", "), len(out))
			}
		},
	})
	get.Flags().String("format", recordTemplate, "Format each record using Golang template format.")
	cmd.AddCommand(get)
	backupCommand := &cobra.Command{
		Use:  "backup",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			stream, err := api.NewMessagesClient(conn).Dump(ctx, &api.DumpRequest{
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
			stream, err := api.NewMessagesClient(conn).Load(ctx, &api.LoadRequest{
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
	cmd.AddCommand(&cobra.Command{
		Use: "bench",
		Run: func(cmd *cobra.Command, _ []string) {
			conn, l := mustDial(ctx, cmd, config)
			count := 0
			ctx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			start := time.Now()
			go func() {
				sigc := make(chan os.Signal, 1)
				signal.Notify(sigc,
					syscall.SIGINT,
					syscall.SIGTERM,
					syscall.SIGQUIT)
				select {
				case <-sigc:
					fmt.Println()
					cancel()
				}
			}()
			go func() {
				for {
					defer close(done)
					_, err := api.NewMessagesClient(conn).PutRecords(ctx, &api.PutRecordsRequest{
						Records: []*api.Record{&api.Record{Payload: []byte("test"), Topic: []byte("test")}},
					})
					if err != nil {
						if ctx.Err() == context.Canceled {
							return
						}
						l.Fatal("failed to put record", zap.Error(err))
					}
					count++
				}
			}()
			<-done
			elapsed := time.Since(start)
			rate := count / int(elapsed.Seconds())
			fmt.Printf("Benchmark done: %d msg in %s\n", count, elapsed.String())
			fmt.Printf("Rate is %d msg/s\n", rate)
		},
	})
	return cmd
}
