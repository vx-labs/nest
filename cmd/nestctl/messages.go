package main

import (
	"context"
	"fmt"
	"io"
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
					{
						Timestamp: time.Now().UnixNano(),
						Topic:     []byte("test"),
						Payload:   []byte("test"),
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
		Use: "get",
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			stream, err := api.NewMessagesClient(conn).GetRecords(ctx, &api.GetRecordsRequest{
				Shard:      config.GetInt64("shard"),
				Patterns:   patterns,
				FromOffset: config.GetUint64("from-offset"),
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
	get.Flags().Int64P("shard", "s", 0, "Fetch records stored in the provided shard.")
	get.Flags().Uint64P("from-offset", "f", 0, "Fetch records written after the given offset.")
	cmd.AddCommand(get)

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
						Records: []*api.Record{&api.Record{Timestamp: time.Now().UnixNano(), Payload: []byte("test"), Topic: []byte("test")}},
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
