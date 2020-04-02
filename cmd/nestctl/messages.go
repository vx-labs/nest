package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

func Messages(ctx context.Context, config *viper.Viper) *cobra.Command {
	mqtt := &cobra.Command{
		Use: "messages",
	}
	mqtt.AddCommand(&cobra.Command{
		Use: "put",
		Run: func(cmd *cobra.Command, _ []string) {
			conn, l := mustDial(ctx, cmd, config)
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			_, err := api.NewMessagesClient(conn).PutRecords(ctx, &api.PutRecordsRequest{})
			if err != nil {
				l.Fatal("failed to put record", zap.Error(err))
			}
			cancel()
		},
	})
	mqtt.AddCommand(&cobra.Command{
		Use:  "get",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			for _, topic := range args {
				stream, err := api.NewMessagesClient(conn).GetRecords(ctx, &api.GetRecordsRequest{
					Topic: []byte(topic),
				})
				out := []*api.Record{}
				for {
					record, err := stream.Recv()
					if err != nil {
						break
					}
					out = append(out, record.Records...)
				}
				if err != io.EOF && err != nil {
					l.Error("failed to get records", zap.Error(err))
				} else {
					for _, elt := range out {
						fmt.Printf("%s %s %s\n", time.Unix(0, elt.Timestamp).Format(time.Stamp), string(elt.Topic), string(elt.Payload))
					}
					fmt.Printf("\nPattern %q: %d messages\n\n", topic, len(out))
				}
			}
		},
	})
	mqtt.AddCommand(&cobra.Command{
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
	return mqtt
}
