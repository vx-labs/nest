package main

import (
	"context"
	"fmt"
	"io"
	"strings"
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
	put := &cobra.Command{
		Use: "put",
		Run: func(cmd *cobra.Command, _ []string) {
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			conn, l := mustDial(ctx, cmd, config)
			userInput := config.GetStringSlice("attribute")
			attributes := make([]*api.EventAttribute, len(userInput))
			for idx := range attributes {
				tokens := strings.Split(userInput[idx], "=")
				if len(tokens) != 2 {
					continue
				}
				attributes[idx] = &api.EventAttribute{
					Key:   tokens[0],
					Value: tokens[1],
				}
			}
			_, err := api.NewEventsClient(conn).PutEvent(ctx, &api.PutEventRequest{
				Timestamp:  time.Now().UnixNano(),
				Tenant:     config.GetString("tenant"),
				Kind:       config.GetString("kind"),
				Service:    config.GetString("service"),
				Attributes: attributes,
			})
			if err != nil {
				l.Fatal("failed to put record", zap.Error(err))
			}
		},
	}
	put.Flags().StringP("kind", "", "test", "Event's kind")
	put.Flags().StringP("service", "s", "test", "Event's service")
	put.Flags().StringP("tenant", "t", "test", "Event's tenant")
	put.Flags().StringSliceP("attribute", "a", nil, "Event's attribute")
	cmd.AddCommand(put)

	get := (&cobra.Command{
		Use: "get",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			conn, l := mustDial(ctx, cmd, config)
			patterns := make([][]byte, len(args))
			for idx := range patterns {
				patterns[idx] = []byte(args[idx])
			}
			stream, err := api.NewEventsClient(conn).GetEvents(ctx, &api.GetEventRequest{
				FromOffset: config.GetInt64("from-offset"),
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
	get.Flags().String("format", eventTemplate, "Format each event using Golang template format.")
	get.Flags().Int64P("from-offset", "f", 0, "Fetch events written after the given timestamp.")
	cmd.AddCommand(get)

	return cmd
}
