package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const streamTemplate = `{{ range $stream := .Streams -}}
• {{ .ID | yellow }}
  Name: {{ .Name }}
  Shards:
{{- range $shard := .Shards}}
    • {{ .ID | yellow }}
      Replicas [{{len .Replicas }}/{{$stream.DesiredReplicaCount}}]:
{{- range $replica := .Replicas}}
        • {{ if eq . $shard.Leader }}[L]{{ else }}[r]{{ end }} {{ . }}:
				{{- range $peer, $peerConfig := $.ShardReplicaState.Peers }}
					{{- if eq $peer $replica}}
						{{- range $shardState, $shardConfig := .Shards }}
							{{- if eq $shardState $shard.ID }}{{.Committed}}{{end}}
						{{- end}}
				{{- end}}
		{{- end}}
{{- end}}
{{ end }}{{ end }}`

func Streams(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "streams",
	}

	readCommand := &cobra.Command{
		Use:  "read",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			stream, err := api.NewNestClient(conn).GetEntries(ctx, &api.GetEntriesInput{
				StreamID:     config.GetString("stream"),
				ShardID:      config.GetUint64("shard"),
				FromOffset:   config.GetUint64("from-offset"),
				MaxBatchSize: 20,
				Watch:        true,
			})
			if err != nil {
				l.Fatal("failed to start stream", zap.Error(err))
			}
			l.Info("stream started")
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					l.Info("stream consumed")
					break
				}
				if err != nil {
					l.Fatal("failed to receive chunk", zap.Error(err))
				}
				for _, record := range chunk.Entries {
					fmt.Println(string(record.Payload))
				}
			}
		},
	}
	readCommand.Flags().StringP("stream", "s", "", "Stream name")
	readCommand.MarkFlagRequired("stream")
	readCommand.Flags().Uint64P("shard", "", 0, "Shard ID")
	readCommand.Flags().Uint64P("from-offset", "", 0, "")

	cmd.AddCommand(readCommand)

	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			out, err := api.NewNestClient(conn).ListStreams(ctx, &api.ListStreamsInput{})
			if err != nil {
				l.Fatal("failed to list streams", zap.Error(err))
			}
			tpl := ParseTemplate(config.GetString("format"))
			sort.Slice(out.Streams, func(i, j int) bool {
				return strings.Compare(out.Streams[i].ID, out.Streams[j].ID) == -1
			})
			err = tpl.Execute(cmd.OutOrStdout(), out)
			if err != nil {
				log.Print(err)

			}
		},
	}
	list.Flags().String("format", streamTemplate, "Format each event using Golang template format.")
	cmd.AddCommand(list)

	create := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			out, err := api.NewNestClient(conn).CreateStream(ctx, &api.CreateStreamInput{
				Name:                config.GetString("name"),
				DesiredReplicaCount: config.GetInt64("replica-count"),
				ShardCount:          config.GetInt64("shard-count"),
			})
			if err != nil {
				l.Fatal("failed to create stream", zap.Error(err))
			}
			log.Println(out.ID)
		},
	}
	create.Flags().StringP("name", "n", "", "The new stream name")
	create.MarkFlagRequired("name")
	create.Flags().Int64P("replica-count", "r", 3, "the new stream replica count")
	create.Flags().Int64P("shard-count", "s", 1, "the new stream shard count")

	cmd.AddCommand(create)

	put := &cobra.Command{
		Use:     "put",
		Aliases: []string{"write", "w"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			p := []byte(config.GetString("payload"))
			bench := config.GetBool("benchmark")
			client := api.NewNestClient(conn)
			input := &api.PutEntryInput{
				StreamID: config.GetString("name"),
				ShardKey: config.GetString("shard-key"),
				Payload:  p,
			}

			for {
				_, err := client.PutEntry(ctx, input)
				if err != nil {
					l.Fatal("failed to write stream", zap.Error(err))
				}
				if !bench {
					fmt.Printf("%d bytes written to %s\n", len(p), config.GetString("name"))
					return
				}
			}
		},
	}
	put.Flags().StringP("name", "n", "", "Stream name")
	put.MarkFlagRequired("name")
	put.Flags().StringP("shard-key", "s", "", "Shard key")
	put.MarkFlagRequired("shard-key")
	put.Flags().StringP("payload", "p", "", "Payload")
	put.MarkFlagRequired("payload")
	put.Flags().Bool("benchmark", false, "continuously write the payload")
	cmd.AddCommand(put)
	return cmd
}
