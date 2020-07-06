package main

import (
	"context"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

const streamTemplate = `• {{ .Name | green }}
  Shards:
{{- range $shard := .ShardMetadatas}}
    • ID: {{ .ID | yellow }}
      Segment Count: {{ .SegmentCount | yellow }}
      Stored bytes: {{ .StoredBytes | yellow }}
      Current offset: {{ .CurrentOffset | yellow }}
{{- end }}`

func Streams(ctx context.Context, config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "streams",
	}

	sstCommand := &cobra.Command{
		Use:  "sst",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			var file io.Writer
			var err error
			if config.GetString("output") == "-" {
				file = cmd.OutOrStdout()
			} else {
				fd, err := os.OpenFile(config.GetString("output"), os.O_RDWR|os.O_CREATE, 0650)
				if err != nil {
					l.Fatal("failed to create tmp file to receive snapshot", zap.Error(err))
				}
				defer fd.Close()
				file = fd
			}
			stream, err := api.NewStreamsClient(conn).SST(ctx, &api.SSTRequest{
				Stream:     config.GetString("stream"),
				Shard:      config.GetUint64("shard"),
				FromOffset: config.GetUint64("from-offset"),
				ToOffset:   config.GetUint64("to-offset"),
			})
			if err != nil {
				l.Fatal("failed to start SST", zap.Error(err))
			}

			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					l.Fatal("failed to receive chunk", zap.Error(err))
				}
				_, err = file.Write(chunk.Chunk)
				if err != nil {
					l.Fatal("failed to write chunk", zap.Error(err))
				}
			}
		},
	}
	sstCommand.Flags().StringP("output", "o", "", "Local file output")
	sstCommand.MarkFlagRequired("output")
	sstCommand.Flags().StringP("stream", "s", "", "Stream name")
	sstCommand.MarkFlagRequired("stream")
	sstCommand.Flags().Uint64P("shard", "", 0, "Shard ID")
	sstCommand.Flags().Uint64P("from-offset", "", 0, "")
	sstCommand.Flags().Uint64P("to-offset", "", 0, "")

	cmd.AddCommand(sstCommand)

	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			out, err := api.NewStreamsClient(conn).ListStreams(ctx, &api.ListStreamsRequest{})
			if err != nil {
				l.Fatal("failed to list streams", zap.Error(err))
			}
			tpl := ParseTemplate(config.GetString("format"))
			sort.Slice(out.StreamMetadatas, func(i, j int) bool {
				return strings.Compare(out.StreamMetadatas[i].Name, out.StreamMetadatas[j].Name) == -1
			})
			for _, elt := range out.StreamMetadatas {
				tpl.Execute(cmd.OutOrStdout(), elt)
			}
		},
	}
	list.Flags().String("format", streamTemplate, "Format each event using Golang template format.")
	cmd.AddCommand(list)
	return cmd
}
