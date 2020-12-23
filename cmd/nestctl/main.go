package main

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cluster "github.com/vx-labs/cluster/clusterpb"
	"go.uber.org/zap"
)

func seedRand() {
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
}
func main() {
	seedRand()
	config := viper.New()
	config.AddConfigPath(configDir())
	config.SetConfigType("yaml")
	config.SetConfigName("config")
	config.SetEnvPrefix("NESTCTL")
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()

	ctx := context.Background()
	rootCmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			config.BindPFlags(cmd.Flags())
			config.BindPFlags(cmd.PersistentFlags())
			if err := config.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
					log.Fatal(err)
				}
			}
		},
	}
	clusterCommand := &cobra.Command{
		Use: "cluster",
	}
	list := &cobra.Command{
		Use: "ls",
		Run: func(cmd *cobra.Command, _ []string) {
			conn, l := mustDial(ctx, cmd, config)
			out, err := cluster.NewMultiRaftClient(conn).GetTopology(ctx, &cluster.GetTopologyRequest{
				ClusterID: "nest",
			})
			if err != nil {
				l.Fatal("failed to get raft topology", zap.Error(err))
			}
			table := getTable([]string{"ID", "Leader", "Address", "Healthchecks", "Suffrage", "Progress", "Latency"}, cmd.OutOrStdout())
			for _, member := range out.GetMembers() {
				healthString := "passing"
				suffrageString := "unknown"
				if !member.IsAlive {
					healthString = "error"
				}
				if member.IsVoter {
					suffrageString = "voter"
				} else {
					suffrageString = "learner"
				}
				table.Append([]string{
					fmt.Sprintf("%x", member.GetID()),
					fmt.Sprintf("%v", member.GetIsLeader()),
					member.GetAddress(),
					healthString,
					suffrageString,
					fmt.Sprintf("%d/%d", member.GetApplied(), out.Committed),
					fmt.Sprintf("%dms", member.LatencyMs),
				})
			}
			table.Render()
		},
	}
	clusterCommand.AddCommand(list)

	removeMember := &cobra.Command{
		Use: "remove-member",
		Run: func(cmd *cobra.Command, args []string) {
			conn, l := mustDial(ctx, cmd, config)
			id, err := strconv.ParseUint(args[0], 16, 64)
			if err != nil {
				l.Fatal("invalid node id specified", zap.Error(err))
			}
			_, err = cluster.NewMultiRaftClient(conn).RemoveMember(ctx, &cluster.RemoveMultiRaftMemberRequest{
				ClusterID: "nest",
				ID:        id,
			})
			if err != nil {
				l.Fatal("failed to remove raft member", zap.Error(err))
			}
		},
	}
	removeMember.Args = cobra.ExactArgs(1)
	clusterCommand.AddCommand(removeMember)

	hostname, _ := os.Hostname()

	rootCmd.AddCommand(clusterCommand)
	rootCmd.AddCommand(Streams(ctx, config))
	//rootCmd.AddCommand(Messages(ctx, config))
	rootCmd.AddCommand(Events(ctx, config))
	rootCmd.AddCommand(Topics(ctx, config))
	rootCmd.PersistentFlags().BoolP("insecure", "k", false, "Disable GRPC client-side TLS validation.")
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "Increase log verbosity.")
	rootCmd.PersistentFlags().BoolP("use-vault", "v", false, "Use Hashicorp Vault to generate GRPC Certificates.")
	rootCmd.PersistentFlags().String("vault-pki-path", "pki/issue/grpc", "Vault PKI certificate issuing path.")
	rootCmd.PersistentFlags().String("vault-pki-common-name", hostname, "Vault PKI certificate Common Name to submit.")
	rootCmd.PersistentFlags().BoolP("use-consul", "c", false, "Use Hashicorp Consul to find nest server.")
	rootCmd.PersistentFlags().String("consul-service-name", "nest", "Consul service name.")
	rootCmd.PersistentFlags().String("consul-service-tag", "rpc", "Consul service tag.")
	rootCmd.PersistentFlags().String("host", "127.0.0.1:1899", "remote GRPC endpoint")
	rootCmd.PersistentFlags().String("rpc-tls-certificate-authority-file", "", "x509 certificate authority used by RPC Client.")
	rootCmd.PersistentFlags().String("rpc-tls-certificate-file", "", "x509 certificate used by RPC Client.")
	rootCmd.PersistentFlags().String("rpc-tls-private-key-file", "", "Private key used by RPC Client.")
	rootCmd.Execute()
}
