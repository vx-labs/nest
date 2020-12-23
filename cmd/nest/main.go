package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/memberlist"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vx-labs/cluster"
	"github.com/vx-labs/nest/nest"
	"github.com/vx-labs/nest/nest/distributed"
	"github.com/vx-labs/wasp/v4/async"
	"github.com/vx-labs/wasp/v4/rpc"
	"github.com/vx-labs/wasp/v4/wasp/stats"
	"go.uber.org/zap"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func localPrivateHost() string {
	// Maybe we are running on fly.io
	flyLocalAddr, err := net.ResolveIPAddr("ip", "fly-local-6pn")
	if err == nil {
		return flyLocalAddr.IP.String()
	}
	// last attempt: use the default outgoing iface.
	conn, err := net.Dial("udp", "example.net:80")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func findPeers(name, tag string, minimumCount int) ([]string, error) {
	config := consulapi.DefaultConfig()
	config.HttpClient = http.DefaultClient
	client, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	var idx uint64
	for {
		services, meta, err := client.Catalog().Service(name, tag, &consulapi.QueryOptions{
			WaitIndex: idx,
			WaitTime:  10 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		idx = meta.LastIndex
		if len(services) < minimumCount {
			continue
		}
		out := make([]string, len(services))
		for idx := range services {
			out[idx] = fmt.Sprintf("%s:%d", services[idx].ServiceAddress, services[idx].ServicePort)
		}
		return out, nil
	}
}

func main() {
	config := viper.New()
	config.SetEnvPrefix("NEST")
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()
	cmd := cobra.Command{
		Use: "nest",
		PreRun: func(cmd *cobra.Command, _ []string) {
			config.BindPFlags(cmd.Flags())
			if !cmd.Flags().Changed("serf-advertized-port") {
				config.Set("serf-advertized-port", config.Get("serf-port"))
			}
			if !cmd.Flags().Changed("raft-advertized-port") {
				config.Set("raft-advertized-port", config.Get("raft-port"))
			}

		},
		Run: func(cmd *cobra.Command, _ []string) {
			ctx, cancel := context.WithCancel(context.Background())
			ctx = nest.StoreLogger(ctx, getLogger(config))
			err := os.MkdirAll(config.GetString("data-dir"), 0700)
			if err != nil {
				nest.L(ctx).Fatal("failed to create data directory", zap.Error(err))
			}
			id, err := loadID(config.GetString("data-dir"))
			if err != nil {
				nest.L(ctx).Fatal("failed to get node ID", zap.Error(err))
			}
			ctx = nest.AddFields(ctx, zap.String("hex_node_id", fmt.Sprintf("%x", id)))
			if config.GetBool("pprof") {
				address := fmt.Sprintf("%s:%d", config.GetString("pprof-address"), config.GetInt("pprof-port"))
				go func() {
					mux := http.NewServeMux()
					mux.HandleFunc("/debug/pprof/", pprof.Index)
					mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
					mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
					mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
					mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
					panic(http.ListenAndServe(address, mux))
				}()
				nest.L(ctx).Info("started pprof", zap.String("pprof_url", fmt.Sprintf("http://%s/", address)))
			}
			operations := async.NewOperations(ctx, nest.L(ctx))
			healthServer := health.NewServer()
			healthServer.SetServingStatus("node", healthpb.HealthCheckResponse_SERVING)
			healthServer.SetServingStatus("rpc", healthpb.HealthCheckResponse_NOT_SERVING)

			if config.GetString("rpc-tls-certificate-file") == "" || config.GetString("rpc-tls-private-key-file") == "" {
				nest.L(ctx).Warn("TLS certificate or private key not provided. GRPC transport security will use a self-signed generated certificate.")
			}
			server := rpc.Server(rpc.ServerConfig{
				VerifyClientCert:            config.GetBool("mtls"),
				TLSCertificateAuthorityPath: config.GetString("rpc-tls-certificate-authority-file"),
				TLSCertificatePath:          config.GetString("rpc-tls-certificate-file"),
				TLSPrivateKeyPath:           config.GetString("rpc-tls-private-key-file"),
			})
			healthpb.RegisterHealthServer(server, healthServer)
			rpcDialer := rpc.GRPCDialer(rpc.ClientConfig{
				InsecureSkipVerify:          config.GetBool("insecure"),
				TLSCertificatePath:          config.GetString("rpc-tls-certificate-file"),
				TLSPrivateKeyPath:           config.GetString("rpc-tls-private-key-file"),
				TLSCertificateAuthorityPath: config.GetString("rpc-tls-certificate-authority-file"),
			})
			clusterListener, err := net.Listen("tcp", net.JoinHostPort("::", fmt.Sprintf("%d", config.GetInt("raft-port"))))
			if err != nil {
				nest.L(ctx).Fatal("cluster listener failed to start", zap.Error(err))
			}

			joinList := config.GetStringSlice("join-node")
			if config.GetBool("consul-join") {
				discoveryStarted := time.Now()
				consulJoinList, err := findPeers(
					config.GetString("consul-service-name"), config.GetString("consul-service-tag"),
					config.GetInt("raft-bootstrap-expect"))
				if err != nil {
					nest.L(ctx).Fatal("failed to find other peers on Consul", zap.Error(err))
				}
				nest.L(ctx).Debug("discovered nodes using Consul",
					zap.Duration("consul_discovery_duration", time.Since(discoveryStarted)), zap.Int("node_count", len(consulJoinList)))
				joinList = append(joinList, consulJoinList...)
			}
			var clusterMultiNode cluster.MultiNode

			bcast := &memberlist.TransmitLimitedQueue{
				RetransmitMult: 3,
				NumNodes: func() int {
					if clusterMultiNode == nil {
						return 1
					}
					return clusterMultiNode.Gossip().MemberCount()
				},
			}
			dstate := distributed.NewState(id, bcast)

			raftConfig := cluster.RaftConfig{
				ExpectedNodeCount: config.GetInt("raft-bootstrap-expect"),
				Network: cluster.NetworkConfig{
					AdvertizedHost: config.GetString("raft-advertized-address"),
					AdvertizedPort: config.GetInt("raft-advertized-port"),
					ListeningPort:  config.GetInt("raft-port"),
				},
			}
			clusterMultiNode = cluster.NewMultiNode(cluster.NodeConfig{
				ID:            id,
				ServiceName:   "nest",
				DataDirectory: config.GetString("data-dir"),
				GossipConfig: cluster.GossipConfig{
					JoinList:                 joinList,
					WANMode:                  true,
					DistributedStateDelegate: dstate.Distributor(),
					Network: cluster.NetworkConfig{
						AdvertizedHost: config.GetString("serf-advertized-address"),
						AdvertizedPort: config.GetInt("serf-advertized-port"),
						ListeningPort:  config.GetInt("serf-port"),
					},
				},
				RaftConfig: raftConfig,
			}, rpcDialer, server, nest.L(ctx))
			shardsController := nest.NewShardsController(id, config.GetString("data-dir"), clusterMultiNode, raftConfig, dstate)
			frontend := nest.NewUserFrontend(shardsController)
			shardsController.Serve(server)
			frontend.Serve(server)

			messageLog, err := nest.NewMessageLog(ctx, shardsController, "messages", nest.L(ctx))
			if err != nil {
				nest.L(ctx).Fatal("failed to create message log", zap.Error(err))
			}
			eventsLog, err := nest.NewEventsLog(ctx, shardsController, "events", nest.L(ctx))
			if err != nil {
				nest.L(ctx).Fatal("failed to create events log", zap.Error(err))
			}
			waspReceiver := nest.NewWaspReceiver(messageLog)
			waspAuditReceived := nest.NewWaspAuditRecorder(eventsLog)
			messageServer := nest.NewServer(messageLog)
			eventsServer := nest.NewEventsServer(eventsLog)
			messageServer.Serve(server)
			eventsServer.Serve(server)
			waspReceiver.Serve(server)
			waspAuditReceived.Serve(server)

			operations.Run("cluster server", func(ctx context.Context) {
				shardsController.Run(ctx)
			})
			operations.Run("cluster listener", func(ctx context.Context) {
				err := server.Serve(clusterListener)
				if err != nil {
					nest.L(ctx).Fatal("cluster listener crashed", zap.Error(err))
				}
			})

			go stats.ListenAndServe(config.GetInt("metrics-port"))
			go func() {
				mux := http.NewServeMux()
				addr := net.JoinHostPort("::", fmt.Sprintf("%d", config.GetInt("health-port")))
				mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					out, err := healthServer.Check(r.Context(), &healthpb.HealthCheckRequest{
						Service: "rpc",
					})
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
						json.NewEncoder(w).Encode(err)
						return
					}
					switch out.Status {
					case healthpb.HealthCheckResponse_SERVING:
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(`{"status": "passing", "msg":"service is running"}`))
					case healthpb.HealthCheckResponse_SERVICE_UNKNOWN:
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte(`{"status": "not_passing", "msg":"service unknown"}`))
					case healthpb.HealthCheckResponse_NOT_SERVING:
						w.WriteHeader(http.StatusTooManyRequests)
						w.Write([]byte(`{"status": "warning", "msg":"service is not serving"}`))
					case healthpb.HealthCheckResponse_UNKNOWN:
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte(`{"status": "not_passing", "msg":"unknown failure"}`))
					}
				}))
				http.ListenAndServe(addr, mux)
			}()
			healthServer.Resume()
			shardsController.WaitReady(ctx)

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT)
			select {
			case <-sigc:
			}
			if config.GetBool("unclean-shutdown") {
				nest.L(ctx).Info("nest unclean shutdown requested")
				os.Exit(0)
			}
			nest.L(ctx).Info("nest shutdown initiated")
			err = shardsController.StopCluster(ctx)
			if err != nil {
				nest.L(ctx).Error("failed to shutdown cluster server", zap.Error(err))
			} else {
				nest.L(ctx).Debug("cluster server stopped")
			}
			err = clusterMultiNode.Shutdown()
			if err != nil {
				nest.L(ctx).Error("failed to shutdown cluster", zap.Error(err))
			} else {
				nest.L(ctx).Debug("cluster stopped")
			}
			healthServer.Shutdown()
			nest.L(ctx).Debug("health server stopped")
			go func() {
				<-time.After(1 * time.Second)
				server.Stop()
			}()
			server.GracefulStop()
			nest.L(ctx).Debug("rpc server stopped")
			clusterListener.Close()
			nest.L(ctx).Debug("rpc listener stopped")
			cancel()
			operations.Wait()
			nest.L(ctx).Debug("asynchronous operations stopped")
			err = shardsController.Close()
			if err != nil {
				nest.L(ctx).Error("failed to close shards", zap.Error(err))
			} else {
				nest.L(ctx).Debug("shards closed")
			}
			nest.L(ctx).Info("nest successfully stopped")
		},
	}
	defaultIP := localPrivateHost()
	cmd.Flags().Bool("pprof", false, "Start pprof endpoint.")
	cmd.Flags().Int("pprof-port", 8080, "Profiling (pprof) port.")
	cmd.Flags().String("pprof-address", "127.0.0.1", "Profiling (pprof) port.")
	cmd.Flags().Bool("debug", false, "Use a fancy logger and increase logging level.")
	cmd.Flags().Bool("mtls", false, "Enforce GRPC service-side TLS certificates validation for client connections.")
	cmd.Flags().Bool("insecure", false, "Disable GRPC client-side TLS validation.")
	cmd.Flags().Bool("consul-join", false, "Use Hashicorp Consul to find other gossip members. Nest won't handle service registration in Consul, you must do it before running Nest.")
	cmd.Flags().String("consul-service-name", "Nest", "Consul auto-join service name.")
	cmd.Flags().String("consul-service-tag", "gossip", "Consul auto-join service tag.")

	cmd.Flags().Int("health-port", 8090, "Start Healthcheck HTTP server on this port.")
	cmd.Flags().Int("metrics-port", 0, "Start Prometheus HTTP metrics server on this port.")
	cmd.Flags().Int("serf-port", 2799, "Membership (Serf) port.")
	cmd.Flags().Int("raft-port", 2899, "Clustering (Raft) port.")
	cmd.Flags().String("serf-advertized-address", defaultIP, "Advertize this adress to other gossip members.")
	cmd.Flags().String("raft-advertized-address", defaultIP, "Advertize this adress to other raft nodes.")
	cmd.Flags().Int("serf-advertized-port", 2799, "Advertize this port to other gossip members.")
	cmd.Flags().Int("raft-advertized-port", 2899, "Advertize this port to other raft nodes.")
	cmd.Flags().StringSliceP("join-node", "j", nil, "Join theses nodes to form a cluster.")
	cmd.Flags().StringP("data-dir", "d", "/tmp/nest", "Nest persistent data location.")

	cmd.Flags().IntP("raft-bootstrap-expect", "n", 1, "Nest will wait for this number of nodes to be available before bootstraping a cluster.")

	cmd.Flags().String("rpc-tls-certificate-authority-file", "", "x509 certificate authority used by RPC Server.")
	cmd.Flags().String("rpc-tls-certificate-file", "", "x509 certificate used by RPC Server.")
	cmd.Flags().String("rpc-tls-private-key-file", "", "Private key used by RPC Server.")

	cmd.Flags().Bool("unclean-shutdown", false, "Do not attempt to leave raft cluster, nor to close commitlog when shutting down.")

	cmd.AddCommand(TLSHelper(config))
	cmd.Execute()
}
