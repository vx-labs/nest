job "${deployment_name}" {
  datacenters = ["dc1"]
  type        = "service"

  update {
    max_parallel     = 1
    min_healthy_time = "30s"
    progress_deadline = "60m"
    healthy_deadline = "30m"
    auto_revert      = true
    canary           = 0
  }

  group "nest" {
    vault {
      policies      = ["nomad-tls-storer"]
      change_mode   = "signal"
      change_signal = "SIGUSR1"
      env           = false
    }

    count = 3

    restart {
      attempts = 3
      interval = "5m"
      delay    = "15s"
      mode     = "delay"
    }

    ephemeral_disk {
      size = 2000
    }

    task "mqtt-collector" {
      shutdown_delay = "5s"
      kill_timeout = "30s"
      driver = "docker"

      env {
        HTTPS_PROXY="http://http.proxy.discovery.fr-par.vx-labs.net:3128"
        CONSUL_HTTP_ADDR = "$${NOMAD_IP_rpc}:8500"
        VAULT_ADDR       = "http://active.vault.service.consul:8200/"
      }

      template {
        change_mode = "restart"
        destination = "local/environment"
        env         = true

        data = <<EOH
{{with secret "secret/data/vx/mqtt"}}
NEST_MQTT_COLLECTOR_BROKER_PASSWORD="{{ .Data.static_tokens }}"
NEST_RPC_TLS_CERTIFICATE_FILE="{{ env "NOMAD_TASK_DIR" }}/cert.pem"
NEST_RPC_TLS_PRIVATE_KEY_FILE="{{ env "NOMAD_TASK_DIR" }}/key.pem"
NEST_RPC_TLS_CERTIFICATE_AUTHORITY_FILE="{{ env "NOMAD_TASK_DIR" }}/ca.pem"
no_proxy="10.0.0.0/8,172.16.0.0/12,*.service.consul"
{{end}}
        EOH
      }

      template {
        change_mode = "restart"
        destination = "local/cert.pem"
        splay       = "1h"

        data = <<EOH
{{- $cn := printf "common_name=%s" (env "NOMAD_ALLOC_ID") -}}
{{- $ipsans := printf "ip_sans=%s" (env "NOMAD_IP_rpc") -}}
{{- $sans := printf "alt_names=messages-beta.iot.cloud.vx-labs.net" -}}
{{- $path := printf "pki/issue/grpc" -}}
{{ with secret $path $cn $ipsans $sans "ttl=48h" }}{{ .Data.certificate }}{{ end }}
EOH
      }

      template {
        change_mode = "restart"
        destination = "local/key.pem"
        splay       = "1h"

        data = <<EOH
{{- $cn := printf "common_name=%s" (env "NOMAD_ALLOC_ID") -}}
{{- $ipsans := printf "ip_sans=%s" (env "NOMAD_IP_rpc") -}}
{{- $sans := printf "alt_names=messages-beta.iot.cloud.vx-labs.net" -}}
{{- $path := printf "pki/issue/grpc" -}}
{{ with secret $path $cn $ipsans $sans "ttl=48h" }}{{ .Data.private_key }}{{ end }}
EOH
      }

      template {
        change_mode = "restart"
        destination = "local/ca.pem"
        splay       = "1h"

        data = <<EOH
{{- $cn := printf "common_name=%s" (env "NOMAD_ALLOC_ID") -}}
{{- $ipsans := printf "ip_sans=%s" (env "NOMAD_IP_rpc") -}}
{{- $sans := printf "alt_names=messages-beta.iot.cloud.vx-labs.net" -}}
{{- $path := printf "pki/issue/grpc" -}}
{{ with secret $path $cn $ipsans $sans "ttl=48h" }}{{ .Data.issuing_ca }}{{ end }}
EOH
      }

      config {
        logging {
          type = "fluentd"

          config {
            fluentd-address = "localhost:24224"
            tag             = "${deployment_name}"
          }
        }

        image = "${service_image}:${service_version}"
        args = [
          "--data-dir", "$${NOMAD_TASK_DIR}",
          "--mtls",
          "--raft-bootstrap-expect", "3",
          "--consul-join",
          "--consul-service-name", "${deployment_name}",
          "--consul-service-tag", "gossip",
          "--metrics-port", "8089",
          "--health-port", "8090",
          "--raft-advertized-address", "$${NOMAD_IP_rpc}", "--raft-advertized-port", "$${NOMAD_HOST_PORT_rpc}",
          "--serf-advertized-address", "$${NOMAD_IP_gossip}", "--serf-advertized-port", "$${NOMAD_HOST_PORT_gossip}",
        ]
        force_pull = true

        port_map {
          gossip  = 2799
          rpc     = 2899
          metrics = 8089
          health = 8090
        }
      }

      resources {
        cpu    = 1024
        memory = 256

        network {
          mbits = 10
          port "rpc" {}
          port "health" {}
          port "gossip" {}
          port "metrics" {}
        }
      }

      service {
        name = "${deployment_name}"
        port = "rpc"
        tags = [
          "rpc",
          "${service_version}",
          "traefik.enable=true",
          "traefik.tcp.routers.${deployment_name}.rule=HostSNI(`messages-beta.iot.cloud.vx-labs.net`)",
          "traefik.tcp.routers.${deployment_name}.entrypoints=https",
          "traefik.tcp.routers.${deployment_name}.service=${deployment_name}",
          "traefik.tcp.routers.${deployment_name}.tls",
          "traefik.tcp.routers.${deployment_name}.tls.passthrough=true",
        ]
        check {
          type     = "http"
          path     = "/health?service=rpc"
          port     = "health"
          interval = "30s"
          timeout  = "2s"
        }
      }
      service {
        name = "${deployment_name}"
        port = "gossip"
        tags = [
          "gossip",
          "${service_version}",
        ]
      }
      service {
        name = "${deployment_name}"
        port = "metrics"
        tags = ["prometheus", "${service_version}"]

        check {
          type     = "http"
          path     = "/metrics"
          port     = "metrics"
          interval = "30s"
          timeout  = "2s"
        }
      }
    }
  }
}
