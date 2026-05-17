job "BD-Salesforce-Service-Stage-App" {
  datacenters = ["glynac-dc"]
  type        = "service"
  namespace   = "extraction-service"

  update {
    max_parallel     = 1
    health_check     = "task_states"
    min_healthy_time = "30s"
  }

  group "black-diamond-salesforce-stage-service" {
    count = 1
    shutdown_delay = "15s"

    network {
      port "http" {
        static       = 5711
        to           = 5711
        host_network = "private"
      }
    }

    # 🔌 Consul Service Discovery Registration
    service {
      name = "black-diamond-salesforce-service-staging"
      tags = ["apps", "logs.promtail"]
      port = "http"
      
      # 🩺 Live Health Check Hook pointing to the route we just verified
      check {
        name     = "api-health"
        type     = "http"
        path     = "/v1/sync/health" # Adjusted to match your FastAPI prefix path
        interval = "30s"
        timeout  = "10s"
      }
    }

    constraint {
      attribute = "${attr.unique.hostname}"
      value     = "Worker-08"
    }

    task "black-diamond-salesforce-service" {
      driver = "docker"

      config {
        image       = "harbor-registry.service.consul:8085/black-diamond/black-diamond-salesforce-service:IMAGE_TAG_PLACEHOLDER"
        ports       = ["http"]
        dns_servers = ["172.17.0.1", "172.18.0.1", "8.8.8.8", "8.8.4.4", "1.1.1.1"]
      }

      # 🛡️ Authenticate with HashiCorp Vault
      vault {
        role = "blackdiamond"
      }

      # 🔑 Vault Secret Injection Template
      # This block pulls values from Vault at launch and exposes them as native environment variables
      template {
        destination = "secrets/env"
        env         = true
        data        = <<EOF
ENVIRONMENT="stage"
PORT="5711"
HOST="0.0.0.0"
FLASK_ENV="production"
FLASK_DEBUG="False"
LOG_LEVEL="INFO"
LOG_FORMAT="json"

# Pull secrets dynamically from the Vault Staging engine path
{{ with secret "secrets/data/blackdiamond/blackdiamond-salesforce-service-staging" }}
SECRET_KEY="{{ .Data.data.SECRET_KEY }}"
SF_CONSUMER_KEY="{{ .Data.data.SF_CONSUMER_KEY }}"
SF_PRIVATE_KEY_PEM="{{ .Data.data.SF_PRIVATE_KEY_PEM }}"
SF_USERNAME="{{ .Data.data.SF_USERNAME }}"
SF_LOGIN_URL="{{ .Data.data.SF_LOGIN_URL }}"
SF_API_VERSION="{{ .Data.data.SF_API_VERSION }}"

MINIO_ENABLED="{{ .Data.data.MINIO_ENABLED }}"
MINIO_ENDPOINT="{{ .Data.data.MINIO_ENDPOINT }}"
MINIO_ACCESS_KEY="{{ .Data.data.MINIO_ACCESS_KEY }}"
MINIO_SECRET_KEY="{{ .Data.data.MINIO_SECRET_KEY }}"
MINIO_BUCKET="salesforce-stage"
MINIO_SECURE="true"

KAFKA_BOOTSTRAP_SERVERS="{{ .Data.data.KAFKA_BOOTSTRAP_SERVERS }}"
PII_MASKING_ENABLED="true"
{{ end }}
EOF
      }

      resources {
        cpu    = 512
        memory = 512
      }
    }
  }
}   