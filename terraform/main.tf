terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

# Existing network where Kafka and Schema Registry are running.
# Terraform will NOT create or destroy this network.
data "docker_network" "java_default" {
  name = "java_default"
}

resource "docker_image" "flink" {
  name = "flink:1.20-java17"
}

# --------------------------------------------------
# Flink JobManager
# --------------------------------------------------

resource "docker_container" "flink_jobmanager" {
  name  = "flink-jobmanager"
  image = docker_image.flink.image_id

  command = [
    "jobmanager"
  ]

  env = [
    "FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager"
  ]

  ports {
    internal = 8081
    external = 8082
  }

  networks_advanced {
    name = data.docker_network.java_default.name
  }

volumes {
  host_path      = "/home/raj-kumar/flink-jobs"
  container_path = "/opt/flink/usrlib"
}

  # Host directory -> Container directory
  volumes {
    host_path      = "/tmp/flink-checkpoints"
    container_path = "/tmp/flink-checkpoints"
  }

  restart = "unless-stopped"
}

# --------------------------------------------------
# Flink TaskManager
# --------------------------------------------------

resource "docker_container" "flink_taskmanager" {
  name  = "flink-taskmanager"
  image = docker_image.flink.image_id

  command = [
    "taskmanager"
  ]

  env = [
    "FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager"
  ]

  networks_advanced {
    name = data.docker_network.java_default.name
  }

    volumes {
      host_path      = "/home/raj-kumar/flink-jobs"
      container_path = "/opt/flink/usrlib"
    }

  # Same host directory -> Same container directory
  volumes {
    host_path      = "/tmp/flink-checkpoints"
    container_path = "/tmp/flink-checkpoints"
  }

  depends_on = [
    docker_container.flink_jobmanager
  ]

  restart = "unless-stopped"
}