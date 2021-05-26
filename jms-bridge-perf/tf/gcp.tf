/////// INSTANCES ///////

// The controller instance, contains prometheus and grafana
resource "google_compute_instance" "controller" {
  name = "bridge-perf-controller"
  machine_type = "e2-standard-4"
  zone = var.zone

  tags = ["bridge-perf"]
  labels = {
    "bridge-perf" = "true"
    "node" = "controller"
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  service_account {
    scopes = ["compute-ro"]
  }

  boot_disk {
    initialize_params {
      image = var.image
   }
  }

  network_interface {
    network = google_compute_network.bridge-perf-net.name
    access_config {
      // Include this section to give the VM an external ip address
    }
  }
}

// The JMS bridge instance
resource "google_compute_instance" "bridge" {
  name = "bridge-perf-bridge"
  machine_type = "e2-standard-8"
  zone = "us-central1-a"

  tags = ["bridge-perf"]
  labels = {
    "bridge-perf" = "true"
    "prometheus" = "true"
    "node_exporter" = "true"
    "jmx_exporter" = "true"
    "node" = "bridge"
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  boot_disk {
    initialize_params {
      image = var.image
      size = 128
      type = "pd-ssd"
    }
  }

  network_interface {
    network = google_compute_network.bridge-perf-net.name

    access_config {
      // Include this section to give the VM an external ip address
    }
  }
}

// JMeter instances
resource "google_compute_instance" "tester" {
  count = 4
  name = "bridge-perf-tester-${count.index}"
  machine_type = "e2-standard-4"
  zone = "us-central1-a"

  tags = ["bridge-perf"]
  labels = {
    "bridge-perf" = "true"
    "prometheus" = "true"
    "node_exporter" = "true"
    "jmx_exporter" = "true"
    "node" = "perf"
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  boot_disk {
    initialize_params {
      image = var.image
    }
  }

  network_interface {
    network = google_compute_network.bridge-perf-net.name

    access_config {
      // Include this section to give the VM an external ip address
    }
  }
}


//////// NETWORKING ////////

resource "google_compute_network" "bridge-perf-net" {
  name = "bridge-perf-net"
}

resource "google_compute_firewall" "bridge-perf-fw-icmp" {
  name    = "bridge-perf-fw-icmp"
  network = google_compute_network.bridge-perf-net.name

  allow {
    protocol = "icmp"
  }

}

resource "google_compute_firewall" "bridge-perf-fw-ssh" {
  name    = "bridge-perf-fw-ssh"
  network = google_compute_network.bridge-perf-net.name
  direction = "INGRESS"
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports = ["22"]
  }
}

resource "google_compute_firewall" "bridge-perf-fw-internal" {
  name    = "bridge-perf-fw-internal"
  network = google_compute_network.bridge-perf-net.name
  direction = "INGRESS"
  source_tags = ["bridge-perf"]

  allow {
    protocol = "tcp"
    ports = ["9100", "8888", "61616"]
  }
}
///// OS Login SSH Key /////

data "google_client_openid_userinfo" "me" {
}

resource "google_os_login_ssh_public_key" "cache" {
  user = data.google_client_openid_userinfo.me.email
  key = file(var.ssh_key)
}

resource "google_project_iam_member" "role-binding" {
  project = var.project
  role = "roles/compute.osAdminLogin"
  member = "user:${data.google_client_openid_userinfo.me.email}"
}

