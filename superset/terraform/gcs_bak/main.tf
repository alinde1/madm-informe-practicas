provider "google" {
 credentials = "${file("../../../MADM-Infrastructure-898b56a72b78.json")}"
 project     = "reliable-osprey-108514"
 region      = "us-west1"
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
 byte_length = 1
}

resource "random_id" "instance_id_2" {
 byte_length = 1
}

// A single Google Cloud Engine instance
resource "google_compute_instance" "web-server" {
 name         = "alm072-instance-${random_id.instance_id.hex}"
 machine_type = "g1-small"
 zone         = "us-west1-a"
 allow_stopping_for_update = true

 boot_disk {
   initialize_params {
     image = "centos-cloud/centos-7"
     size = 20
     type = "pd-standard"
   }
 }

// Make sure flask is installed on all new instances for later steps
 metadata_startup_script = "sudo yum update; sudo yum install -yq python36 python36-pip rsync firefox screen; sudo pip3.6 install selenium pickledb"

 metadata {
   sshKeys = "alinde:${file("~/.ssh/id_rsa.pub")}"
 }

 network_interface {
   network = "superset-network"

   access_config {
     // Include this section to give the VM an external ip address
   }
 }
}

resource "google_compute_instance" "database" {
 name         = "alm072-instance-${random_id.instance_id_2.hex}"
 machine_type = "g1-small"
 zone         = "us-west1-a"
 allow_stopping_for_update = true

 boot_disk {
   initialize_params {
     image = "centos-cloud/centos-7"
     size = 20
     type = "pd-standard"
   }
 }

// Make sure flask is installed on all new instances for later steps
 metadata_startup_script = "sudo yum update; sudo yum install -yq python36 python36-pip rsync firefox screen; sudo pip3.6 install selenium pickledb"

 metadata {
   sshKeys = "alinde:${file("~/.ssh/id_rsa.pub")}"
 }

 network_interface {
   network = "superset-network"

   access_config {
     // Include this section to give the VM an external ip address
   }
 }
}

resource "google_compute_firewall" "superset" {
  name    = "superset-firewall"
  network = "${google_compute_network.superset.name}"

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["8088","22","5432"]
  }

  #source_tags = ["superset"]
}

resource "google_compute_network" "superset" {
  name = "superset-network"
}

output "ip" {
 value = "${google_compute_instance.web-server.network_interface.0.access_config.0.nat_ip}"
}
