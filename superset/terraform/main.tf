provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.region}"
}


resource "aws_instance" "superset" {
  ami = "ami-0957ba512eafd08d9"
  instance_type = "t2.small"
  subnet_id = "subnet-6b0c0926"
  security_groups = ["${aws_security_group.superset.id}"]
  key_name = "terraform"
  tags = {
    Name = "superset"
  }
  ebs_block_device {
    device_name = "/dev/sdg"
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }
}

resource "aws_eip" "superset_eip" {
  instance = "${aws_instance.superset.id}"
}

resource "aws_security_group" "superset" {
  name        = "superset"
  description = "Security Group para Superset"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["212.66.164.13/32", "88.6.135.178/32", "80.36.58.187/32", "83.39.179.89/32"]
  }

  ingress {
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = ["212.66.164.13/32", "88.6.135.178/32", "80.36.58.187/32", "83.39.179.89/32"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "pruebas_terraform"
  }
}
