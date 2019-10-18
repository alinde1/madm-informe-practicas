# Deploy a host with Superset.

1. Create a VM with terraform.

```buildoutcfg
[alinde@localhost superset]$ cd terraform
[alinde@localhost superset]$ terraform apply
```

1. Install and configure Superset with Ansible.

```buildoutcfg
[alinde@localhost superset]$ cd ansible
[alinde@localhost superset]$ TF_STATE=../terraform ansible-playbook -i ~/bin/terraform-inventory -u centos site.yml
```