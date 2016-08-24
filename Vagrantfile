# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

MEMORY = 3072

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "centos/7"

  config.vm.provision :shell, path: "vagrant/provision.sh"

  config.vm.provider "libvirt" do |v|
    v.memory = MEMORY
  end
end
