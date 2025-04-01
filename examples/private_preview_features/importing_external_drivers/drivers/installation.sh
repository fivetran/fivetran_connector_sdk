#!/bin/bash

# Script to install libpq5 and libpq-dev on Debian/Ubuntu or CentOS/RHEL/Fedora systems.

# Detect OS
OS=$(uname -s)
# DISTRO=$(awk -F= '$1=="NAME" {print $2}' /etc/os-release | tr -d '"')

echo '{"level":"INFO", "message": "Detected OS: '$OS'", "message-origin": "installation_sh"}'

# Update package lists
update_packages() {
  if [[ "$OS" == "Linux" ]]; then
      sudo apt-get update
  else
    echo '{"level":"INFO", "message": "Unsupported OS.", "message-origin": "installation_sh"}'
    exit 1
  fi
}

# Install libpq5
install_libpq5() {
  if [[ "$OS" == "Linux" ]]; then
    sudo apt-get install -y libpq5 libpq-dev
  else
    echo '{"level":"INFO", "message": "Unsupported OS.", "message-origin": "installation_sh"}'
    exit 1
  fi
}

# Main execution
update_packages
install_libpq5

echo '{"level":"INFO", "message": "libpq5 installation complete.", "message-origin": "installation_sh"}'