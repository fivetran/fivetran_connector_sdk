#!/bin/bash

# Script to install python3-dev, default-libmysqlclient-dev, build-essential on Debian/Ubuntu systems.

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

# Install libpgtypes
install_libmysqlclients() {
  if [[ "$OS" == "Linux" ]]; then
    sudo apt-get install -y python3-dev default-libmysqlclient-dev build-essential
  else
    echo '{"level":"INFO", "message": "Unsupported OS.", "message-origin": "installation_sh"}'
    exit 1
  fi
}

# Main execution
update_packages
install_libmysqlclients

echo '{"level":"INFO", "message": "All python3-dev, default-libmysqlclient-dev, build-essential installations complete.", "message-origin": "installation_sh"}'