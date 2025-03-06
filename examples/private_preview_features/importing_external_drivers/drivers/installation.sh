#!/bin/bash

# Script to install libpq5 and libpq-dev on Debian/Ubuntu or CentOS/RHEL/Fedora systems.

# Detect OS
OS=$(uname -s)
DISTRO=$(awk -F= '$1=="NAME" {print $2}' /etc/os-release | tr -d '"')

echo '{"level":"INFO", "message": "Detected OS: '$OS'", "message-origin": "installation_sh"}'
echo '{"level":"INFO", "message": "Detected Distribution: '$DISTRO'", "message-origin": "installation_sh"}'

# Update package lists
update_packages() {
  if [[ "$OS" == "Linux" ]]; then
    if [[ "$DISTRO" == *"Debian"* || "$DISTRO" == *"Ubuntu"* ]]; then
      sudo apt-get update
    elif [[ "$DISTRO" == *"CentOS"* || "$DISTRO" == *"RHEL"* ]]; then
      sudo yum update -y
    elif [[ "$DISTRO" == *"Fedora"* ]]; then
      sudo dnf update -y
    else
      echo '{"level":"INFO", "message": "Unsupported Linux distribution.", "message-origin": "installation_sh"}'
      exit 1
    fi
  else
    echo '{"level":"INFO", "message": "Unsupported OS.", "message-origin": "installation_sh"}'
    exit 1
  fi
}

# Install libpq5
install_libpq5() {
  if [[ "$OS" == "Linux" ]]; then
    if [[ "$DISTRO" == *"Debian"* || "$DISTRO" == *"Ubuntu"* ]]; then
      sudo apt-get install -y libpq5 libpq-dev
    elif [[ "$DISTRO" == *"CentOS"* || "$DISTRO" == *"RHEL"* ]]; then
      sudo yum install -y postgresql-libs postgresql-devel # This package provides libpq on RHEL/CentOS
    elif [[ "$DISTRO" == *"Fedora"* ]]; then
        sudo dnf install -y postgresql-libs postgresql-devel #This package provides libpq on Fedora
    else
      echo '{"level":"INFO", "message": "Unsupported Linux distribution.", "message-origin": "installation_sh"}'
      exit 1
    fi
  else
    echo '{"level":"INFO", "message": "Unsupported OS.", "message-origin": "installation_sh"}'
    exit 1
  fi
}

# Main execution
update_packages
install_libpq5

echo '{"level":"INFO", "message": "libpq5 installation complete.", "message-origin": "installation_sh"}'