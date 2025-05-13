#!/bin/bash

set -e  # Exit on any error

apt-get update

# 1. Install appropriate JDK
echo "Installing JDK..."
apt-get install -y openjdk-17-jre
echo "JRE 17 installed successfully."

# 2. Download JDBC Driver ZIP
echo "Downloading Informix JDBC driver..."
mkdir -p /opt/informix
curl -L -o /opt/informix/informixjdbcdriver.zip https://dbschema.com/jdbc-drivers/informixjdbcdriver.zip

# 3. Unzip the JDBC driver
echo "Unzipping JDBC driver..."
unzip /opt/informix/informixjdbcdriver.zip -d /opt/informix

echo "JDBC driver unzipped to /opt/informix"
echo "Installation of Informix JDBC driver complete."