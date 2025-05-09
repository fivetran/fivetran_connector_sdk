#!/bin/bash

set -e  # Exit on any error

# 1. Install appropriate JDK
echo "Installing JDK..."
if apt-cache search openjdk-17-jdk | grep -q openjdk-17-jdk; then
  apt-get install -y openjdk-17-jdk
elif apt-cache search openjdk-17-jre | grep -q openjdk-17-jre; then
  apt-get install -y openjdk-17-jre
elif apt-cache search default-jdk | grep -q default-jdk; then
  apt-get install -y default-jdk
else
  echo "Warning: Could not find a suitable JDK package. JayDeBeAPI requires Java to work."
  echo "Please install Java manually before continuing."
fi

# 2. Download JDBC Driver ZIP
echo "Downloading Informix JDBC driver..."
mkdir -p /opt/informix
curl -L -o /opt/informix/informixjdbcdriver.zip https://dbschema.com/jdbc-drivers/informixjdbcdriver.zip

# 3. Unzip the JDBC driver
echo "Unzipping JDBC driver..."
unzip /opt/informix/informixjdbcdriver.zip -d /opt/informix

echo "JDBC driver unzipped to /opt/informix"
echo "Installation of Informix JDBC driver complete."