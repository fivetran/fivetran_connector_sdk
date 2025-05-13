#!/bin/bash

set -e  # Exit on any error

apt-get update

# 1. Install appropriate JDK
echo "Installing JDK..."
apt-get install -y openjdk-17-jre
echo "JRE 17 installed successfully."

echo "Downloading Informix JDBC jar and BSON jar from Maven Central..."
mkdir -p /opt/informix
curl -L -o /opt/informix/jdbc-15.0.0.1.1.jar \
  https://repo1.maven.org/maven2/com/ibm/informix/jdbc/15.0.0.1.1/jdbc-15.0.0.1.1.jar

curl -L -o /opt/informix/bson-3.8.0.jar \
  https://repo1.maven.org/maven2/org/mongodb/bson/5.5.0/bson-3.8.0.jar

echo "Installation of Informix JDBC driver complete."