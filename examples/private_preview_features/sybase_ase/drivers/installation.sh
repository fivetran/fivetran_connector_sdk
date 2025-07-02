#!/bin/bash

set -e

echo "Updating package list and installing required packages..."
apt-get update && apt-get install -y \
    freetds-dev \
    freetds-bin \
    unixodbc \
    unixodbc-dev \
    tdsodbc \
    curl \
    gcc \
    g++ \
    make

echo "Registering FreeTDS ODBC driver in /etc/odbcinst.ini..."
cat <<EOF >> /etc/odbcinst.ini

[FreeTDS]
Description=FreeTDS ODBC Driver for Sybase ASE
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
EOF

echo "Installation complete."