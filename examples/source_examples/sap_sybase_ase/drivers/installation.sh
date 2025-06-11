#!/bin/bash
set -e

echo "Installing FreeTDS and UnixODBC..."

apt-get update
apt-get install -y freetds-bin freetds-dev unixodbc unixodbc-dev tdsodbc

# Find the FreeTDS driver path
FREETDS_DRIVER_PATH=$(find /usr -name "libtdsodbc.so" 2>/dev/null | head -1)
if [ -z "$FREETDS_DRIVER_PATH" ]; then
    echo "FreeTDS ODBC driver not found. Installation may have failed."
    exit 1
fi

echo "FreeTDS driver found at: $FREETDS_DRIVER_PATH"

# Configure odbcinst.ini (driver definition)
echo "Configuring ODBC drivers..."
bash -c "cat > /etc/odbcinst.ini << EOL
[FreeTDS]
Description = FreeTDS Driver for Sybase/MS SQL
Driver = $FREETDS_DRIVER_PATH
Setup = $FREETDS_DRIVER_PATH
FileUsage = 1
EOL"

echo "Testing ODBC configuration..."
odbcinst -j

echo "FreeTDS setup complete!"