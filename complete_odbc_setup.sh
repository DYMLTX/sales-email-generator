#!/bin/bash

echo "Installing complete ODBC setup..."
echo "You'll need to run this with sudo permissions."
echo ""

# Install odbcinst tool and development files
sudo apt-get install -y odbcinst odbcinst1debian2 unixodbc-dev

# Check if msodbcsql18 is installed
if ! dpkg -l | grep -q msodbcsql18; then
    echo "Installing Microsoft ODBC Driver 18..."
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    sudo apt-get update
    sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
fi

echo ""
echo "Verifying installation..."
odbcinst -j

echo ""
echo "Available drivers:"
odbcinst -q -d

echo ""
echo "Complete! You should now be able to connect to Azure SQL."