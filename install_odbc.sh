#!/bin/bash

echo "Installing Microsoft ODBC Driver for SQL Server..."
echo "You'll need to run this with sudo permissions."
echo ""

# Add Microsoft's GPG key
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

# Add Microsoft's repository
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update package list
sudo apt-get update

# Install ODBC driver and tools
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

echo ""
echo "ODBC driver installation complete!"
echo "You can now test the connection."