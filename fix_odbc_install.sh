#!/bin/bash

echo "Fixing ODBC installation..."
echo "You'll need to run this with sudo permissions."
echo ""

# Install the correct packages for Ubuntu 24.04
echo "Installing ODBC libraries..."
sudo apt-get update
sudo apt-get install -y unixodbc unixodbc-dev odbcinst

# Fix Microsoft GPG key issue
echo ""
echo "Fixing Microsoft repository key..."
curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /usr/share/keyrings/microsoft-prod.gpg > /dev/null

# Update the repository list with the correct signing key
echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update and install
echo ""
echo "Installing Microsoft ODBC Driver..."
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Verify installation
echo ""
echo "Verifying installation..."
echo "ODBC Configuration:"
odbcinst -j

echo ""
echo "Available drivers:"
odbcinst -q -d

echo ""
echo "Installation complete!"