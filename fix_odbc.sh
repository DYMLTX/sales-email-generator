#!/bin/bash

echo "Installing missing ODBC libraries..."
echo "You'll need to run this with sudo permissions."
echo ""

# Install unixodbc library
sudo apt-get install -y unixodbc

echo ""
echo "ODBC libraries installation complete!"