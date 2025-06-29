#!/usr/bin/env python3
"""Quick script to test Azure SQL Database connection."""

import os
from dotenv import load_dotenv
import pyodbc
import sys

# Load environment variables
load_dotenv()

def test_connection():
    """Test the Azure SQL connection with provided credentials."""
    
    # Connection parameters
    server = os.getenv('AZURE_DB_SERVER', 'max-sql-server.database.windows.net')
    database = os.getenv('AZURE_DB_DATABASE', '')
    username = os.getenv('AZURE_DB_USERNAME', '')
    password = os.getenv('AZURE_DB_PASSWORD', '')
    use_azure_ad = os.getenv('AZURE_DB_USE_AZURE_AD', 'false').lower() == 'true'
    
    print(f"Testing connection to: {server}")
    print(f"Database: {database}")
    print(f"Authentication: {'Azure AD' if use_azure_ad else 'SQL Authentication'}")
    
    if not database:
        print("❌ ERROR: Database name not provided!")
        print("Please set AZURE_DB_DATABASE in your .env file")
        return False
    
    try:
        if use_azure_ad:
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
                f"Authentication=ActiveDirectoryDefault"
            )
        else:
            if not username or not password:
                print("❌ ERROR: Username or password not provided!")
                print("Please set AZURE_DB_USERNAME and AZURE_DB_PASSWORD in your .env file")
                return False
                
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"Uid={username};"
                f"Pwd={password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30"
            )
        
        print("\n🔄 Attempting to connect...")
        conn = pyodbc.connect(connection_string)
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT @@version")
        version = cursor.fetchone()[0]
        
        print("✅ Connection successful!")
        print(f"\nServer version: {version.split('\\n')[0]}")
        
        # List available tables
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        print(f"\nFound {len(tables)} tables in the database:")
        for table in tables[:10]:  # Show first 10 tables
            print(f"  - {table[0]}")
        
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more")
        
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print(f"\n❌ Connection failed!")
        print(f"Error: {e}")
        
        # Common error diagnostics
        if "Login failed" in str(e):
            print("\n💡 Tip: Check your username and password")
        elif "Cannot open database" in str(e):
            print("\n💡 Tip: Verify the database name is correct")
        elif "server was not found" in str(e):
            print("\n💡 Tip: Check the server address and network connectivity")
        elif "ODBC Driver" in str(e):
            print("\n💡 Tip: You may need to install the SQL Server ODBC driver:")
            print("    Ubuntu/Debian: sudo apt-get install unixodbc-dev")
            print("    Mac: brew install unixodbc")
            
        return False

if __name__ == "__main__":
    # First, let's check if we have the required ODBC driver
    try:
        drivers = [x for x in pyodbc.drivers() if 'SQL Server' in x]
        if drivers:
            print(f"Available SQL Server drivers: {drivers}")
        else:
            print("⚠️  No SQL Server ODBC drivers found!")
            print("\nTo install on Ubuntu/WSL:")
            print("curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -")
            print("curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list")
            print("sudo apt-get update")
            print("sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18")
            sys.exit(1)
    except Exception as e:
        print(f"Error checking drivers: {e}")
    
    print("-" * 50)
    success = test_connection()
    sys.exit(0 if success else 1)