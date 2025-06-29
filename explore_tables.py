#!/usr/bin/env python3
"""Quick script to explore Azure database tables."""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd

# Load environment variables
load_dotenv()

# Connection parameters
server = os.getenv('AZURE_DB_SERVER')
database = os.getenv('AZURE_DB_DATABASE')
username = os.getenv('AZURE_DB_USERNAME')
password = os.getenv('AZURE_DB_PASSWORD')

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

print("Connecting to database...")
conn = pyodbc.connect(connection_string)

# Find contact and account related tables
query = """
SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
AND (LOWER(TABLE_NAME) LIKE '%contact%' 
     OR LOWER(TABLE_NAME) LIKE '%account%'
     OR LOWER(TABLE_NAME) LIKE '%company%'
     OR LOWER(TABLE_NAME) LIKE '%lead%'
     OR LOWER(TABLE_NAME) LIKE '%prospect%')
ORDER BY TABLE_NAME
"""

print("\nSearching for relevant tables...")
df = pd.read_sql(query, conn)

print(f"\nFound {len(df)} relevant tables:")
for _, row in df.iterrows():
    print(f"  - {row['TABLE_NAME']}")

# Check specific tables we expect
expected_tables = ['contact', 'Contact', 'account', 'Account', 'contacts', 'accounts']
print("\nChecking for expected tables:")
for table in expected_tables:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  ✓ {table}: {count:,} rows")
    except:
        print(f"  ✗ {table}: not found or error")

conn.close()