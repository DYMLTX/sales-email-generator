#!/usr/bin/env python3
"""Analyze prospect data structure for email campaigns."""

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

# 1. Analyze Contact table structure
print("\n1. CONTACT TABLE STRUCTURE:")
print("-" * 50)
query = """
SELECT TOP 1 COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'contact'
ORDER BY ORDINAL_POSITION
"""

# Get all columns
query = """
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'contact'
ORDER BY ORDINAL_POSITION
"""

cursor = conn.cursor()
cursor.execute(query)
columns = cursor.fetchall()

print(f"Total columns: {len(columns)}")
print("\nKey columns:")
key_columns = ['id', 'email', 'firstname', 'lastname', 'company', 'jobtitle', 
               'phone', 'industry', 'city', 'state', 'country', 
               'createdate', 'lastmodifieddate', 'lifecyclestage']

for col in columns:
    if any(key in col[0].lower() for key in key_columns):
        print(f"  - {col[0]} ({col[1]})")

# 2. Sample contact data
print("\n2. SAMPLE CONTACT DATA:")
print("-" * 50)
query = """
SELECT TOP 5
    id,
    email,
    firstname,
    lastname,
    jobtitle,
    company,
    lifecyclestage
FROM contact
WHERE email IS NOT NULL 
AND email != ''
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# 3. Contact data quality
print("\n3. CONTACT DATA QUALITY:")
print("-" * 50)
query = """
SELECT 
    COUNT(*) as total_contacts,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
    COUNT(CASE WHEN firstname IS NOT NULL AND firstname != '' THEN 1 END) as with_firstname,
    COUNT(CASE WHEN lastname IS NOT NULL AND lastname != '' THEN 1 END) as with_lastname,
    COUNT(CASE WHEN jobtitle IS NOT NULL AND jobtitle != '' THEN 1 END) as with_jobtitle,
    COUNT(CASE WHEN company IS NOT NULL AND company != '' THEN 1 END) as with_company
FROM contact
"""

cursor.execute(query)
result = cursor.fetchone()
total = result[0]

print(f"Total contacts: {total:,}")
print(f"Unique emails: {result[1]:,}")
print(f"With email: {result[2]:,} ({result[2]/total*100:.1f}%)")
print(f"With first name: {result[3]:,} ({result[3]/total*100:.1f}%)")
print(f"With last name: {result[4]:,} ({result[4]/total*100:.1f}%)")
print(f"With job title: {result[5]:,} ({result[5]/total*100:.1f}%)")
print(f"With company: {result[6]:,} ({result[6]/total*100:.1f}%)")

# 4. Look for company/account tables
print("\n4. COMPANY/ACCOUNT TABLES:")
print("-" * 50)

# Check company table
try:
    query = "SELECT COUNT(*) FROM company"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    print(f"Company table: {count:,} rows")
    
    # Sample company data
    query = """
    SELECT TOP 5
        COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'company'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query)
    cols = [row[0] for row in cursor.fetchall()]
    print(f"Company columns preview: {', '.join(cols)}...")
    
except:
    print("Company table not accessible")

# Check Account table
try:
    query = "SELECT COUNT(*) FROM Account"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    print(f"\nAccount table: {count:,} rows")
except:
    print("\nAccount table not accessible")

# 5. Check for relationships
print("\n5. CONTACT-COMPANY RELATIONSHIPS:")
print("-" * 50)

# Look for association tables
association_tables = ['contact_company', 'ContactCompanyAssociations', 
                     'company_contact', 'CompanyContactAssociations']

for table in association_tables:
    try:
        query = f"SELECT COUNT(*) FROM {table}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"{table}: {count:,} associations")
    except:
        continue

# 6. Industry breakdown for targeting
print("\n6. POTENTIAL TARGET INDUSTRIES:")
print("-" * 50)
query = """
SELECT TOP 20
    COALESCE(company, 'Unknown') as company_name,
    COUNT(*) as contact_count
FROM contact
WHERE company IS NOT NULL 
AND company != ''
GROUP BY company
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

conn.close()
print("\nAnalysis complete!")