#!/usr/bin/env python3
"""Analyze prospect data structure for email campaigns - fixed version."""

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

# 1. Sample contact data with correct column names
print("\n1. SAMPLE CONTACT DATA:")
print("-" * 50)
query = """
SELECT TOP 10
    email,
    firstname,
    lastname,
    jobtitle,
    company,
    lifecyclestage,
    phone,
    city,
    state
FROM contact
WHERE email IS NOT NULL 
AND email != ''
AND firstname IS NOT NULL
AND lastname IS NOT NULL
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# 2. Contact data quality
print("\n2. CONTACT DATA QUALITY:")
print("-" * 50)
query = """
SELECT 
    COUNT(*) as total_contacts,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
    COUNT(CASE WHEN firstname IS NOT NULL AND firstname != '' THEN 1 END) as with_firstname,
    COUNT(CASE WHEN lastname IS NOT NULL AND lastname != '' THEN 1 END) as with_lastname,
    COUNT(CASE WHEN jobtitle IS NOT NULL AND jobtitle != '' THEN 1 END) as with_jobtitle,
    COUNT(CASE WHEN company IS NOT NULL AND company != '' THEN 1 END) as with_company,
    COUNT(CASE WHEN industry IS NOT NULL AND industry != '' THEN 1 END) as with_industry,
    COUNT(CASE WHEN lifecyclestage IS NOT NULL AND lifecyclestage != '' THEN 1 END) as with_lifecycle
FROM contact
"""

cursor = conn.cursor()
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
print(f"With industry: {result[7]:,} ({result[7]/total*100:.1f}%)")
print(f"With lifecycle stage: {result[8]:,} ({result[8]/total*100:.1f}%)")

# 3. Lifecycle stage breakdown
print("\n3. LIFECYCLE STAGE BREAKDOWN:")
print("-" * 50)
query = """
SELECT 
    COALESCE(lifecyclestage, 'Unknown') as stage,
    COUNT(*) as count
FROM contact
GROUP BY lifecyclestage
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# 4. Industry breakdown for MAX.Live targeting
print("\n4. TARGET INDUSTRIES FOR MUSIC SPONSORSHIPS:")
print("-" * 50)
query = """
SELECT TOP 20
    COALESCE(industry, 'Unknown') as industry,
    COUNT(*) as contact_count
FROM contact
WHERE email IS NOT NULL 
AND email != ''
AND industry IS NOT NULL
AND industry != ''
GROUP BY industry
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# 5. Company breakdown (potential sponsors)
print("\n5. TOP COMPANIES BY CONTACT COUNT:")
print("-" * 50)
query = """
SELECT TOP 30
    company,
    COUNT(*) as contact_count,
    COUNT(DISTINCT email) as unique_emails
FROM contact
WHERE company IS NOT NULL 
AND company != ''
AND email IS NOT NULL
AND email != ''
GROUP BY company
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

# 6. Marketing-qualified contacts
print("\n6. MARKETING-QUALIFIED CONTACTS:")
print("-" * 50)
query = """
SELECT 
    COUNT(*) as qualified_contacts
FROM contact
WHERE email IS NOT NULL 
AND email != ''
AND firstname IS NOT NULL 
AND firstname != ''
AND lastname IS NOT NULL 
AND lastname != ''
AND company IS NOT NULL 
AND company != ''
AND (jobtitle LIKE '%marketing%'
     OR jobtitle LIKE '%brand%'
     OR jobtitle LIKE '%sponsor%'
     OR jobtitle LIKE '%partner%'
     OR jobtitle LIKE '%director%'
     OR jobtitle LIKE '%manager%'
     OR jobtitle LIKE '%vp%'
     OR jobtitle LIKE '%vice president%'
     OR jobtitle LIKE '%chief%'
     OR jobtitle LIKE '%head%')
"""

cursor.execute(query)
qualified = cursor.fetchone()[0]
print(f"Contacts with marketing/sponsorship-related titles: {qualified:,}")

# 7. Geographic distribution
print("\n7. GEOGRAPHIC DISTRIBUTION (TOP STATES):")
print("-" * 50)
query = """
SELECT TOP 15
    COALESCE(state, 'Unknown') as state,
    COUNT(*) as contact_count
FROM contact
WHERE email IS NOT NULL 
AND email != ''
GROUP BY state
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))

conn.close()
print("\nAnalysis complete!")