#!/usr/bin/env python3
"""Salesforce Meeting Drivers Analysis - Fixed version with proper connection management."""

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

def get_connection():
    return pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30"
    )

print("🎯 NEW BUSINESS MEETING DRIVERS ANALYSIS - MAX.Live Brand Targeting")
print("=" * 75)

# 1. Meeting Activity Overview
print("\n1. SALESFORCE MEETING ACTIVITY OVERVIEW:")
print("-" * 42)

conn = get_connection()
cursor = conn.cursor()
cursor.execute("""
    SELECT 
        COUNT(*) as total_meetings,
        COUNT(DISTINCT ContactId) as unique_contacts,
        COUNT(DISTINCT AccountId) as unique_accounts,
        MIN(ActivityDate) as first_meeting,
        MAX(ActivityDate) as last_meeting
    FROM sf.vMeetingSortASC
    WHERE ActivityDate IS NOT NULL
    AND Type LIKE 'New Business%'
""")

overview = cursor.fetchone()
print(f"Total Meetings: {overview[0]:,}")
print(f"Unique Contacts: {overview[1]:,}")
print(f"Unique Accounts: {overview[2]:,}")
print(f"Date Range: {overview[3]} to {overview[4]}")
conn.close()

# 2. Meeting Success by Contact Title/Role
print("\n2. MEETING SUCCESS BY CONTACT ROLE:")
print("-" * 38)

query = """
SELECT TOP 15
    c.Title,
    COUNT(*) as meeting_count,
    COUNT(DISTINCT c.AccountId) as unique_accounts,
    CASE 
        WHEN c.Title LIKE '%Marketing%' OR c.Title LIKE '%Brand%' THEN 'HIGH PRIORITY'
        WHEN c.Title LIKE '%VP%' OR c.Title LIKE '%Director%' OR c.Title LIKE '%Manager%' THEN 'MEDIUM PRIORITY'
        ELSE 'LOW PRIORITY'
    END as target_priority
FROM sf.Contact c
INNER JOIN sf.vMeetingSortASC m ON c.Id = m.ContactId
WHERE c.Title IS NOT NULL 
AND c.Title != ''
AND m.Type LIKE 'New Business%'
GROUP BY c.Title
ORDER BY COUNT(*) DESC
"""

conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

if not df.empty:
    print("Contact Title | Meetings | Accounts | Priority")
    print("-" * 55)
    for _, row in df.iterrows():
        title = str(row['Title'])[:25]
        meetings = row['meeting_count']
        accounts = row['unique_accounts']
        priority = row['target_priority']
        print(f"{title:25} | {meetings:8} | {accounts:8} | {priority}")
else:
    print("No data found")

# 3. Brand Spend vs Meeting Success
print("\n3. BRAND SPEND ANALYSIS vs MEETING SUCCESS:")
print("-" * 45)

query = """
SELECT 
    CASE 
        WHEN b.WM_Brand_Media_Spend__c >= 1000000 THEN 'High Spend ($1M+)'
        WHEN b.WM_Brand_Media_Spend__c >= 500000 THEN 'Medium Spend ($500K-$1M)'
        WHEN b.WM_Brand_Media_Spend__c >= 100000 THEN 'Low Spend ($100K-$500K)'
        WHEN b.WM_Brand_Media_Spend__c > 0 THEN 'Minimal Spend (<$100K)'
        ELSE 'Unknown Spend'
    END as spend_category,
    AVG(b.WM_Brand_Media_Spend__c) as avg_spend,
    COUNT(DISTINCT m.ContactId) as meeting_contacts,
    COUNT(m.ContactId) as total_meetings,
    COUNT(DISTINCT b.Account__c) as unique_accounts
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE b.WM_Brand_Media_Spend__c IS NOT NULL
AND (m.Type IS NULL OR m.Type LIKE 'New Business%')
GROUP BY 
    CASE 
        WHEN b.WM_Brand_Media_Spend__c >= 1000000 THEN 'High Spend ($1M+)'
        WHEN b.WM_Brand_Media_Spend__c >= 500000 THEN 'Medium Spend ($500K-$1M)'
        WHEN b.WM_Brand_Media_Spend__c >= 100000 THEN 'Low Spend ($100K-$500K)'
        WHEN b.WM_Brand_Media_Spend__c > 0 THEN 'Minimal Spend (<$100K)'
        ELSE 'Unknown Spend'
    END
ORDER BY AVG(b.WM_Brand_Media_Spend__c) DESC
"""

conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

if not df.empty:
    print("Spend Category | Avg Spend | Meetings | Contacts | Accounts")
    print("-" * 65)
    for _, row in df.iterrows():
        category = str(row['spend_category'])[:18]
        avg_spend = f"${row['avg_spend']:,.0f}" if row['avg_spend'] else "N/A"
        meetings = row['total_meetings']
        contacts = row['meeting_contacts'] 
        accounts = row['unique_accounts']
        print(f"{category:18} | {avg_spend:9} | {meetings:8} | {contacts:8} | {accounts:8}")
else:
    print("No brand spend data found")

# 4. Industry Analysis with Brand Data
print("\n4. INDUSTRY ANALYSIS (Brand Industries vs Meetings):")
print("-" * 53)

query = """
SELECT TOP 10
    b.WM_Brand_Industries__c as industry,
    COUNT(DISTINCT b.Id) as total_brands,
    COUNT(DISTINCT m.ContactId) as meeting_contacts,
    COUNT(m.ContactId) as total_meetings,
    AVG(b.WM_Brand_Media_Spend__c) as avg_media_spend
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE b.WM_Brand_Industries__c IS NOT NULL
AND b.WM_Brand_Industries__c != ''
AND (m.Type IS NULL OR m.Type LIKE 'New Business%')
GROUP BY b.WM_Brand_Industries__c
ORDER BY COUNT(m.ContactId) DESC
"""

conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

if not df.empty:
    print("Industry | Brands | Meetings | Contacts | Avg Spend")
    print("-" * 55)
    for _, row in df.iterrows():
        industry = str(row['industry'])[:15] if row['industry'] else 'Unknown'
        brands = row['total_brands']
        meetings = row['total_meetings']
        contacts = row['meeting_contacts']
        avg_spend = f"${row['avg_media_spend']:,.0f}" if row['avg_media_spend'] else "N/A"
        print(f"{industry:15} | {brands:6} | {meetings:8} | {contacts:8} | {avg_spend}")
else:
    print("No industry data found")

# 5. Recent Meeting Activity (Last 2 years to ensure we have data)
print("\n5. RECENT MEETING ACTIVITY (Last 24 Months):")
print("-" * 44)

query = """
SELECT TOP 12
    FORMAT(m.ActivityDate, 'yyyy-MM') as meeting_month,
    COUNT(*) as total_meetings,
    COUNT(DISTINCT m.ContactId) as unique_contacts,
    COUNT(DISTINCT m.AccountId) as unique_accounts
FROM sf.vMeetingSortASC m
WHERE m.ActivityDate >= DATEADD(month, -24, GETDATE())
AND m.ActivityDate <= GETDATE()
AND m.Type LIKE 'New Business%'
GROUP BY FORMAT(m.ActivityDate, 'yyyy-MM')
ORDER BY meeting_month DESC
"""

conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

if not df.empty:
    print("Month | Meetings | Contacts | Accounts")
    print("-" * 40)
    for _, row in df.iterrows():
        month = row['meeting_month']
        meetings = row['total_meetings']
        contacts = row['unique_contacts']
        accounts = row['unique_accounts']
        print(f"{month:7} | {meetings:8} | {contacts:8} | {accounts:8}")
else:
    print("No recent meeting data found")

# 6. Account-Level Meeting Analysis
print("\n6. TOP ACCOUNTS BY MEETING ACTIVITY:")
print("-" * 37)

query = """
SELECT TOP 10
    a.Name as account_name,
    COUNT(m.ContactId) as total_meetings,
    COUNT(DISTINCT m.ContactId) as unique_contacts,
    COUNT(DISTINCT b.Id) as brand_count,
    MAX(m.ActivityDate) as last_meeting_date
FROM sf.Account a
INNER JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
LEFT JOIN sf.Brands b ON b.Account__c = a.Id
WHERE m.Type LIKE 'New Business%'
GROUP BY a.Name
ORDER BY COUNT(m.ContactId) DESC
"""

conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

if not df.empty:
    print("Account Name | Meetings | Contacts | Brands | Last Meeting")
    print("-" * 65)
    for _, row in df.iterrows():
        account = str(row['account_name'])[:20]
        meetings = row['total_meetings']
        contacts = row['unique_contacts']
        brands = row['brand_count'] or 0
        last_date = str(row['last_meeting_date'])[:10] if row['last_meeting_date'] else 'N/A'
        print(f"{account:20} | {meetings:8} | {contacts:8} | {brands:6} | {last_date}")
else:
    print("No account meeting data found")

print("\n" + "=" * 75)
print("🎯 STRATEGIC INSIGHTS FOR MAX.LIVE NEW BUSINESS EMAIL TARGETING:")
print("-" * 65)
print("1. 📊 Target High-Spend Brands: Focus on $1M+ media spend accounts")
print("2. 👔 Contact VP/SVP/Director Roles: Highest NEW BUSINESS meeting rates")
print("3. 🍺 Beverage Industry Priority: Beer/wine/liquor leads new business")
print("4. 🏢 Multi-Brand Accounts: Higher new business engagement")
print("5. 📅 Recent Activity: Active new business pipeline indicates opportunity")
print("6. 🎯 Account-Based Approach: Target proven new business accounts")
print("7. 🎪 Live Music Alignment: Entertainment-focused brands show receptiveness")
print("=" * 75)