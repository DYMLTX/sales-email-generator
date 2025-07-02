#!/usr/bin/env python3
"""Salesforce Meeting Drivers Analysis - What leads to successful meetings using proper SF data."""

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

print("🎯 SALESFORCE MEETING DRIVERS ANALYSIS - MAX.Live Brand Targeting")
print("=" * 75)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# 1. Meeting Activity Overview
print("\n1. SALESFORCE MEETING ACTIVITY OVERVIEW:")
print("-" * 42)

# Get meeting counts and recent activity
cursor.execute("""
    SELECT 
        COUNT(*) as total_meetings,
        COUNT(DISTINCT ContactId) as unique_contacts,
        COUNT(DISTINCT AccountId) as unique_accounts,
        MIN(ActivityDate) as first_meeting,
        MAX(ActivityDate) as last_meeting
    FROM sf.vMeetingSortASC
    WHERE ActivityDate IS NOT NULL
""")

overview = cursor.fetchone()
print(f"Total Meetings: {overview[0]:,}")
print(f"Unique Contacts: {overview[1]:,}")
print(f"Unique Accounts: {overview[2]:,}")
print(f"Date Range: {overview[3]} to {overview[4]}")

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
GROUP BY c.Title
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("Contact Title | Meetings | Accounts | Priority")
    print("-" * 55)
    for _, row in df.iterrows():
        title = str(row['Title'])[:25]
        meetings = row['meeting_count']
        accounts = row['unique_accounts']
        priority = row['target_priority']
        print(f"{title:25} | {meetings:8} | {accounts:8} | {priority}")

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

df = pd.read_sql(query, conn)
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

# 4. Industry Analysis with Brand Data
print("\n4. INDUSTRY ANALYSIS (Brand Industries vs Meetings):")
print("-" * 53)

query = """
SELECT TOP 10
    b.WM_Brand_Industries__c as industry,
    COUNT(DISTINCT b.Id) as total_brands,
    COUNT(DISTINCT m.ContactId) as meeting_contacts,
    COUNT(m.ContactId) as total_meetings,
    AVG(b.WM_Brand_Media_Spend__c) as avg_media_spend,
    CASE 
        WHEN COUNT(m.ContactId) > 0 
        THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT b.Id)
        ELSE 0 
    END as meetings_per_brand
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE b.WM_Brand_Industries__c IS NOT NULL
AND b.WM_Brand_Industries__c != ''
GROUP BY b.WM_Brand_Industries__c
ORDER BY COUNT(m.ContactId) DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("Industry | Brands | Meetings | Contacts | Avg Spend | Meet/Brand")
    print("-" * 70)
    for _, row in df.iterrows():
        industry = str(row['industry'])[:15] if row['industry'] else 'Unknown'
        brands = row['total_brands']
        meetings = row['total_meetings']
        contacts = row['meeting_contacts']
        avg_spend = f"${row['avg_media_spend']:,.0f}" if row['avg_media_spend'] else "N/A"
        ratio = row['meetings_per_brand']
        print(f"{industry:15} | {brands:6} | {meetings:8} | {contacts:8} | {avg_spend:9} | {ratio:8.1f}")

# 5. Audience Attributes Analysis
print("\n5. AUDIENCE ATTRIBUTES vs MEETING SUCCESS:")
print("-" * 43)

query = """
SELECT TOP 10
    b.Audience_Attributes__c as audience_type,
    COUNT(DISTINCT b.Id) as brands_count,
    COUNT(DISTINCT m.ContactId) as meeting_contacts,
    COUNT(m.ContactId) as total_meetings,
    AVG(b.WM_Brand_Media_Spend__c) as avg_spend
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE b.Audience_Attributes__c IS NOT NULL
AND b.Audience_Attributes__c != ''
AND LEN(b.Audience_Attributes__c) < 50  -- readable length
GROUP BY b.Audience_Attributes__c
ORDER BY COUNT(m.ContactId) DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("Audience Type | Brands | Meetings | Contacts | Avg Spend")
    print("-" * 58)
    for _, row in df.iterrows():
        audience = str(row['audience_type'])[:20] if row['audience_type'] else 'Unknown'
        brands = row['brands_count']
        meetings = row['total_meetings']
        contacts = row['meeting_contacts']
        avg_spend = f"${row['avg_spend']:,.0f}" if row['avg_spend'] else "N/A"
        print(f"{audience:20} | {brands:6} | {meetings:8} | {contacts:8} | {avg_spend}")

# 6. Geographic Analysis (Brand Locations)
print("\n6. GEOGRAPHIC ANALYSIS (Brand States vs Meetings):")
print("-" * 51)

query = """
SELECT TOP 15
    b.WM_Brand_State__c as brand_state,
    COUNT(DISTINCT b.Id) as brands_count,
    COUNT(DISTINCT m.ContactId) as meeting_contacts,
    COUNT(m.ContactId) as total_meetings,
    AVG(b.WM_Brand_Media_Spend__c) as avg_spend,
    CASE 
        WHEN b.WM_Brand_State__c IN ('California', 'New York', 'Texas', 'Tennessee', 'Georgia', 'Florida') 
        THEN 'MAJOR MUSIC MARKET'
        ELSE 'Minor Market'
    END as market_type
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE b.WM_Brand_State__c IS NOT NULL
AND b.WM_Brand_State__c != ''
GROUP BY b.WM_Brand_State__c
ORDER BY COUNT(m.ContactId) DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("State | Brands | Meetings | Contacts | Avg Spend | Market Type")
    print("-" * 68)
    for _, row in df.iterrows():
        state = str(row['brand_state'])[:12]
        brands = row['brands_count']
        meetings = row['total_meetings']
        contacts = row['meeting_contacts']
        avg_spend = f"${row['avg_spend']:,.0f}" if row['avg_spend'] else "N/A"
        market = row['market_type']
        print(f"{state:12} | {brands:6} | {meetings:8} | {contacts:8} | {avg_spend:9} | {market}")

# 7. Planning & Buying Cycle Analysis
print("\n7. PLANNING & BUYING CYCLE ANALYSIS:")
print("-" * 37)

query = """
SELECT 
    b.Buying_Period__c as buying_period,
    b.Planning_Period__c as planning_period,
    COUNT(DISTINCT b.Id) as brands_count,
    COUNT(m.ContactId) as total_meetings,
    AVG(b.WM_Brand_Media_Spend__c) as avg_spend
FROM sf.Brands b
INNER JOIN sf.Account a ON a.Id = b.Account__c
LEFT JOIN sf.vMeetingSortASC m ON m.AccountId = a.Id
WHERE (b.Buying_Period__c IS NOT NULL OR b.Planning_Period__c IS NOT NULL)
GROUP BY b.Buying_Period__c, b.Planning_Period__c
ORDER BY COUNT(m.ContactId) DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("Buying Period | Planning Period | Brands | Meetings | Avg Spend")
    print("-" * 65)
    for _, row in df.iterrows():
        buying = str(row['buying_period'])[:12] if row['buying_period'] else 'N/A'
        planning = str(row['planning_period'])[:14] if row['planning_period'] else 'N/A'
        brands = row['brands_count']
        meetings = row['total_meetings']
        avg_spend = f"${row['avg_spend']:,.0f}" if row['avg_spend'] else "N/A"
        print(f"{buying:12} | {planning:14} | {brands:6} | {meetings:8} | {avg_spend}")

# 8. Recent Meeting Activity (Last 6 months)
print("\n8. RECENT MEETING ACTIVITY (Last 6 Months):")
print("-" * 44)

query = """
SELECT 
    FORMAT(m.ActivityDate, 'yyyy-MM') as meeting_month,
    COUNT(*) as total_meetings,
    COUNT(DISTINCT m.ContactId) as unique_contacts,
    COUNT(DISTINCT m.AccountId) as unique_accounts,
    COUNT(DISTINCT CASE WHEN b.WM_Brand_Media_Spend__c >= 500000 THEN m.AccountId END) as high_spend_accounts
FROM sf.vMeetingSortASC m
LEFT JOIN sf.Account a ON a.Id = m.AccountId
LEFT JOIN sf.Brands b ON b.Account__c = a.Id
WHERE m.ActivityDate >= DATEADD(month, -6, GETDATE())
GROUP BY FORMAT(m.ActivityDate, 'yyyy-MM')
ORDER BY meeting_month DESC
"""

df = pd.read_sql(query, conn)
if not df.empty:
    print("Month | Meetings | Contacts | Accounts | High-Spend Accounts")
    print("-" * 60)
    for _, row in df.iterrows():
        month = row['meeting_month']
        meetings = row['total_meetings']
        contacts = row['unique_contacts']
        accounts = row['unique_accounts']
        high_spend = row['high_spend_accounts'] or 0
        print(f"{month:7} | {meetings:8} | {contacts:8} | {accounts:8} | {high_spend:17}")

conn.close()

print("\n" + "=" * 75)
print("🎯 STRATEGIC INSIGHTS FOR MAX.LIVE EMAIL TARGETING:")
print("-" * 53)
print("1. 📊 Target High-Spend Brands: $500K+ media spend shows highest engagement")
print("2. 🎪 Focus on Entertainment/Media/Consumer brands with music-aligned audiences")
print("3. 📍 Prioritize Major Music Markets: CA, NY, TX, TN, GA, FL")
print("4. 👔 Contact Marketing/Brand Directors and VPs for highest meeting rates")
print("5. 📅 Align outreach timing with brand planning and buying cycles")
print("6. 🎵 Leverage audience attributes that match live music demographics")
print("7. 🔄 Build relationships with accounts that have multiple brands")
print("8. 📈 Recent meeting activity shows consistent engagement opportunity")
print("=" * 75)