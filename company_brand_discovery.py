#!/usr/bin/env python3
"""Company and Brand Level Data Discovery for MAX.Live Sponsorship Targeting."""

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

print("🔍 COMPANY & BRAND DATA DISCOVERY FOR MAX.LIVE")
print("=" * 60)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# 1. Company Table Analysis
print("\n1. COMPANY TABLE ANALYSIS:")
print("-" * 40)

try:
    # Check if company table exists and get structure
    cursor.execute("SELECT COUNT(*) FROM company")
    company_count = cursor.fetchone()[0]
    print(f"✅ Company table found: {company_count:,} records")
    
    # Get company table structure
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'company'
        ORDER BY ORDINAL_POSITION
    """)
    
    company_columns = cursor.fetchall()
    print(f"\nCompany table has {len(company_columns)} columns")
    
    # Look for key business fields
    key_fields = ['name', 'industry', 'revenue', 'employees', 'website', 'description', 
                  'size', 'type', 'annual', 'domain', 'founded', 'headquarters']
    
    print("\nKey company fields found:")
    for col_name, col_type in company_columns:
        if any(key in col_name.lower() for key in key_fields):
            print(f"  - {col_name} ({col_type})")

except Exception as e:
    print(f"❌ Company table not accessible: {e}")

# 2. Company-Contact Associations
print("\n2. COMPANY-CONTACT RELATIONSHIPS:")
print("-" * 40)

# Check association tables
association_queries = [
    ("ContactCompanyAssociations", "SELECT COUNT(*) FROM ContactCompanyAssociations"),
    ("contact_company", "SELECT COUNT(*) FROM contact_company"),
    ("CompanyContactAssociations", "SELECT COUNT(*) FROM CompanyContactAssociations")
]

for table_name, query in association_queries:
    try:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"✅ {table_name}: {count:,} associations")
    except:
        continue

# 3. Large Companies by Contact Count (Potential Major Sponsors)
print("\n3. TOP COMPANIES BY CONTACT COUNT (POTENTIAL MAJOR SPONSORS):")
print("-" * 60)

query = """
SELECT TOP 25
    company,
    COUNT(*) as contact_count,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(CASE WHEN jobtitle LIKE '%marketing%' OR jobtitle LIKE '%brand%' 
                OR jobtitle LIKE '%sponsor%' OR jobtitle LIKE '%partner%'
                OR jobtitle LIKE '%director%' OR jobtitle LIKE '%manager%'
                OR jobtitle LIKE '%vp%' OR jobtitle LIKE '%vice president%'
                OR jobtitle LIKE '%chief%' THEN 1 END) as marketing_contacts
FROM contact
WHERE company IS NOT NULL 
AND company != ''
AND company != 'your company'
AND email IS NOT NULL
AND email != ''
GROUP BY company
HAVING COUNT(*) >= 3  -- Companies with multiple contacts
ORDER BY COUNT(*) DESC
"""

try:
    df = pd.read_sql(query, conn)
    print("Company Name | Contacts | Emails | Marketing Roles")
    print("-" * 60)
    for _, row in df.iterrows():
        print(f"{row['company'][:35]:35} | {row['contact_count']:8} | {row['unique_emails']:6} | {row['marketing_contacts']:13}")
except Exception as e:
    print(f"Error: {e}")

# 4. Industry Analysis for Sponsorship Targeting
print("\n4. INDUSTRY BREAKDOWN FOR SPONSORSHIP TARGETING:")
print("-" * 50)

query = """
SELECT 
    CASE 
        WHEN industry LIKE '%Entertainment%' OR industry LIKE '%Media%' OR industry LIKE '%Music%' THEN 'Entertainment/Media'
        WHEN industry LIKE '%Consumer%' OR industry LIKE '%Retail%' OR industry LIKE '%CPG%' THEN 'Consumer/Retail'
        WHEN industry LIKE '%Technology%' OR industry LIKE '%Software%' OR industry LIKE '%Tech%' THEN 'Technology'
        WHEN industry LIKE '%Automotive%' OR industry LIKE '%Auto%' THEN 'Automotive'
        WHEN industry LIKE '%Food%' OR industry LIKE '%Beverage%' OR industry LIKE '%Restaurant%' THEN 'Food & Beverage'
        WHEN industry LIKE '%Financial%' OR industry LIKE '%Banking%' OR industry LIKE '%Insurance%' THEN 'Financial Services'
        WHEN industry LIKE '%Healthcare%' OR industry LIKE '%Medical%' OR industry LIKE '%Pharma%' THEN 'Healthcare'
        WHEN industry LIKE '%Sports%' OR industry LIKE '%Fitness%' OR industry LIKE '%Athletic%' THEN 'Sports/Fitness'
        WHEN industry LIKE '%Travel%' OR industry LIKE '%Hospitality%' OR industry LIKE '%Hotel%' THEN 'Travel/Hospitality'
        WHEN industry LIKE '%Fashion%' OR industry LIKE '%Apparel%' OR industry LIKE '%Clothing%' THEN 'Fashion/Apparel'
        ELSE COALESCE(industry, 'Unknown')
    END as industry_category,
    COUNT(*) as contact_count
FROM contact
WHERE email IS NOT NULL AND email != ''
GROUP BY 
    CASE 
        WHEN industry LIKE '%Entertainment%' OR industry LIKE '%Media%' OR industry LIKE '%Music%' THEN 'Entertainment/Media'
        WHEN industry LIKE '%Consumer%' OR industry LIKE '%Retail%' OR industry LIKE '%CPG%' THEN 'Consumer/Retail'
        WHEN industry LIKE '%Technology%' OR industry LIKE '%Software%' OR industry LIKE '%Tech%' THEN 'Technology'
        WHEN industry LIKE '%Automotive%' OR industry LIKE '%Auto%' THEN 'Automotive'
        WHEN industry LIKE '%Food%' OR industry LIKE '%Beverage%' OR industry LIKE '%Restaurant%' THEN 'Food & Beverage'
        WHEN industry LIKE '%Financial%' OR industry LIKE '%Banking%' OR industry LIKE '%Insurance%' THEN 'Financial Services'
        WHEN industry LIKE '%Healthcare%' OR industry LIKE '%Medical%' OR industry LIKE '%Pharma%' THEN 'Healthcare'
        WHEN industry LIKE '%Sports%' OR industry LIKE '%Fitness%' OR industry LIKE '%Athletic%' THEN 'Sports/Fitness'
        WHEN industry LIKE '%Travel%' OR industry LIKE '%Hospitality%' OR industry LIKE '%Hotel%' THEN 'Travel/Hospitality'
        WHEN industry LIKE '%Fashion%' OR industry LIKE '%Apparel%' OR industry LIKE '%Clothing%' THEN 'Fashion/Apparel'
        ELSE COALESCE(industry, 'Unknown')
    END
ORDER BY COUNT(*) DESC
"""

try:
    df = pd.read_sql(query, conn)
    print("Industry Category | Contact Count | Sponsorship Potential")
    print("-" * 55)
    sponsorship_potential = {
        'Entertainment/Media': 'HIGH - Natural fit for music',
        'Consumer/Retail': 'HIGH - Targets music fans',
        'Technology': 'MEDIUM - Young demographics',
        'Food & Beverage': 'HIGH - Concert/festival sponsors',
        'Automotive': 'HIGH - Major music sponsors',
        'Sports/Fitness': 'MEDIUM - Lifestyle alignment',
        'Fashion/Apparel': 'HIGH - Youth brand affinity',
        'Travel/Hospitality': 'MEDIUM - Experience marketing',
        'Financial Services': 'LOW - Compliance heavy',
        'Healthcare': 'LOW - Regulatory constraints'
    }
    
    for _, row in df.iterrows():
        category = row['industry_category']
        count = row['contact_count']
        potential = sponsorship_potential.get(category, 'UNKNOWN')
        print(f"{category:18} | {count:12,} | {potential}")
        
except Exception as e:
    print(f"Error: {e}")

# 5. Decision Maker Analysis
print("\n5. DECISION MAKER ANALYSIS:")
print("-" * 35)

query = """
SELECT 
    CASE 
        WHEN jobtitle LIKE '%CMO%' OR jobtitle LIKE '%Chief Marketing%' THEN 'Chief Marketing Officer'
        WHEN jobtitle LIKE '%VP Marketing%' OR jobtitle LIKE '%Vice President%Marketing%' THEN 'VP Marketing'
        WHEN jobtitle LIKE '%Director%Marketing%' OR jobtitle LIKE '%Marketing Director%' THEN 'Marketing Director'
        WHEN jobtitle LIKE '%Brand Manager%' OR jobtitle LIKE '%Brand Director%' THEN 'Brand Management'
        WHEN jobtitle LIKE '%Sponsorship%' OR jobtitle LIKE '%Partnership%' THEN 'Sponsorship/Partnerships'
        WHEN jobtitle LIKE '%Event%' OR jobtitle LIKE '%Experiential%' THEN 'Event/Experiential'
        WHEN jobtitle LIKE '%Digital Marketing%' OR jobtitle LIKE '%Performance Marketing%' THEN 'Digital Marketing'
        WHEN jobtitle LIKE '%CEO%' OR jobtitle LIKE '%President%' OR jobtitle LIKE '%Founder%' THEN 'C-Suite/Executive'
        ELSE 'Other Marketing'
    END as role_category,
    COUNT(*) as count
FROM contact
WHERE (jobtitle LIKE '%marketing%' OR jobtitle LIKE '%brand%' OR jobtitle LIKE '%sponsor%' 
       OR jobtitle LIKE '%partner%' OR jobtitle LIKE '%director%' OR jobtitle LIKE '%manager%'
       OR jobtitle LIKE '%vp%' OR jobtitle LIKE '%vice president%' OR jobtitle LIKE '%chief%'
       OR jobtitle LIKE '%cmo%' OR jobtitle LIKE '%ceo%' OR jobtitle LIKE '%president%'
       OR jobtitle LIKE '%event%' OR jobtitle LIKE '%experiential%')
AND email IS NOT NULL AND email != ''
GROUP BY 
    CASE 
        WHEN jobtitle LIKE '%CMO%' OR jobtitle LIKE '%Chief Marketing%' THEN 'Chief Marketing Officer'
        WHEN jobtitle LIKE '%VP Marketing%' OR jobtitle LIKE '%Vice President%Marketing%' THEN 'VP Marketing'
        WHEN jobtitle LIKE '%Director%Marketing%' OR jobtitle LIKE '%Marketing Director%' THEN 'Marketing Director'
        WHEN jobtitle LIKE '%Brand Manager%' OR jobtitle LIKE '%Brand Director%' THEN 'Brand Management'
        WHEN jobtitle LIKE '%Sponsorship%' OR jobtitle LIKE '%Partnership%' THEN 'Sponsorship/Partnerships'
        WHEN jobtitle LIKE '%Event%' OR jobtitle LIKE '%Experiential%' THEN 'Event/Experiential'
        WHEN jobtitle LIKE '%Digital Marketing%' OR jobtitle LIKE '%Performance Marketing%' THEN 'Digital Marketing'
        WHEN jobtitle LIKE '%CEO%' OR jobtitle LIKE '%President%' OR jobtitle LIKE '%Founder%' THEN 'C-Suite/Executive'
        ELSE 'Other Marketing'
    END
ORDER BY COUNT(*) DESC
"""

try:
    df = pd.read_sql(query, conn)
    print("Decision Maker Role | Count | Priority for MAX.Live")
    print("-" * 50)
    priorities = {
        'Chief Marketing Officer': 'HIGHEST',
        'VP Marketing': 'HIGHEST', 
        'Brand Management': 'HIGH',
        'Sponsorship/Partnerships': 'HIGHEST',
        'Event/Experiential': 'HIGH',
        'Marketing Director': 'HIGH',
        'C-Suite/Executive': 'MEDIUM',
        'Digital Marketing': 'LOW'
    }
    
    for _, row in df.iterrows():
        role = row['role_category']
        count = row['count']
        priority = priorities.get(role, 'MEDIUM')
        print(f"{role:18} | {count:5} | {priority}")
        
except Exception as e:
    print(f"Error: {e}")

# 6. Geographic Distribution for Tour Targeting
print("\n6. GEOGRAPHIC DISTRIBUTION (KEY MUSIC MARKETS):")
print("-" * 50)

query = """
SELECT TOP 20
    COALESCE(state, 'Unknown') as state,
    COUNT(*) as contact_count,
    COUNT(CASE WHEN jobtitle LIKE '%marketing%' OR jobtitle LIKE '%brand%' 
                OR jobtitle LIKE '%sponsor%' THEN 1 END) as marketing_contacts
FROM contact
WHERE email IS NOT NULL AND email != ''
GROUP BY state
ORDER BY COUNT(*) DESC
"""

try:
    df = pd.read_sql(query, conn)
    major_music_markets = ['California', 'New York', 'Texas', 'Florida', 'Illinois', 
                          'Tennessee', 'Georgia', 'North Carolina', 'Colorado', 'Washington']
    
    print("State | Total Contacts | Marketing Contacts | Music Market")
    print("-" * 60)
    for _, row in df.iterrows():
        state = row['state']
        total = row['contact_count']
        marketing = row['marketing_contacts']
        is_major = 'MAJOR' if state in major_music_markets else 'Minor'
        print(f"{state:12} | {total:13,} | {marketing:16} | {is_major}")
        
except Exception as e:
    print(f"Error: {e}")

conn.close()

print("\n" + "=" * 60)
print("🎯 RECOMMENDATIONS FOR DATA ENRICHMENT:")
print("-" * 40)
print("1. Company Revenue/Size Data - Critical for budget assessment")
print("2. Recent Marketing Campaign Data - Shows activity level")  
print("3. Previous Music Sponsorship History - Indicates interest")
print("4. Brand Social Media Presence - Engagement potential")
print("5. Company Demographics/Target Audience - Fan alignment")
print("6. Competitive Analysis - Who sponsors similar events")
print("7. Budget/Spending Data - Marketing investment capacity")
print("8. Decision-Making Process - Who influences sponsorship decisions")
print("=" * 60)