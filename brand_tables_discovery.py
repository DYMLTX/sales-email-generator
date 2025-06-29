#!/usr/bin/env python3
"""Discover Brand tables with demographics and audience data for MAX.Live targeting."""

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

print("🏢 BRAND TABLES DISCOVERY - Demographics & Audience Data")
print("=" * 60)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# 1. Find all Brand tables
print("\n1. BRAND TABLES INVENTORY:")
print("-" * 35)

query = """
SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
AND (LOWER(TABLE_NAME) LIKE '%brand%')
ORDER BY TABLE_NAME
"""

df = pd.read_sql(query, conn)
brand_tables = df['TABLE_NAME'].tolist()

print(f"Found {len(brand_tables)} Brand-related tables:")
for i, table in enumerate(brand_tables, 1):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{i:2}. {table:35} | {count:,} records")
    except:
        print(f"{i:2}. {table:35} | Error accessing")

# 2. Analyze key Brand tables structure
print("\n2. BRAND TABLE STRUCTURES:")
print("-" * 30)

key_brand_tables = ['BrandsCompanyAssociations']  # Start with the obvious one
for table in brand_tables:
    if any(word in table.lower() for word in ['association', 'demographic', 'audience', 'preference']):
        if table not in key_brand_tables:
            key_brand_tables.append(table)

for table_name in key_brand_tables[:5]:  # Analyze top 5 most relevant
    try:
        print(f"\n📊 {table_name.upper()}:")
        print("-" * (len(table_name) + 5))
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Records: {count:,}")
        
        # Get column structure
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        print(f"Columns: {len(columns)}")
        
        # Look for key demographic/audience fields
        key_fields = ['age', 'gender', 'income', 'demographic', 'audience', 'target', 
                     'interest', 'preference', 'music', 'genre', 'artist', 'brand',
                     'account', 'company', 'id']
        
        print("\nKey fields found:")
        relevant_cols = []
        for col_name, col_type in columns:
            if any(key in col_name.lower() for key in key_fields):
                relevant_cols.append((col_name, col_type))
                print(f"  - {col_name} ({col_type})")
        
        # Sample data if possible
        if relevant_cols and count > 0:
            print(f"\nSample data from {table_name}:")
            sample_cols = [col[0] for col in relevant_cols[:5]]  # First 5 relevant columns
            col_list = ', '.join(f"[{col}]" for col in sample_cols)
            
            try:
                cursor.execute(f"SELECT TOP 3 {col_list} FROM {table_name}")
                samples = cursor.fetchall()
                
                # Print header
                header = " | ".join(f"{col:15}" for col in sample_cols)
                print(f"  {header}")
                print(f"  {'-' * len(header)}")
                
                # Print samples
                for row in samples:
                    row_str = " | ".join(f"{str(val)[:15]:15}" for val in row)
                    print(f"  {row_str}")
                    
            except Exception as e:
                print(f"  Error sampling data: {e}")
                
    except Exception as e:
        print(f"Error analyzing {table_name}: {e}")

# 3. Brand-Company Associations Analysis
print("\n3. BRAND-COMPANY ASSOCIATIONS ANALYSIS:")
print("-" * 40)

try:
    # Analyze BrandsCompanyAssociations table
    query = """
    SELECT TOP 10
        BrandId,
        CompanyId,
        CAST(associatedcompanyid AS varchar) as account_id
    FROM BrandsCompanyAssociations
    WHERE BrandId IS NOT NULL 
    AND CompanyId IS NOT NULL
    """
    
    df = pd.read_sql(query, conn)
    print("Brand-Company mapping sample:")
    print(df.to_string(index=False))
    
    # Count associations
    cursor.execute("SELECT COUNT(*) FROM BrandsCompanyAssociations")
    total_associations = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT BrandId) FROM BrandsCompanyAssociations")
    unique_brands = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT CompanyId) FROM BrandsCompanyAssociations")
    unique_companies = cursor.fetchone()[0]
    
    print(f"\nAssociation Statistics:")
    print(f"  Total associations: {total_associations:,}")
    print(f"  Unique brands: {unique_brands:,}")
    print(f"  Unique companies: {unique_companies:,}")
    
except Exception as e:
    print(f"Error analyzing brand associations: {e}")

# 4. Look for demographic/audience preference tables
print("\n4. BRAND AUDIENCE & PREFERENCE TABLES:")
print("-" * 40)

audience_tables = [t for t in brand_tables if any(word in t.lower() 
                  for word in ['preference', 'audience', 'demographic', 'target'])]

if audience_tables:
    for table in audience_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"📈 {table}: {count:,} records")
            
            # Get a few sample columns to understand structure
            cursor.execute(f"""
                SELECT TOP 5 COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION
            """)
            sample_cols = [row[0] for row in cursor.fetchall()]
            print(f"    Key columns: {', '.join(sample_cols)}")
            
        except Exception as e:
            print(f"    Error: {e}")
else:
    print("No specific audience/preference tables found")

# 5. Artist-Brand relationship tables
print("\n5. ARTIST-BRAND RELATIONSHIP TABLES:")
print("-" * 35)

artist_brand_tables = [t for t in brand_tables if 'artist' in t.lower()]
if not artist_brand_tables:
    # Look in all tables for artist-brand relationships
    query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    AND (LOWER(TABLE_NAME) LIKE '%artist%brand%' 
         OR LOWER(TABLE_NAME) LIKE '%brand%artist%')
    ORDER BY TABLE_NAME
    """
    df = pd.read_sql(query, conn)
    artist_brand_tables = df['TABLE_NAME'].tolist()

if artist_brand_tables:
    for table in artist_brand_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"🎵 {table}: {count:,} records")
        except:
            print(f"🎵 {table}: Error accessing")
else:
    print("No direct artist-brand relationship tables found")

# 6. Sample brand targeting query
print("\n6. SAMPLE BRAND TARGETING OPPORTUNITY:")
print("-" * 38)

try:
    # Try to find brands associated with companies we have contacts for
    query = """
    SELECT TOP 10
        b.BrandId,
        b.CompanyId,
        c.company as company_name,
        COUNT(DISTINCT c.email) as contact_count
    FROM BrandsCompanyAssociations b
    INNER JOIN contact c ON CAST(c.associatedcompanyid AS varchar) = CAST(b.CompanyId AS varchar)
    WHERE c.email IS NOT NULL
    AND c.company IS NOT NULL
    GROUP BY b.BrandId, b.CompanyId, c.company
    HAVING COUNT(DISTINCT c.email) >= 5  -- Companies with multiple contacts
    ORDER BY COUNT(DISTINCT c.email) DESC
    """
    
    df = pd.read_sql(query, conn)
    if not df.empty:
        print("Brands with contactable companies:")
        print("BrandID | CompanyID | Company Name | Contacts")
        print("-" * 50)
        for _, row in df.iterrows():
            print(f"{row['BrandId']:7} | {row['CompanyId']:9} | {row['company_name'][:25]:25} | {row['contact_count']:8}")
    else:
        print("No direct brand-contact matches found in sample")
        
except Exception as e:
    print(f"Error in brand targeting query: {e}")

conn.close()

print("\n" + "=" * 60)
print("🎯 NEXT STEPS FOR BRAND-LEVEL TARGETING:")
print("-" * 40)
print("1. Map brand demographic data to contact companies")
print("2. Analyze brand audience preferences vs artist fan bases")  
print("3. Create brand affinity scoring for artist matching")
print("4. Build demographic alignment algorithms")
print("5. Identify brands with music-friendly target audiences")
print("6. Create brand-specific value propositions")
print("=" * 60)