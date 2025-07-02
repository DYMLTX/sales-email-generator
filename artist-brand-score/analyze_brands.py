#!/usr/bin/env python3
"""
Analyze sf.Brands table structure for artist-brand matching
Focus on brands with media spend > $5M
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd

# Load environment variables
load_dotenv()

def get_connection():
    server = os.getenv('AZURE_DB_SERVER')
    database = os.getenv('AZURE_DB_DATABASE')
    username = os.getenv('AZURE_DB_USERNAME')
    password = os.getenv('AZURE_DB_PASSWORD')
    
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

def analyze_brands_structure():
    """
    Analyze the sf.Brands table structure and audience data
    """
    print("🎨 ANALYZING SF.BRANDS TABLE FOR ARTIST MATCHING")
    print("=" * 52)
    
    conn = get_connection()
    
    # Check data types and structure
    print("\n1. COLUMN STRUCTURE:")
    print("-" * 20)
    
    column_query = """
    SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
        CHARACTER_MAXIMUM_LENGTH,
        IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'sf' 
    AND TABLE_NAME = 'Brands'
    AND COLUMN_NAME IN (
        'Audience_Attributes__c',
        'Audience_Description__c',
        'WM_Brand_Description__c', 
        'WM_Brand_Website__c',
        'WM_Brand_Industries__c',
        'WM_Brand_Media_Spend__c'
    )
    ORDER BY COLUMN_NAME
    """
    
    columns_df = pd.read_sql(column_query, conn)
    print(columns_df.to_string(index=False))
    
    # Count brands with media spend > $5M
    print("\n2. HIGH-SPEND BRANDS COUNT:")
    print("-" * 27)
    
    count_query = """
    SELECT 
        COUNT(*) as total_brands,
        COUNT(CASE WHEN WM_Brand_Media_Spend__c > 5000000 THEN 1 END) as high_spend_brands,
        COUNT(CASE WHEN WM_Brand_Media_Spend__c > 5000000 AND Audience_Attributes__c IS NOT NULL THEN 1 END) as high_spend_with_audience_attrs,
        COUNT(CASE WHEN WM_Brand_Media_Spend__c > 5000000 AND Audience_Description__c IS NOT NULL THEN 1 END) as high_spend_with_audience_desc
    FROM sf.Brands
    """
    
    counts = pd.read_sql(count_query, conn)
    print(f"Total Brands: {counts.iloc[0]['total_brands']:,}")
    print(f"High Spend (>$5M): {counts.iloc[0]['high_spend_brands']:,}")
    print(f"High Spend + Audience Attributes: {counts.iloc[0]['high_spend_with_audience_attrs']:,}")
    print(f"High Spend + Audience Description: {counts.iloc[0]['high_spend_with_audience_desc']:,}")
    
    # Sample high-spend brands
    print("\n3. SAMPLE HIGH-SPEND BRANDS:")
    print("-" * 30)
    
    sample_query = """
    SELECT TOP 10
        Name,
        WM_Brand_Media_Spend__c,
        WM_Brand_Industries__c,
        LEFT(WM_Brand_Description__c, 100) as Brand_Description_Sample,
        LEFT(Audience_Description__c, 100) as Audience_Description_Sample,
        LEFT(Audience_Attributes__c, 100) as Audience_Attributes_Sample
    FROM sf.Brands
    WHERE WM_Brand_Media_Spend__c > 5000000
    ORDER BY WM_Brand_Media_Spend__c DESC
    """
    
    samples_df = pd.read_sql(sample_query, conn)
    
    for _, row in samples_df.iterrows():
        print(f"\nBrand: {row['Name']}")
        print(f"Media Spend: ${row['WM_Brand_Media_Spend__c']:,.0f}")
        print(f"Industry: {row['WM_Brand_Industries__c']}")
        print(f"Description: {row['Brand_Description_Sample'] or 'N/A'}...")
        print(f"Audience Desc: {row['Audience_Description_Sample'] or 'N/A'}...")
        print(f"Audience Attrs: {row['Audience_Attributes_Sample'] or 'N/A'}...")
        print("-" * 50)
    
    # Analyze audience attributes format
    print("\n4. AUDIENCE ATTRIBUTES FORMAT ANALYSIS:")
    print("-" * 40)
    
    attrs_query = """
    SELECT TOP 20
        Name,
        Audience_Attributes__c
    FROM sf.Brands
    WHERE WM_Brand_Media_Spend__c > 5000000
    AND Audience_Attributes__c IS NOT NULL
    AND Audience_Attributes__c != ''
    """
    
    attrs_df = pd.read_sql(attrs_query, conn)
    
    if len(attrs_df) > 0:
        print("Sample Audience_Attributes__c values:")
        for _, row in attrs_df.head(5).iterrows():
            print(f"\n{row['Name']}:")
            print(f"  {row['Audience_Attributes__c']}")
    else:
        print("No brands found with populated Audience_Attributes__c")
    
    conn.close()

def analyze_industry_distribution():
    """
    Analyze industry distribution for high-spend brands
    """
    print("\n5. INDUSTRY DISTRIBUTION (>$5M BRANDS):")
    print("-" * 38)
    
    conn = get_connection()
    
    industry_query = """
    SELECT 
        WM_Brand_Industries__c,
        COUNT(*) as brand_count,
        AVG(WM_Brand_Media_Spend__c) as avg_spend,
        SUM(WM_Brand_Media_Spend__c) as total_spend
    FROM sf.Brands
    WHERE WM_Brand_Media_Spend__c > 5000000
    AND WM_Brand_Industries__c IS NOT NULL
    GROUP BY WM_Brand_Industries__c
    ORDER BY brand_count DESC
    """
    
    industry_df = pd.read_sql(industry_query, conn)
    
    print("Industry | Brand Count | Avg Spend | Total Spend")
    print("-" * 55)
    
    for _, row in industry_df.head(15).iterrows():
        industry = str(row['WM_Brand_Industries__c'])[:20]
        count = row['brand_count']
        avg_spend = row['avg_spend']
        total_spend = row['total_spend']
        
        print(f"{industry:20} | {count:11} | ${avg_spend:8,.0f} | ${total_spend:11,.0f}")
    
    conn.close()

def main():
    """
    Main execution function
    """
    analyze_brands_structure()
    analyze_industry_distribution()
    
    print("\n🎯 BRANDS ANALYSIS COMPLETE!")
    print("=" * 29)
    print("Ready for artist-brand matching analysis")

if __name__ == "__main__":
    main()