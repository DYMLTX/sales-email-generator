#!/usr/bin/env python3
"""
Comprehensive Prospect Scoring for MAX.Live
Score ALL Salesforce contacts and export to CSV/Excel
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

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

def score_all_contacts():
    """
    Score ALL Salesforce contacts with comprehensive analysis
    """
    print("🎯 COMPREHENSIVE MAX.LIVE PROSPECT SCORING")
    print("=" * 52)
    print("⏳ Scoring ALL Salesforce contacts... (this may take a few minutes)")
    
    query = """
    WITH MeetingContacts AS (
        -- Contacts with New Business meetings
        SELECT DISTINCT 
            c.Id as ContactId,
            c.AccountId,
            COUNT(*) as meeting_count,
            MAX(m.ActivityDate) as last_meeting_date
        FROM sf.Contact c
        INNER JOIN sf.vMeetingSortASC m ON c.Id = m.ContactId
        WHERE m.Type LIKE 'New Business%'
        GROUP BY c.Id, c.AccountId
    ),
    
    AccountMetrics AS (
        -- Account-level aggregated metrics
        SELECT 
            a.Id as AccountId,
            a.Name as AccountName,
            
            -- Brand metrics
            COUNT(DISTINCT b.Id) as brand_count,
            AVG(b.WM_Brand_Media_Spend__c) as avg_media_spend,
            MAX(b.WM_Brand_Media_Spend__c) as max_media_spend,
            AVG(b.Social_Spend__c) as avg_social_spend,
            SUM(CASE WHEN b.WM_Brand_Industries__c LIKE '%beer%' OR 
                           b.WM_Brand_Industries__c LIKE '%wine%' OR
                           b.WM_Brand_Industries__c LIKE '%liquor%' THEN 1 ELSE 0 END) as beverage_brands,
            SUM(CASE WHEN b.WM_Brand_Industries__c LIKE '%entertainment%' OR
                           b.WM_Brand_Industries__c LIKE '%media%' THEN 1 ELSE 0 END) as entertainment_brands,
            SUM(CASE WHEN b.WM_Brand_Industries__c LIKE '%automotive%' THEN 1 ELSE 0 END) as automotive_brands,
            SUM(CASE WHEN b.WM_Brand_Industries__c LIKE '%food%' OR
                           b.WM_Brand_Industries__c LIKE '%packaged%' THEN 1 ELSE 0 END) as food_brands,
            COUNT(CASE WHEN b.Audience_Attributes__c IS NOT NULL AND b.Audience_Attributes__c != '' THEN 1 END) as brands_with_audience_data,
            
            -- Planning cycles
            COUNT(CASE WHEN b.Buying_Period__c IS NOT NULL THEN 1 END) as brands_with_buying_period,
            COUNT(CASE WHEN b.Planning_Period__c IS NOT NULL THEN 1 END) as brands_with_planning_period,
            
            -- Meeting activity
            COUNT(DISTINCT mc.ContactId) as contacts_with_meetings,
            SUM(COALESCE(mc.meeting_count, 0)) as total_meetings,
            MAX(mc.last_meeting_date) as last_account_meeting
            
        FROM sf.Account a
        LEFT JOIN sf.Brands b ON b.Account__c = a.Id
        LEFT JOIN MeetingContacts mc ON mc.AccountId = a.Id
        WHERE a.Name != 'Music Audience Exchange'
        GROUP BY a.Id, a.Name
    ),
    
    ContactScores AS (
        -- Score all contacts comprehensively
        SELECT 
            c.Id as ContactId,
            c.AccountId,
            c.FirstName,
            c.LastName,
            c.Title,
            c.Email,
            c.Phone,
            am.AccountName,
            
            -- Account metrics
            COALESCE(am.brand_count, 0) as account_brand_count,
            COALESCE(am.avg_media_spend, 0) as account_avg_media_spend,
            COALESCE(am.max_media_spend, 0) as account_max_media_spend,
            COALESCE(am.avg_social_spend, 0) as account_avg_social_spend,
            COALESCE(am.beverage_brands, 0) as account_beverage_brands,
            COALESCE(am.entertainment_brands, 0) as account_entertainment_brands,
            COALESCE(am.automotive_brands, 0) as account_automotive_brands,
            COALESCE(am.food_brands, 0) as account_food_brands,
            COALESCE(am.brands_with_audience_data, 0) as brands_with_audience_data,
            COALESCE(am.contacts_with_meetings, 0) as account_contacts_with_meetings,
            COALESCE(am.total_meetings, 0) as account_total_meetings,
            am.last_account_meeting,
            
            -- Has meetings flag
            CASE WHEN mc.ContactId IS NOT NULL THEN 1 ELSE 0 END as has_new_business_meetings,
            COALESCE(mc.meeting_count, 0) as personal_meeting_count,
            mc.last_meeting_date as personal_last_meeting,
            
            -- Title scoring (0-30 points)
            CASE 
                WHEN c.Title LIKE '%Music%' THEN 30
                WHEN c.Title LIKE '%Marketing%' OR c.Title LIKE '%Brand%' THEN 25
                WHEN c.Title LIKE '%VP%' OR c.Title LIKE '%Vice President%' THEN 20
                WHEN c.Title LIKE '%Director%' THEN 15
                WHEN c.Title LIKE '%Manager%' THEN 10
                WHEN c.Title LIKE '%Coordinator%' OR c.Title LIKE '%Specialist%' THEN 8
                WHEN c.Title LIKE '%Associate%' THEN 6
                ELSE 5
            END as title_score,
            
            -- Account spend scoring (0-25 points)
            CASE 
                WHEN COALESCE(am.avg_media_spend, 0) >= 10000000 THEN 25
                WHEN COALESCE(am.avg_media_spend, 0) >= 5000000 THEN 20
                WHEN COALESCE(am.avg_media_spend, 0) >= 1000000 THEN 15
                WHEN COALESCE(am.avg_media_spend, 0) >= 500000 THEN 10
                WHEN COALESCE(am.avg_media_spend, 0) >= 100000 THEN 5
                ELSE 0
            END as spend_score,
            
            -- Brand portfolio scoring (0-20 points)
            CASE 
                WHEN COALESCE(am.brand_count, 0) >= 50 THEN 20
                WHEN COALESCE(am.brand_count, 0) >= 20 THEN 15
                WHEN COALESCE(am.brand_count, 0) >= 10 THEN 10
                WHEN COALESCE(am.brand_count, 0) >= 5 THEN 5
                WHEN COALESCE(am.brand_count, 0) >= 1 THEN 2
                ELSE 0
            END as portfolio_score,
            
            -- Industry alignment scoring (0-15 points) - MAX.Live focus
            CASE 
                WHEN COALESCE(am.beverage_brands, 0) >= 5 THEN 15
                WHEN COALESCE(am.beverage_brands, 0) >= 2 THEN 12
                WHEN COALESCE(am.beverage_brands, 0) >= 1 THEN 10
                WHEN COALESCE(am.entertainment_brands, 0) >= 2 THEN 8
                WHEN COALESCE(am.entertainment_brands, 0) >= 1 THEN 6
                WHEN COALESCE(am.automotive_brands, 0) >= 2 THEN 4
                WHEN COALESCE(am.automotive_brands, 0) >= 1 THEN 3
                ELSE 0
            END as industry_score,
            
            -- Account activity scoring (0-10 points)
            CASE 
                WHEN COALESCE(am.total_meetings, 0) >= 100 THEN 10
                WHEN COALESCE(am.total_meetings, 0) >= 50 THEN 8
                WHEN COALESCE(am.total_meetings, 0) >= 20 THEN 6
                WHEN COALESCE(am.total_meetings, 0) >= 10 THEN 4
                WHEN COALESCE(am.total_meetings, 0) >= 5 THEN 2
                WHEN COALESCE(am.total_meetings, 0) >= 1 THEN 1
                ELSE 0
            END as activity_score
            
        FROM sf.Contact c
        INNER JOIN AccountMetrics am ON am.AccountId = c.AccountId
        LEFT JOIN MeetingContacts mc ON mc.ContactId = c.Id
        WHERE c.Email IS NOT NULL 
        AND c.Email != ''
        AND c.Email NOT LIKE '%@musicaudienceexchange%'  -- Exclude internal emails
    )
    
    SELECT 
        *,
        -- Total score (0-100)
        title_score + spend_score + portfolio_score + industry_score + activity_score as total_score,
        
        -- Priority tier based on score
        CASE 
            WHEN (title_score + spend_score + portfolio_score + industry_score + activity_score) >= 85 THEN 'VERY HIGH'
            WHEN (title_score + spend_score + portfolio_score + industry_score + activity_score) >= 70 THEN 'HIGH'
            WHEN (title_score + spend_score + portfolio_score + industry_score + activity_score) >= 50 THEN 'MEDIUM'
            WHEN (title_score + spend_score + portfolio_score + industry_score + activity_score) >= 30 THEN 'LOW'
            ELSE 'VERY LOW'
        END as priority_tier,
        
        -- Industry focus for MAX.Live
        CASE 
            WHEN COALESCE(account_beverage_brands, 0) > 0 THEN 'Beverage'
            WHEN COALESCE(account_entertainment_brands, 0) > 0 THEN 'Entertainment'
            WHEN COALESCE(account_automotive_brands, 0) > 0 THEN 'Automotive'
            WHEN COALESCE(account_food_brands, 0) > 0 THEN 'Food & CPG'
            ELSE 'Other'
        END as primary_industry,
        
        -- Contact tier based on title
        CASE 
            WHEN Title LIKE '%Music%' THEN 'Music Specialist'
            WHEN Title LIKE '%VP%' OR Title LIKE '%Vice President%' THEN 'Executive'
            WHEN Title LIKE '%Director%' THEN 'Director'
            WHEN Title LIKE '%Marketing%' OR Title LIKE '%Brand%' THEN 'Marketing Professional'
            WHEN Title LIKE '%Manager%' THEN 'Manager'
            ELSE 'Other Role'
        END as contact_tier,
        
        -- Account size tier
        CASE 
            WHEN COALESCE(account_avg_media_spend, 0) >= 5000000 THEN 'Enterprise ($5M+)'
            WHEN COALESCE(account_avg_media_spend, 0) >= 1000000 THEN 'Large ($1M-$5M)'
            WHEN COALESCE(account_avg_media_spend, 0) >= 500000 THEN 'Medium ($500K-$1M)'
            WHEN COALESCE(account_avg_media_spend, 0) >= 100000 THEN 'Small ($100K-$500K)'
            ELSE 'Minimal (<$100K)'
        END as account_size_tier
        
    FROM ContactScores
    ORDER BY total_score DESC, account_avg_media_spend DESC
    """
    
    conn = get_connection()
    print("🔄 Executing comprehensive scoring query...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"✅ Scoring complete: {len(df):,} contacts analyzed")
    return df

def analyze_population_results(df):
    """
    Analyze the full population scoring results
    """
    print(f"\n📊 POPULATION ANALYSIS RESULTS:")
    print("-" * 35)
    
    # Overall statistics
    print(f"Total Contacts Scored: {len(df):,}")
    print(f"Average Score: {df['total_score'].mean():.1f}")
    print(f"Median Score: {df['total_score'].median():.1f}")
    print(f"Score Range: {df['total_score'].min():.0f} - {df['total_score'].max():.0f}")
    
    # Priority distribution
    print(f"\n🎯 PRIORITY DISTRIBUTION:")
    priority_dist = df['priority_tier'].value_counts()
    for tier in ['VERY HIGH', 'HIGH', 'MEDIUM', 'LOW', 'VERY LOW']:
        if tier in priority_dist:
            count = priority_dist[tier]
            pct = count / len(df) * 100
            meeting_rate = (df[df['priority_tier'] == tier]['has_new_business_meetings']).mean() * 100
            print(f"  {tier:10}: {count:6,} contacts ({pct:4.1f}%) - {meeting_rate:4.1f}% have meetings")
    
    # Industry distribution
    print(f"\n🏭 INDUSTRY DISTRIBUTION:")
    industry_dist = df['primary_industry'].value_counts()
    for industry, count in industry_dist.items():
        pct = count / len(df) * 100
        avg_score = df[df['primary_industry'] == industry]['total_score'].mean()
        print(f"  {industry:15}: {count:6,} contacts ({pct:4.1f}%) - Avg Score: {avg_score:.1f}")
    
    # Account size distribution
    print(f"\n💰 ACCOUNT SIZE DISTRIBUTION:")
    size_dist = df['account_size_tier'].value_counts()
    for size, count in size_dist.items():
        pct = count / len(df) * 100
        avg_score = df[df['account_size_tier'] == size]['total_score'].mean()
        print(f"  {size:20}: {count:6,} contacts ({pct:4.1f}%) - Avg Score: {avg_score:.1f}")
    
    # Top performers
    print(f"\n🏆 TOP 10 HIGHEST SCORING PROSPECTS:")
    print("-" * 41)
    
    top_10 = df.head(10)
    print("Name | Title | Company | Score | Industry | Size | Email")
    print("-" * 80)
    
    for _, row in top_10.iterrows():
        name = f"{row['FirstName']} {row['LastName']}"[:15] if pd.notna(row['FirstName']) else "Unknown"
        title = str(row['Title'])[:20] if pd.notna(row['Title']) else 'No Title'
        company = str(row['AccountName'])[:15] if pd.notna(row['AccountName']) else 'Unknown'
        score = int(row['total_score'])
        industry = str(row['primary_industry'])[:10]
        size = str(row['account_size_tier'])[:10]
        email = str(row['Email'])[:25] if pd.notna(row['Email']) else 'No Email'
        
        print(f"{name:15} | {title:20} | {company:15} | {score:3} | {industry:10} | {size:10} | {email}")

def export_results(df):
    """
    Export results to CSV and Excel files
    """
    print(f"\n💾 EXPORTING RESULTS:")
    print("-" * 21)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Full dataset
    csv_file = f"max_live_all_prospects_{timestamp}.csv"
    excel_file = f"max_live_all_prospects_{timestamp}.xlsx"
    
    # Export to CSV
    df.to_csv(csv_file, index=False)
    print(f"✅ CSV exported: {csv_file} ({len(df):,} records)")
    
    # Export to Excel with multiple sheets
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Full data
            df.to_excel(writer, sheet_name='All Prospects', index=False)
            
            # High priority prospects
            high_priority = df[df['priority_tier'].isin(['VERY HIGH', 'HIGH'])]
            high_priority.to_excel(writer, sheet_name='High Priority', index=False)
            
            # By industry
            beverage = df[df['primary_industry'] == 'Beverage']
            entertainment = df[df['primary_industry'] == 'Entertainment']
            automotive = df[df['primary_industry'] == 'Automotive']
            
            if len(beverage) > 0:
                beverage.to_excel(writer, sheet_name='Beverage Industry', index=False)
            if len(entertainment) > 0:
                entertainment.to_excel(writer, sheet_name='Entertainment', index=False)
            if len(automotive) > 0:
                automotive.to_excel(writer, sheet_name='Automotive', index=False)
            
            # Summary statistics
            summary_data = {
                'Metric': ['Total Contacts', 'Average Score', 'Median Score', 'Very High Priority', 'High Priority', 'Medium Priority'],
                'Value': [
                    len(df),
                    f"{df['total_score'].mean():.1f}",
                    f"{df['total_score'].median():.1f}",
                    len(df[df['priority_tier'] == 'VERY HIGH']),
                    len(df[df['priority_tier'] == 'HIGH']),
                    len(df[df['priority_tier'] == 'MEDIUM'])
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"✅ Excel exported: {excel_file} (multiple sheets)")
        
    except ImportError:
        print("⚠️  Excel export requires openpyxl: pip install openpyxl")
        print(f"📊 CSV file available: {csv_file}")
    
    # Create focused prospect lists
    top_500_file = f"max_live_top_500_prospects_{timestamp}.csv"
    df.head(500).to_csv(top_500_file, index=False)
    print(f"✅ Top 500 CSV: {top_500_file}")
    
    high_priority_file = f"max_live_high_priority_prospects_{timestamp}.csv"
    high_priority = df[df['priority_tier'].isin(['VERY HIGH', 'HIGH'])]
    high_priority.to_csv(high_priority_file, index=False)
    print(f"✅ High Priority CSV: {high_priority_file} ({len(high_priority):,} records)")
    
    return {
        'csv_file': csv_file,
        'excel_file': excel_file,
        'top_500_file': top_500_file,
        'high_priority_file': high_priority_file
    }

def main():
    """
    Main execution function
    """
    print("🎪 MAX.LIVE COMPREHENSIVE PROSPECT SCORING")
    print("=" * 50)
    
    # Score all contacts
    df = score_all_contacts()
    
    # Analyze results
    analyze_population_results(df)
    
    # Export to files
    files = export_results(df)
    
    print(f"\n🎯 ANALYSIS COMPLETE!")
    print("=" * 22)
    print("✅ Full population scored and analyzed")
    print("✅ Statistical distribution calculated")
    print("✅ Results exported to CSV and Excel")
    print("✅ Priority segments identified")
    print("🚀 Ready for comprehensive email campaign targeting!")
    
    return df, files

if __name__ == "__main__":
    df, files = main()