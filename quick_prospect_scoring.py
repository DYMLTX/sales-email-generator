#!/usr/bin/env python3
"""
Quick Prospect Scoring for MAX.Live
Fast statistical analysis to identify high-value prospects
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import numpy as np

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

def calculate_prospect_scores():
    """
    Calculate statistical prospect scores based on meeting patterns
    """
    print("🎯 MAX.LIVE PROSPECT SCORING ANALYSIS")
    print("=" * 50)
    
    query = """
    WITH MeetingContacts AS (
        -- Contacts with New Business meetings
        SELECT DISTINCT 
            c.Id as ContactId,
            c.AccountId,
            COUNT(*) as meeting_count
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
            SUM(CASE WHEN b.WM_Brand_Industries__c LIKE '%beer%' OR 
                           b.WM_Brand_Industries__c LIKE '%wine%' OR
                           b.WM_Brand_Industries__c LIKE '%liquor%' THEN 1 ELSE 0 END) as beverage_brands,
            
            -- Meeting activity
            COUNT(DISTINCT mc.ContactId) as contacts_with_meetings,
            SUM(mc.meeting_count) as total_meetings
            
        FROM sf.Account a
        LEFT JOIN sf.Brands b ON b.Account__c = a.Id
        LEFT JOIN MeetingContacts mc ON mc.AccountId = a.Id
        WHERE a.Name != 'Music Audience Exchange'
        GROUP BY a.Id, a.Name
    ),
    
    ContactScores AS (
        -- Score all contacts based on title and account metrics
        SELECT 
            c.Id as ContactId,
            c.AccountId,
            c.FirstName,
            c.LastName,
            c.Title,
            c.Email,
            am.AccountName,
            am.brand_count,
            am.avg_media_spend,
            am.beverage_brands,
            am.contacts_with_meetings,
            am.total_meetings,
            
            -- Has meetings flag
            CASE WHEN mc.ContactId IS NOT NULL THEN 1 ELSE 0 END as has_meetings,
            
            -- Title scoring (0-30 points)
            CASE 
                WHEN c.Title LIKE '%Music%' THEN 30
                WHEN c.Title LIKE '%Marketing%' OR c.Title LIKE '%Brand%' THEN 25
                WHEN c.Title LIKE '%VP%' OR c.Title LIKE '%Vice President%' THEN 20
                WHEN c.Title LIKE '%Director%' THEN 15
                WHEN c.Title LIKE '%Manager%' THEN 10
                ELSE 5
            END as title_score,
            
            -- Account spend scoring (0-25 points)
            CASE 
                WHEN am.avg_media_spend >= 10000000 THEN 25
                WHEN am.avg_media_spend >= 5000000 THEN 20
                WHEN am.avg_media_spend >= 1000000 THEN 15
                WHEN am.avg_media_spend >= 500000 THEN 10
                WHEN am.avg_media_spend >= 100000 THEN 5
                ELSE 0
            END as spend_score,
            
            -- Brand portfolio scoring (0-20 points)
            CASE 
                WHEN am.brand_count >= 50 THEN 20
                WHEN am.brand_count >= 20 THEN 15
                WHEN am.brand_count >= 10 THEN 10
                WHEN am.brand_count >= 5 THEN 5
                ELSE 0
            END as portfolio_score,
            
            -- Beverage industry bonus (0-15 points)
            CASE 
                WHEN am.beverage_brands >= 5 THEN 15
                WHEN am.beverage_brands >= 2 THEN 10
                WHEN am.beverage_brands >= 1 THEN 5
                ELSE 0
            END as beverage_score,
            
            -- Account meeting activity (0-10 points)
            CASE 
                WHEN am.total_meetings >= 100 THEN 10
                WHEN am.total_meetings >= 50 THEN 7
                WHEN am.total_meetings >= 20 THEN 5
                WHEN am.total_meetings >= 10 THEN 3
                WHEN am.total_meetings >= 1 THEN 1
                ELSE 0
            END as activity_score
            
        FROM sf.Contact c
        INNER JOIN AccountMetrics am ON am.AccountId = c.AccountId
        LEFT JOIN MeetingContacts mc ON mc.ContactId = c.Id
        WHERE c.Email IS NOT NULL 
        AND c.Email != ''
    )
    
    SELECT TOP 500
        *,
        -- Total score (0-100)
        title_score + spend_score + portfolio_score + beverage_score + activity_score as total_score,
        
        -- Priority tier
        CASE 
            WHEN (title_score + spend_score + portfolio_score + beverage_score + activity_score) >= 80 THEN 'VERY HIGH'
            WHEN (title_score + spend_score + portfolio_score + beverage_score + activity_score) >= 60 THEN 'HIGH'
            WHEN (title_score + spend_score + portfolio_score + beverage_score + activity_score) >= 40 THEN 'MEDIUM'
            ELSE 'LOW'
        END as priority_tier
        
    FROM ContactScores
    ORDER BY total_score DESC, avg_media_spend DESC
    """
    
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

def analyze_results(df):
    """
    Analyze and display scoring results
    """
    print(f"\n📊 ANALYSIS RESULTS ({len(df):,} prospects scored):")
    print("-" * 52)
    
    # Priority distribution
    print("Priority Distribution:")
    priority_dist = df['priority_tier'].value_counts()
    for tier, count in priority_dist.items():
        pct = count / len(df) * 100
        meeting_rate = (df[df['priority_tier'] == tier]['has_meetings']).mean() * 100
        print(f"  {tier:10}: {count:4} contacts ({pct:4.1f}%) - {meeting_rate:4.1f}% have meetings")
    
    # Top prospects
    print(f"\n🎯 TOP 20 PROSPECTS FOR MAX.LIVE:")
    print("-" * 42)
    
    top_20 = df.head(20)
    
    print("Name | Title | Score | Spend | Brands | Beverage | Meetings | Email")
    print("-" * 90)
    
    for _, row in top_20.iterrows():
        name = f"{row['FirstName']} {row['LastName']}"[:15] if pd.notna(row['FirstName']) else "Unknown"
        title = str(row['Title'])[:20] if pd.notna(row['Title']) else 'No Title'
        score = int(row['total_score'])
        spend = f"${row['avg_media_spend']:,.0f}" if row['avg_media_spend'] > 0 else "N/A"
        brands = int(row['brand_count']) if row['brand_count'] > 0 else 0
        beverage = "✓" if row['beverage_brands'] > 0 else "✗"
        meetings = "✓" if row['has_meetings'] else "✗"
        email = str(row['Email'])[:25] if pd.notna(row['Email']) else 'No Email'
        
        print(f"{name:15} | {title:20} | {score:3} | {spend:8} | {brands:4} | {beverage:8} | {meetings:8} | {email}")
    
    # Account-level insights
    print(f"\n🏢 TOP ACCOUNTS BY PROSPECT QUALITY:")
    print("-" * 38)
    
    account_summary = df.groupby(['AccountName']).agg({
        'total_score': ['count', 'mean', 'max'],
        'avg_media_spend': 'first',
        'beverage_brands': 'first',
        'has_meetings': 'sum'
    }).round(1)
    
    account_summary.columns = ['prospect_count', 'avg_score', 'max_score', 'media_spend', 'beverage_brands', 'contacts_with_meetings']
    account_summary = account_summary.sort_values(['avg_score', 'prospect_count'], ascending=False).head(15)
    
    print("Account | Prospects | Avg Score | Max Score | Media Spend | Beverage | Meetings")
    print("-" * 85)
    
    for account, row in account_summary.iterrows():
        account_name = str(account)[:20]
        prospects = int(row['prospect_count'])
        avg_score = row['avg_score']
        max_score = int(row['max_score'])
        spend = f"${row['media_spend']:,.0f}" if row['media_spend'] > 0 else "N/A"
        beverage = "✓" if row['beverage_brands'] > 0 else "✗"
        meetings = int(row['contacts_with_meetings'])
        
        print(f"{account_name:20} | {prospects:7} | {avg_score:7.1f} | {max_score:7} | {spend:9} | {beverage:8} | {meetings:6}")
    
    # Score breakdown analysis
    print(f"\n📈 SCORE COMPONENT ANALYSIS:")
    print("-" * 31)
    
    high_priority = df[df['priority_tier'].isin(['VERY HIGH', 'HIGH'])]
    
    print(f"High Priority Contacts ({len(high_priority)}): ")
    print(f"  Avg Title Score: {high_priority['title_score'].mean():.1f}/30")
    print(f"  Avg Spend Score: {high_priority['spend_score'].mean():.1f}/25") 
    print(f"  Avg Portfolio Score: {high_priority['portfolio_score'].mean():.1f}/20")
    print(f"  Avg Beverage Score: {high_priority['beverage_score'].mean():.1f}/15")
    print(f"  Avg Activity Score: {high_priority['activity_score'].mean():.1f}/10")
    
    return df

def save_results(df):
    """
    Save results to CSV for campaign use
    """
    # Save full results
    output_file = 'max_live_prospect_scores.csv'
    df.to_csv(output_file, index=False)
    
    # Save high priority contacts only
    high_priority = df[df['priority_tier'].isin(['VERY HIGH', 'HIGH'])]
    high_priority_file = 'max_live_high_priority_prospects.csv'
    high_priority.to_csv(high_priority_file, index=False)
    
    print(f"\n💾 Results saved:")
    print(f"  Full results: {output_file} ({len(df)} contacts)")
    print(f"  High priority: {high_priority_file} ({len(high_priority)} contacts)")
    
    return output_file, high_priority_file

def main():
    """
    Main execution function
    """
    print("🎪 MAX.LIVE STATISTICAL PROSPECT SCORING")
    print("=" * 48)
    
    # Calculate scores
    df = calculate_prospect_scores()
    
    # Analyze results  
    df_analyzed = analyze_results(df)
    
    # Save results
    output_files = save_results(df_analyzed)
    
    print(f"\n🎯 READY FOR EMAIL CAMPAIGN TARGETING!")
    print("=" * 42)
    print("✅ High-value prospects identified and scored")
    print("✅ Priority tiers assigned for outreach strategy") 
    print("✅ Account-level insights for campaign planning")
    print("🚀 Begin with VERY HIGH and HIGH priority contacts!")

if __name__ == "__main__":
    main()