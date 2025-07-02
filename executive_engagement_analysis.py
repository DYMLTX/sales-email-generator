#!/usr/bin/env python3
"""
Executive Engagement Analysis for MAX.Live
Analyzes whether senior executives in meetings earlier leads to better conversion
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

def classify_seniority_level(title):
    """
    Classify contact seniority into detailed levels
    """
    if pd.isna(title) or title == '':
        return 'Unknown'
    
    title_lower = str(title).lower()
    
    # C-Level (Highest)
    if any(x in title_lower for x in ['ceo', 'chief executive', 'president', 'cmo', 'chief marketing', 
                                      'cto', 'chief technology', 'cfo', 'chief financial', 'coo', 'chief operating']):
        return 'C-Level'
    
    # Senior VP / EVP (Very High)
    if any(x in title_lower for x in ['senior vice president', 'senior vp', 'svp', 'executive vice president', 'evp']):
        return 'Senior VP'
    
    # VP Level (High)
    if any(x in title_lower for x in ['vice president', 'vp ', ' vp']):
        return 'VP'
    
    # Senior Director (Medium-High)
    if any(x in title_lower for x in ['senior director', 'sr director', 'sr. director']):
        return 'Senior Director'
    
    # Director (Medium)
    if 'director' in title_lower:
        return 'Director'
    
    # Senior Manager (Medium-Low)
    if any(x in title_lower for x in ['senior manager', 'sr manager', 'sr. manager']):
        return 'Senior Manager'
    
    # Manager (Low)
    if 'manager' in title_lower:
        return 'Manager'
    
    # Individual Contributor
    return 'Individual Contributor'

def analyze_executive_engagement_timing():
    """
    Analyze executive engagement timing patterns for converted vs non-converted accounts
    """
    print("🎯 EXECUTIVE ENGAGEMENT TIMING ANALYSIS")
    print("=" * 45)
    
    conn = get_connection()
    
    # Get meeting data with seniority classification (2019+ only)
    query = """
    WITH CustomerAccounts AS (
        SELECT DISTINCT AccountId
        FROM sf.Opportunity
        WHERE StageName = 'Closed Won'
        AND (CloseDate >= '2019-01-01' OR CloseDate IS NULL)  -- Include recent customers
    ),
    MeetingData AS (
        SELECT 
            a.Id as AccountId,
            a.Name as AccountName,
            CASE WHEN ca.AccountId IS NOT NULL THEN 1 ELSE 0 END as is_customer,
            c.Id as ContactId,
            c.Title,
            m.ActivityDate,
            m.Type,
            ROW_NUMBER() OVER (PARTITION BY a.Id ORDER BY m.ActivityDate) as meeting_sequence
        FROM sf.Account a
        LEFT JOIN CustomerAccounts ca ON ca.AccountId = a.Id
        LEFT JOIN sf.Contact c ON c.AccountId = a.Id
        LEFT JOIN sf.vMeetingSortASC m ON m.ContactId = c.Id
        WHERE m.Type LIKE 'New Business%'
        AND m.ActivityDate >= '2019-01-01'  -- Focus on recent meetings
        AND a.Name != 'Music Audience Exchange'
        AND a.Name NOT LIKE '%Ford%'  -- Exclude Ford to remove bias
        AND c.Title IS NOT NULL
        AND c.Title != ''
    )
    SELECT * FROM MeetingData
    ORDER BY AccountId, ActivityDate
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"📊 Analyzing {len(df):,} meeting records across {df['AccountId'].nunique():,} accounts")
    
    # Add seniority classification
    df['seniority_level'] = df['Title'].apply(classify_seniority_level)
    
    # Create seniority score (higher = more senior)
    seniority_scores = {
        'C-Level': 7,
        'Senior VP': 6,
        'VP': 5,
        'Senior Director': 4,
        'Director': 3,
        'Senior Manager': 2,
        'Manager': 1,
        'Individual Contributor': 0,
        'Unknown': 0
    }
    df['seniority_score'] = df['seniority_level'].map(seniority_scores)
    
    return df

def analyze_first_meeting_seniority(df):
    """
    Analyze seniority level of first meeting contacts
    """
    print("\n1. FIRST MEETING SENIORITY ANALYSIS:")
    print("-" * 37)
    
    # Get first meeting per account
    first_meetings = df[df['meeting_sequence'] == 1].copy()
    
    # Analyze by conversion status
    conversion_seniority = first_meetings.groupby(['is_customer', 'seniority_level']).size().unstack(fill_value=0)
    conversion_seniority_pct = conversion_seniority.div(conversion_seniority.sum(axis=1), axis=0) * 100
    
    print("First Meeting Seniority Distribution:")
    print("Seniority Level | Customers | Non-Customers | Customer %")
    print("-" * 60)
    
    for seniority in ['C-Level', 'Senior VP', 'VP', 'Senior Director', 'Director', 'Senior Manager', 'Manager', 'Individual Contributor']:
        if seniority in conversion_seniority.columns:
            customers = conversion_seniority.loc[1, seniority] if 1 in conversion_seniority.index else 0
            non_customers = conversion_seniority.loc[0, seniority] if 0 in conversion_seniority.index else 0
            
            total = customers + non_customers
            customer_pct = (customers / total * 100) if total > 0 else 0
            
            print(f"{seniority:15} | {customers:9} | {non_customers:13} | {customer_pct:9.1f}%")

def analyze_early_executive_access(df):
    """
    Analyze whether early executive access (first 3 meetings) correlates with conversion
    """
    print("\n2. EARLY EXECUTIVE ACCESS ANALYSIS:")
    print("-" * 36)
    
    # Define early meetings as first 3 meetings per account
    early_meetings = df[df['meeting_sequence'] <= 3].copy()
    
    # Calculate executive access metrics per account
    account_executive_metrics = early_meetings.groupby('AccountId').agg({
        'is_customer': 'first',  # Customer status
        'seniority_score': ['max', 'mean'],  # Highest and average seniority in early meetings
        'meeting_sequence': 'count'  # Number of early meetings
    }).round(2)
    
    # Flatten column names
    account_executive_metrics.columns = ['is_customer', 'max_early_seniority', 'avg_early_seniority', 'early_meeting_count']
    
    # Analyze by conversion status
    conversion_groups = account_executive_metrics.groupby('is_customer')
    
    print("Early Executive Access Metrics (First 3 Meetings):")
    print("Status | Accounts | Max Seniority | Avg Seniority | Early Meetings")
    print("-" * 70)
    
    for is_customer, group in conversion_groups:
        status = "Customer" if is_customer else "Non-Customer"
        count = len(group)
        max_seniority = group['max_early_seniority'].mean()
        avg_seniority = group['avg_early_seniority'].mean()
        early_meetings = group['early_meeting_count'].mean()
        
        print(f"{status:12} | {count:8} | {max_seniority:13.1f} | {avg_seniority:13.1f} | {early_meetings:13.1f}")
    
    # Statistical significance test
    from scipy import stats
    customers = account_executive_metrics[account_executive_metrics['is_customer'] == 1]
    non_customers = account_executive_metrics[account_executive_metrics['is_customer'] == 0]
    
    if len(customers) > 0 and len(non_customers) > 0:
        t_stat, p_value = stats.ttest_ind(customers['max_early_seniority'], non_customers['max_early_seniority'])
        print(f"\nStatistical Test (Max Early Seniority):")
        print(f"T-statistic: {t_stat:.3f}, P-value: {p_value:.3f}")
        print(f"Significant difference: {'Yes' if p_value < 0.05 else 'No'}")

def analyze_executive_progression(df):
    """
    Analyze how executive engagement progresses over time
    """
    print("\n3. EXECUTIVE ENGAGEMENT PROGRESSION:")
    print("-" * 37)
    
    # Create meeting buckets (early, middle, late)
    account_meeting_counts = df.groupby('AccountId')['meeting_sequence'].max()
    
    def categorize_meeting_phase(row):
        total_meetings = account_meeting_counts[row['AccountId']]
        sequence = row['meeting_sequence']
        
        if total_meetings <= 3:
            return 'Early' if sequence <= 2 else 'Late'
        elif total_meetings <= 6:
            if sequence <= 2:
                return 'Early'
            elif sequence <= 4:
                return 'Middle'
            else:
                return 'Late'
        else:
            third = total_meetings // 3
            if sequence <= third:
                return 'Early'
            elif sequence <= 2 * third:
                return 'Middle'
            else:
                return 'Late'
    
    df['meeting_phase'] = df.apply(categorize_meeting_phase, axis=1)
    
    # Analyze seniority by phase and conversion
    phase_analysis = df.groupby(['is_customer', 'meeting_phase']).agg({
        'seniority_score': ['mean', 'max', 'count']
    }).round(2)
    
    phase_analysis.columns = ['avg_seniority', 'max_seniority', 'meeting_count']
    
    print("Executive Engagement by Meeting Phase:")
    print("Status | Phase | Avg Seniority | Max Seniority | Meetings")
    print("-" * 60)
    
    for (is_customer, phase), row in phase_analysis.iterrows():
        status = "Customer" if is_customer else "Non-Customer"
        print(f"{status:12} | {phase:5} | {row['avg_seniority']:13.1f} | {row['max_seniority']:13.1f} | {row['meeting_count']:8.0f}")

def analyze_c_level_timing(df):
    """
    Analyze C-Level engagement timing specifically
    """
    print("\n4. C-LEVEL ENGAGEMENT TIMING:")
    print("-" * 31)
    
    # Focus on C-Level meetings
    c_level_meetings = df[df['seniority_level'] == 'C-Level'].copy()
    
    if len(c_level_meetings) == 0:
        print("❌ No C-Level meetings found in dataset")
        return
    
    # Analyze C-Level engagement by conversion
    c_level_by_account = c_level_meetings.groupby('AccountId').agg({
        'is_customer': 'first',
        'meeting_sequence': 'min',  # First C-Level meeting sequence
        'ContactId': 'nunique'  # Number of unique C-Level contacts
    }).rename(columns={'meeting_sequence': 'first_c_level_meeting', 'ContactId': 'c_level_contacts'})
    
    conversion_groups = c_level_by_account.groupby('is_customer')
    
    print("C-Level Engagement Analysis:")
    print("Status | Accounts | Avg First C-Level Meeting | Avg C-Level Contacts")
    print("-" * 70)
    
    for is_customer, group in conversion_groups:
        status = "Customer" if is_customer else "Non-Customer"
        count = len(group)
        first_meeting = group['first_c_level_meeting'].mean()
        c_contacts = group['c_level_contacts'].mean()
        
        print(f"{status:12} | {count:8} | {first_meeting:25.1f} | {c_contacts:19.1f}")

def create_executive_insights(df):
    """
    Generate key insights and recommendations
    """
    print("\n🎯 KEY EXECUTIVE ENGAGEMENT INSIGHTS:")
    print("-" * 38)
    
    # Calculate key metrics
    customers = df[df['is_customer'] == 1]
    non_customers = df[df['is_customer'] == 0]
    
    # Early executive access
    early_customers = customers[customers['meeting_sequence'] <= 3].groupby('AccountId')['seniority_score'].max().mean()
    early_non_customers = non_customers[non_customers['meeting_sequence'] <= 3].groupby('AccountId')['seniority_score'].max().mean()
    
    # C-Level presence
    customer_accounts_with_c_level = len(customers[customers['seniority_level'] == 'C-Level']['AccountId'].unique())
    total_customer_accounts = len(customers['AccountId'].unique())
    c_level_penetration_customers = (customer_accounts_with_c_level / total_customer_accounts * 100) if total_customer_accounts > 0 else 0
    
    non_customer_accounts_with_c_level = len(non_customers[non_customers['seniority_level'] == 'C-Level']['AccountId'].unique())
    total_non_customer_accounts = len(non_customers['AccountId'].unique())
    c_level_penetration_non_customers = (non_customer_accounts_with_c_level / total_non_customer_accounts * 100) if total_non_customer_accounts > 0 else 0
    
    print(f"📊 Early Executive Access (First 3 Meetings):")
    print(f"   Customers: {early_customers:.1f} avg seniority score")
    print(f"   Non-Customers: {early_non_customers:.1f} avg seniority score")
    print(f"   Difference: {early_customers - early_non_customers:.1f}")
    
    print(f"\n📊 C-Level Penetration:")
    print(f"   Customer Accounts: {c_level_penetration_customers:.1f}%")
    print(f"   Non-Customer Accounts: {c_level_penetration_non_customers:.1f}%")
    
    print(f"\n🎯 RECOMMENDATIONS:")
    print(f"1. {'Prioritize early executive access' if early_customers > early_non_customers else 'Executive timing may not be critical'}")
    print(f"2. {'C-Level engagement correlates with conversion' if c_level_penetration_customers > c_level_penetration_non_customers else 'C-Level access may not be decisive'}")
    print(f"3. Focus on {max(['VP', 'Director', 'Manager'], key=lambda x: len(customers[customers['seniority_level'] == x]))} level relationships")

def export_executive_analysis(df):
    """
    Export executive engagement analysis results
    """
    print("\n💾 EXPORTING EXECUTIVE ANALYSIS:")
    print("-" * 33)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Export account-level executive metrics
    account_metrics = df.groupby('AccountId').agg({
        'AccountName': 'first',
        'is_customer': 'first',
        'seniority_score': ['max', 'mean'],
        'ContactId': 'nunique',
        'meeting_sequence': 'max'
    })
    
    account_metrics.columns = ['AccountName', 'is_customer', 'max_seniority', 'avg_seniority', 'unique_contacts', 'total_meetings']
    
    # Add early executive access flag
    early_meetings = df[df['meeting_sequence'] <= 3]
    early_executive_access = early_meetings.groupby('AccountId')['seniority_score'].max()
    account_metrics['early_executive_access'] = account_metrics.index.map(early_executive_access).fillna(0)
    account_metrics['has_early_executive'] = account_metrics['early_executive_access'] >= 5  # VP+ level
    
    output_file = f'max_live_executive_engagement_analysis_{timestamp}.csv'
    account_metrics.reset_index().to_csv(output_file, index=False)
    print(f"✅ Executive analysis exported: {output_file} ({len(account_metrics):,} accounts)")

def main():
    """
    Main execution function
    """
    print("🎪 MAX.LIVE EXECUTIVE ENGAGEMENT ANALYSIS (2019+, Ford Excluded)")
    print("=" * 66)
    print("Analyzing executive engagement timing patterns (2019-2025 data, Ford accounts excluded)...\n")
    
    try:
        # Get meeting data with seniority analysis
        df = analyze_executive_engagement_timing()
        
        # Run analyses
        analyze_first_meeting_seniority(df)
        analyze_early_executive_access(df)
        analyze_executive_progression(df)
        analyze_c_level_timing(df)
        create_executive_insights(df)
        export_executive_analysis(df)
        
        print("\n🎯 EXECUTIVE ANALYSIS COMPLETE!")
        print("=" * 32)
        print("✅ Seniority patterns identified")
        print("✅ Executive timing analyzed")
        print("✅ Conversion correlations measured")
        print("✅ Strategic insights generated")
        print("🚀 Ready for executive engagement optimization!")
        
    except Exception as e:
        print(f"❌ Error in analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()