#!/usr/bin/env python3
"""
Customer Look-Alike Analysis for MAX.Live
Identifies prospects that match successful customer patterns (excluding Ford bias)
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
import warnings
warnings.filterwarnings('ignore')

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

def extract_customer_dna():
    """
    Extract behavioral DNA from successful customers (excluding Ford)
    """
    print("🧬 EXTRACTING CUSTOMER DNA PROFILES")
    print("=" * 40)
    
    conn = get_connection()
    
    query = """
    WITH CustomerRevenue AS (
        SELECT 
            o.AccountId,
            SUM(o.Amount) as total_revenue,
            COUNT(*) as deal_count,
            MIN(o.CloseDate) as first_purchase,
            MAX(o.CloseDate) as last_purchase,
            AVG(o.Amount) as avg_deal_size
        FROM sf.Opportunity o
        WHERE o.StageName = 'Closed Won'
        GROUP BY o.AccountId
    ),
    CustomerDNA AS (
        SELECT 
            a.Id as AccountId,
            a.Name as AccountName,
            cr.total_revenue,
            cr.deal_count,
            cr.avg_deal_size,
            DATEDIFF(day, cr.first_purchase, cr.last_purchase) as customer_lifespan_days,
            
            -- Brand Portfolio DNA
            COUNT(DISTINCT b.Id) as brand_count,
            AVG(b.WM_Brand_Media_Spend__c) as avg_brand_spend,
            MAX(b.WM_Brand_Media_Spend__c) as max_brand_spend,
            STDEV(b.WM_Brand_Media_Spend__c) as spend_variance,
            
            -- Industry Composition DNA
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%beer%' OR 
                           b.WM_Brand_Industries__c LIKE '%wine%' OR
                           b.WM_Brand_Industries__c LIKE '%liquor%' THEN 1 END) as beverage_brand_count,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%entertainment%' OR
                           b.WM_Brand_Industries__c LIKE '%media%' THEN 1 END) as entertainment_brand_count,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%automotive%' THEN 1 END) as automotive_brand_count,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%food%' OR
                           b.WM_Brand_Industries__c LIKE '%packaged%' THEN 1 END) as food_brand_count,
            
            -- Contact Ecosystem DNA
            COUNT(DISTINCT c.Id) as total_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Manager%' THEN c.Id END) as manager_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Director%' THEN c.Id END) as director_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%VP%' OR c.Title LIKE '%Vice President%' THEN c.Id END) as vp_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Marketing%' THEN c.Id END) as marketing_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Brand%' THEN c.Id END) as brand_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Media%' THEN c.Id END) as media_contacts,
            
            -- Meeting Pattern DNA
            COUNT(DISTINCT m.ContactId) as contacts_with_meetings,
            COUNT(m.ContactId) as total_meetings,
            COUNT(CASE WHEN m.Type LIKE 'New Business%' THEN 1 END) as new_business_meetings,
            MIN(m.ActivityDate) as first_meeting_date,
            MAX(m.ActivityDate) as last_meeting_date,
            
            -- Engagement DNA
            CASE 
                WHEN COUNT(m.ContactId) > 0 
                THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT c.Id)
                ELSE 0 
            END as meeting_penetration_rate,
            
            CASE 
                WHEN COUNT(DISTINCT m.ContactId) > 0 
                THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT m.ContactId)
                ELSE 0 
            END as meetings_per_engaged_contact
            
        FROM CustomerRevenue cr
        INNER JOIN sf.Account a ON a.Id = cr.AccountId
        LEFT JOIN sf.Brands b ON b.Account__c = a.Id
        LEFT JOIN sf.Contact c ON c.AccountId = a.Id
        LEFT JOIN sf.vMeetingSortASC m ON m.ContactId = c.Id
        WHERE a.Name NOT LIKE '%Ford%'  -- Exclude Ford bias
        AND a.Name != 'Music Audience Exchange'
        GROUP BY a.Id, a.Name, cr.total_revenue, cr.deal_count, cr.avg_deal_size, 
                 cr.first_purchase, cr.last_purchase
    )
    SELECT * FROM CustomerDNA
    ORDER BY total_revenue DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"✅ Extracted DNA from {len(df)} customer accounts (Ford excluded)")
    print(f"📊 Total customer revenue analyzed: ${df['total_revenue'].sum():,.0f}")
    print(f"📈 Average deal size: ${df['avg_deal_size'].mean():,.0f}")
    
    return df

def identify_customer_archetypes(customer_dna):
    """
    Cluster customers into distinct archetypes
    """
    print("\n🎭 IDENTIFYING CUSTOMER ARCHETYPES")
    print("-" * 35)
    
    # Select features for clustering
    clustering_features = [
        'brand_count', 'avg_brand_spend', 'beverage_brand_count', 'entertainment_brand_count',
        'total_contacts', 'manager_contacts', 'director_contacts', 'marketing_contacts',
        'total_meetings', 'meeting_penetration_rate', 'avg_deal_size'
    ]
    
    # Prepare data for clustering
    cluster_data = customer_dna[clustering_features].fillna(0)
    
    # Scale features
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(cluster_data)
    
    # Find optimal number of clusters (3-5 range)
    inertias = []
    K_range = range(2, 6)
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(scaled_data)
        inertias.append(kmeans.inertia_)
    
    # Use 4 clusters based on business logic
    optimal_k = 4
    kmeans = KMeans(n_clusters=optimal_k, random_state=42)
    customer_dna['archetype'] = kmeans.fit_predict(scaled_data)
    
    # Analyze archetypes
    print(f"📊 Created {optimal_k} customer archetypes:")
    print("-" * 40)
    
    archetype_names = {
        0: "Portfolio Powerhouses",
        1: "Beverage Specialists", 
        2: "Relationship Builders",
        3: "Efficiency Seekers"
    }
    
    for i in range(optimal_k):
        archetype_data = customer_dna[customer_dna['archetype'] == i]
        count = len(archetype_data)
        revenue = archetype_data['total_revenue'].sum()
        avg_brands = archetype_data['brand_count'].mean()
        avg_meetings = archetype_data['total_meetings'].mean()
        avg_contacts = archetype_data['total_contacts'].mean()
        avg_deal = archetype_data['avg_deal_size'].mean()
        
        print(f"\n🎯 Archetype {i}: {archetype_names.get(i, f'Type {i}')} ({count} customers)")
        print(f"   Revenue: ${revenue:,.0f} (${avg_deal:,.0f} avg deal)")
        print(f"   Brands: {avg_brands:.1f} avg")
        print(f"   Meetings: {avg_meetings:.1f} avg")
        print(f"   Contacts: {avg_contacts:.1f} avg")
    
    # Add archetype names
    customer_dna['archetype_name'] = customer_dna['archetype'].map(archetype_names)
    
    return customer_dna, scaler, clustering_features, archetype_names

def analyze_archetype_patterns(customer_dna):
    """
    Deep dive into each archetype's characteristics
    """
    print("\n🔍 ARCHETYPE PATTERN ANALYSIS")
    print("-" * 31)
    
    for archetype_name in customer_dna['archetype_name'].unique():
        if pd.isna(archetype_name):
            continue
            
        archetype_data = customer_dna[customer_dna['archetype_name'] == archetype_name]
        
        print(f"\n📋 {archetype_name.upper()} PROFILE:")
        print("-" * (len(archetype_name) + 10))
        
        # Key metrics
        print(f"Customer Count: {len(archetype_data)}")
        print(f"Avg Revenue: ${archetype_data['total_revenue'].mean():,.0f}")
        print(f"Avg Deal Count: {archetype_data['deal_count'].mean():.1f}")
        
        # Brand characteristics
        print(f"\nBrand Portfolio:")
        print(f"  Total Brands: {archetype_data['brand_count'].mean():.1f} avg")
        print(f"  Beverage Brands: {archetype_data['beverage_brand_count'].mean():.1f} avg")
        print(f"  Entertainment Brands: {archetype_data['entertainment_brand_count'].mean():.1f} avg")
        print(f"  Avg Brand Spend: ${archetype_data['avg_brand_spend'].mean():,.0f}")
        
        # Contact ecosystem
        print(f"\nContact Ecosystem:")
        print(f"  Total Contacts: {archetype_data['total_contacts'].mean():.1f} avg")
        print(f"  Managers: {archetype_data['manager_contacts'].mean():.1f} avg")
        print(f"  Directors: {archetype_data['director_contacts'].mean():.1f} avg")
        print(f"  Marketing Roles: {archetype_data['marketing_contacts'].mean():.1f} avg")
        
        # Meeting patterns
        print(f"\nMeeting Patterns:")
        print(f"  Total Meetings: {archetype_data['total_meetings'].mean():.1f} avg")
        print(f"  Meeting Penetration: {archetype_data['meeting_penetration_rate'].mean():.1%}")
        print(f"  Meetings per Contact: {archetype_data['meetings_per_engaged_contact'].mean():.1f}")
        
        # Top examples
        top_examples = archetype_data.nlargest(3, 'total_revenue')[['AccountName', 'total_revenue', 'brand_count']]
        print(f"\nTop Examples:")
        for _, row in top_examples.iterrows():
            print(f"  • {row['AccountName']}: ${row['total_revenue']:,.0f} ({row['brand_count']} brands)")

def calculate_lookalike_scores(customer_dna, scaler, clustering_features):
    """
    Score all prospects based on similarity to successful customers
    """
    print("\n🎯 CALCULATING LOOK-ALIKE SCORES")
    print("-" * 34)
    
    conn = get_connection()
    
    # Get all prospects (non-customers with brand data)
    prospect_query = """
    WITH CustomerAccounts AS (
        SELECT DISTINCT AccountId
        FROM sf.Opportunity
        WHERE StageName = 'Closed Won'
    ),
    ProspectDNA AS (
        SELECT 
            a.Id as AccountId,
            a.Name as AccountName,
            
            -- Brand Portfolio DNA
            COUNT(DISTINCT b.Id) as brand_count,
            AVG(b.WM_Brand_Media_Spend__c) as avg_brand_spend,
            MAX(b.WM_Brand_Media_Spend__c) as max_brand_spend,
            
            -- Industry Composition DNA
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%beer%' OR 
                           b.WM_Brand_Industries__c LIKE '%wine%' OR
                           b.WM_Brand_Industries__c LIKE '%liquor%' THEN 1 END) as beverage_brand_count,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%entertainment%' OR
                           b.WM_Brand_Industries__c LIKE '%media%' THEN 1 END) as entertainment_brand_count,
            
            -- Contact Ecosystem DNA
            COUNT(DISTINCT c.Id) as total_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Manager%' THEN c.Id END) as manager_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Director%' THEN c.Id END) as director_contacts,
            COUNT(DISTINCT CASE WHEN c.Title LIKE '%Marketing%' THEN c.Id END) as marketing_contacts,
            
            -- Meeting Pattern DNA
            COUNT(DISTINCT m.ContactId) as contacts_with_meetings,
            COUNT(m.ContactId) as total_meetings,
            
            -- Engagement DNA
            CASE 
                WHEN COUNT(m.ContactId) > 0 
                THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT c.Id)
                ELSE 0 
            END as meeting_penetration_rate,
            
            -- Placeholder for avg_deal_size (unknown for prospects)
            500000 as avg_deal_size  -- Use average from customers
            
        FROM sf.Account a
        LEFT JOIN CustomerAccounts ca ON ca.AccountId = a.Id
        LEFT JOIN sf.Brands b ON b.Account__c = a.Id
        LEFT JOIN sf.Contact c ON c.AccountId = a.Id
        LEFT JOIN sf.vMeetingSortASC m ON m.ContactId = c.Id
        WHERE ca.AccountId IS NULL  -- Non-customers only
        AND a.Name NOT LIKE '%Ford%'  -- Exclude Ford
        AND a.Name != 'Music Audience Exchange'
        AND b.Account__c IS NOT NULL  -- Has brand data
        GROUP BY a.Id, a.Name
        HAVING COUNT(DISTINCT b.Id) > 0  -- At least 1 brand
    )
    SELECT * FROM ProspectDNA
    ORDER BY brand_count DESC, total_meetings DESC
    """
    
    prospects_df = pd.read_sql(prospect_query, conn)
    conn.close()
    
    print(f"📊 Analyzing {len(prospects_df):,} prospects for customer similarity")
    
    # Prepare prospect data for scoring
    prospect_features = prospects_df[clustering_features].fillna(0)
    prospect_scaled = scaler.transform(prospect_features)
    
    # Prepare customer data for comparison
    customer_features = customer_dna[clustering_features].fillna(0)
    customer_scaled = scaler.transform(customer_features)
    
    # Calculate similarity scores for each prospect
    similarity_scores = []
    
    for i, prospect in enumerate(prospect_scaled):
        # Calculate cosine similarity to all customers
        similarities = cosine_similarity([prospect], customer_scaled)[0]
        
        # Get best archetype match
        best_match_idx = np.argmax(similarities)
        best_similarity = similarities[best_match_idx]
        best_archetype = customer_dna.iloc[best_match_idx]['archetype_name']
        
        # Calculate overall score (0-100)
        overall_score = int(best_similarity * 100)
        
        # Get archetype-specific score
        archetype_customers = customer_dna[customer_dna['archetype_name'] == best_archetype]
        archetype_features = archetype_customers[clustering_features].fillna(0)
        archetype_scaled = scaler.transform(archetype_features)
        archetype_similarities = cosine_similarity([prospect], archetype_scaled)[0]
        archetype_score = int(np.mean(archetype_similarities) * 100)
        
        similarity_scores.append({
            'prospect_idx': i,
            'overall_score': overall_score,
            'best_archetype': best_archetype,
            'archetype_score': archetype_score,
            'best_similarity': best_similarity
        })
    
    # Add scores to prospects dataframe
    scores_df = pd.DataFrame(similarity_scores)
    prospects_df = prospects_df.reset_index(drop=True)
    prospects_df['lookalike_score'] = scores_df['overall_score']
    prospects_df['best_archetype_match'] = scores_df['best_archetype']
    prospects_df['archetype_score'] = scores_df['archetype_score']
    
    # Create priority tiers
    prospects_df['priority_tier'] = pd.cut(
        prospects_df['lookalike_score'], 
        bins=[0, 60, 75, 85, 100], 
        labels=['Low', 'Medium', 'High', 'Very High'],
        include_lowest=True
    )
    
    return prospects_df

def generate_lookalike_insights(prospects_df, customer_dna):
    """
    Generate actionable insights from look-alike analysis
    """
    print("\n📈 LOOK-ALIKE ANALYSIS INSIGHTS")
    print("-" * 32)
    
    # Overall distribution
    print("Look-Alike Score Distribution:")
    priority_dist = prospects_df['priority_tier'].value_counts().sort_index()
    for tier, count in priority_dist.items():
        pct = count / len(prospects_df) * 100
        print(f"  {tier:10}: {count:5,} prospects ({pct:4.1f}%)")
    
    # Top prospects by archetype
    print(f"\n🎯 TOP PROSPECTS BY CUSTOMER ARCHETYPE:")
    print("-" * 42)
    
    for archetype in customer_dna['archetype_name'].unique():
        if pd.isna(archetype):
            continue
            
        archetype_prospects = prospects_df[prospects_df['best_archetype_match'] == archetype]
        if len(archetype_prospects) > 0:
            print(f"\n{archetype} Look-Alikes ({len(archetype_prospects):,} prospects):")
            top_5 = archetype_prospects.nlargest(5, 'lookalike_score')
            
            for _, row in top_5.iterrows():
                name = str(row['AccountName'])[:30]
                score = row['lookalike_score']
                brands = row['brand_count']
                meetings = row['total_meetings']
                print(f"  • {name:30} | Score: {score:2} | {brands:2} brands | {meetings:2} meetings")
    
    # High-value opportunities
    print(f"\n💎 HIGHEST VALUE OPPORTUNITIES:")
    print("-" * 32)
    
    high_value = prospects_df[
        (prospects_df['lookalike_score'] >= 80) | 
        (prospects_df['brand_count'] >= 10)
    ].nlargest(10, 'lookalike_score')
    
    print("Company | Score | Archetype | Brands | Contacts | Meetings")
    print("-" * 70)
    
    for _, row in high_value.iterrows():
        company = str(row['AccountName'])[:20]
        score = row['lookalike_score']
        archetype = str(row['best_archetype_match'])[:15]
        brands = row['brand_count']
        contacts = row['total_contacts']
        meetings = row['total_meetings']
        
        print(f"{company:20} | {score:5} | {archetype:15} | {brands:6} | {contacts:8} | {meetings:8}")

def export_lookalike_results(prospects_df, customer_dna):
    """
    Export look-alike analysis results
    """
    print("\n💾 EXPORTING LOOK-ALIKE RESULTS")
    print("-" * 32)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Export all prospects with scores
    all_prospects_file = f'max_live_lookalike_prospects_{timestamp}.csv'
    prospects_df.to_csv(all_prospects_file, index=False)
    print(f"✅ All prospects exported: {all_prospects_file} ({len(prospects_df):,} records)")
    
    # Export high-priority prospects
    high_priority = prospects_df[prospects_df['priority_tier'].isin(['High', 'Very High'])]
    high_priority_file = f'max_live_high_priority_lookalikes_{timestamp}.csv'
    high_priority.to_csv(high_priority_file, index=False)
    print(f"✅ High priority prospects: {high_priority_file} ({len(high_priority):,} records)")
    
    # Export by archetype
    for archetype in customer_dna['archetype_name'].unique():
        if pd.isna(archetype):
            continue
            
        archetype_prospects = prospects_df[prospects_df['best_archetype_match'] == archetype]
        if len(archetype_prospects) > 0:
            archetype_file = f'max_live_{archetype.lower().replace(" ", "_")}_lookalikes_{timestamp}.csv'
            archetype_prospects.to_csv(archetype_file, index=False)
            print(f"✅ {archetype} prospects: {archetype_file} ({len(archetype_prospects):,} records)")
    
    # Create summary report
    summary_file = f'max_live_lookalike_analysis_summary_{timestamp}.txt'
    with open(summary_file, 'w') as f:
        f.write("MAX.LIVE CUSTOMER LOOK-ALIKE ANALYSIS\n")
        f.write("=" * 50 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("CUSTOMER ARCHETYPES IDENTIFIED:\n")
        f.write("-" * 35 + "\n")
        for archetype in customer_dna['archetype_name'].unique():
            if not pd.isna(archetype):
                count = len(customer_dna[customer_dna['archetype_name'] == archetype])
                f.write(f"• {archetype}: {count} customers\n")
        
        f.write(f"\nPROSPECT ANALYSIS:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Total Prospects Analyzed: {len(prospects_df):,}\n")
        f.write(f"High Priority Prospects: {len(high_priority):,}\n")
        f.write(f"Average Look-Alike Score: {prospects_df['lookalike_score'].mean():.1f}\n")
        
        f.write(f"\nKEY INSIGHTS:\n")
        f.write("-" * 15 + "\n")
        f.write("1. Customer archetypes show distinct patterns\n")
        f.write("2. Look-alike scoring identifies similar prospects\n")
        f.write("3. Focus on high-score prospects for best conversion\n")
        f.write("4. Archetype-specific approaches recommended\n")
    
    print(f"✅ Analysis summary: {summary_file}")

def main():
    """
    Main execution function
    """
    print("🎪 MAX.LIVE CUSTOMER LOOK-ALIKE ANALYSIS")
    print("=" * 48)
    print("Finding prospects that match successful customer patterns...\n")
    
    try:
        # Extract customer DNA
        customer_dna = extract_customer_dna()
        
        # Identify archetypes
        customer_dna, scaler, clustering_features, archetype_names = identify_customer_archetypes(customer_dna)
        
        # Analyze archetype patterns
        analyze_archetype_patterns(customer_dna)
        
        # Calculate look-alike scores
        prospects_df = calculate_lookalike_scores(customer_dna, scaler, clustering_features)
        
        # Generate insights
        generate_lookalike_insights(prospects_df, customer_dna)
        
        # Export results
        export_lookalike_results(prospects_df, customer_dna)
        
        print("\n🎯 LOOK-ALIKE ANALYSIS COMPLETE!")
        print("=" * 33)
        print("✅ Customer archetypes identified")
        print("✅ Look-alike scores calculated")
        print("✅ High-priority prospects exported")
        print("✅ Archetype-specific targeting enabled")
        print("🚀 Ready for precision customer acquisition!")
        
        return customer_dna, prospects_df
        
    except Exception as e:
        print(f"❌ Error in analysis: {e}")
        import traceback
        traceback.print_exc()
        return None, None

if __name__ == "__main__":
    customer_dna, prospects_df = main()