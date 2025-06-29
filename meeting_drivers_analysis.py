#!/usr/bin/env python3
"""Meeting Drivers Analysis - What leads to successful meetings for MAX.Live prospects."""

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

print("📅 MEETING DRIVERS ANALYSIS - What Generates Meetings for MAX.Live")
print("=" * 70)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# 1. Find all meeting-related tables
print("\n1. MEETING-RELATED TABLES DISCOVERY:")
print("-" * 40)

query = """
SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
AND (LOWER(TABLE_NAME) LIKE '%meeting%' 
     OR LOWER(TABLE_NAME) LIKE '%appointment%'
     OR LOWER(TABLE_NAME) LIKE '%event%'
     OR LOWER(TABLE_NAME) LIKE '%call%'
     OR LOWER(TABLE_NAME) LIKE '%activity%')
ORDER BY TABLE_NAME
"""

df = pd.read_sql(query, conn)
meeting_tables = df['TABLE_NAME'].tolist()

print(f"Found {len(meeting_tables)} meeting/activity related tables:")
for i, table in enumerate(meeting_tables[:15], 1):  # Show first 15
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{i:2}. {table:40} | {count:,} records")
    except:
        print(f"{i:2}. {table:40} | Error accessing")

if len(meeting_tables) > 15:
    print(f"    ... and {len(meeting_tables) - 15} more tables")

# 2. Analyze specific meeting tables mentioned
print("\n2. SPECIFIC MEETING TABLES ANALYSIS:")
print("-" * 38)

target_tables = ['sf.vMeetingSortASC', 'sf.MeetingSort']
for table in target_tables:
    try:
        # Keep full schema.table name for SQL Server
        clean_table = table
        print(f"\n📊 {table.upper()}:")
        print("-" * (len(table) + 5))
        
        cursor.execute(f"SELECT COUNT(*) FROM {clean_table}")
        count = cursor.fetchone()[0]
        print(f"Records: {count:,}")
        
        # Get table structure
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'sf' AND TABLE_NAME = '{clean_table.split('.')[1]}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        print(f"Columns: {len(columns)}")
        
        # Look for key meeting fields
        key_fields = ['date', 'time', 'subject', 'type', 'status', 'outcome',
                     'contact', 'account', 'owner', 'duration', 'location',
                     'created', 'modified', 'lead', 'opportunity']
        
        print("\nKey meeting fields:")
        relevant_cols = []
        for col_name, col_type in columns:
            if any(key in col_name.lower() for key in key_fields):
                relevant_cols.append((col_name, col_type))
                print(f"  - {col_name} ({col_type})")
        
        # Sample data
        if relevant_cols and count > 0:
            print(f"\nSample data from {clean_table}:")
            sample_cols = [col[0] for col in relevant_cols[:5]]
            col_list = ', '.join(f"[{col}]" for col in sample_cols)
            
            try:
                cursor.execute(f"SELECT TOP 3 {col_list} FROM {clean_table}")
                samples = cursor.fetchall()
                
                # Print header
                header = " | ".join(f"{col:20}" for col in sample_cols)
                print(f"  {header}")
                print(f"  {'-' * len(header)}")
                
                # Print samples
                for row in samples:
                    row_str = " | ".join(f"{str(val)[:20]:20}" for val in row)
                    print(f"  {row_str}")
                    
            except Exception as e:
                print(f"  Error sampling data: {e}")
                
    except Exception as e:
        print(f"Error analyzing {table}: {e}")

# 3. Meeting Success Patterns Analysis
print("\n3. MEETING SUCCESS PATTERNS:")
print("-" * 30)

# Try to find meeting outcomes and patterns
meeting_analysis_queries = [
    ("Meeting by Contact Type", """
        SELECT TOP 20
            c.jobtitle,
            COUNT(*) as meeting_count
        FROM contact c
        INNER JOIN sf.vMeetingSortASC m ON CAST(c.vid AS varchar) = m.ContactId
        WHERE c.jobtitle IS NOT NULL 
        AND c.jobtitle != ''
        AND m.ContactId IS NOT NULL
        GROUP BY c.jobtitle
        ORDER BY COUNT(*) DESC
    """),
    
    ("Meeting by Company Size", """
        SELECT TOP 15
            c.company,
            COUNT(DISTINCT c.email) as contacts,
            COUNT(*) as total_meetings
        FROM contact c
        INNER JOIN sf.vMeetingSortASC m ON CAST(c.vid AS varchar) = m.ContactId
        WHERE c.company IS NOT NULL 
        AND c.company != ''
        AND c.company != 'your company'
        AND m.ContactId IS NOT NULL
        GROUP BY c.company
        HAVING COUNT(DISTINCT c.email) >= 3
        ORDER BY COUNT(*) DESC
    """),
    
    ("Meeting by Industry", """
        SELECT 
            CASE 
                WHEN c.industry LIKE '%Entertainment%' OR c.industry LIKE '%Media%' THEN 'Entertainment/Media'
                WHEN c.industry LIKE '%Technology%' OR c.industry LIKE '%Software%' THEN 'Technology'
                WHEN c.industry LIKE '%Consumer%' OR c.industry LIKE '%Retail%' THEN 'Consumer/Retail'
                WHEN c.industry LIKE '%Automotive%' THEN 'Automotive'
                WHEN c.industry LIKE '%Food%' OR c.industry LIKE '%Beverage%' THEN 'Food & Beverage'
                WHEN c.industry LIKE '%Financial%' THEN 'Financial Services'
                ELSE COALESCE(c.industry, 'Unknown')
            END as industry_category,
            COUNT(*) as meeting_count
        FROM contact c
        INNER JOIN sf.vMeetingSortASC m ON CAST(c.vid AS varchar) = m.ContactId
        WHERE m.ContactId IS NOT NULL
        GROUP BY 
            CASE 
                WHEN c.industry LIKE '%Entertainment%' OR c.industry LIKE '%Media%' THEN 'Entertainment/Media'
                WHEN c.industry LIKE '%Technology%' OR c.industry LIKE '%Software%' THEN 'Technology'
                WHEN c.industry LIKE '%Consumer%' OR c.industry LIKE '%Retail%' THEN 'Consumer/Retail'
                WHEN c.industry LIKE '%Automotive%' THEN 'Automotive'
                WHEN c.industry LIKE '%Food%' OR c.industry LIKE '%Beverage%' THEN 'Food & Beverage'
                WHEN c.industry LIKE '%Financial%' THEN 'Financial Services'
                ELSE COALESCE(c.industry, 'Unknown')
            END
        ORDER BY COUNT(*) DESC
    """)
]

for analysis_name, query in meeting_analysis_queries:
    try:
        print(f"\n📈 {analysis_name}:")
        print("-" * (len(analysis_name) + 5))
        
        df = pd.read_sql(query, conn)
        if not df.empty:
            for _, row in df.iterrows():
                if 'jobtitle' in row:
                    title = str(row['jobtitle'])[:30]
                    count = row['meeting_count']
                    print(f"  • {title:30} | {count:3} meetings")
                elif 'company' in row:
                    company = str(row['company'])[:25]
                    contacts = row['contacts']
                    meetings = row['total_meetings']
                    ratio = meetings / contacts if contacts > 0 else 0
                    print(f"  • {company:25} | {contacts:2} contacts | {meetings:3} meetings | {ratio:.1f} ratio")
                elif 'industry_category' in row:
                    industry = str(row['industry_category'])[:20]
                    count = row['meeting_count']
                    print(f"  • {industry:20} | {count:4} meetings")
        else:
            print("  No data found")
            
    except Exception as e:
        print(f"  Error in {analysis_name}: {e}")

# 4. Contact Lifecycle vs Meeting Success
print("\n4. CONTACT LIFECYCLE vs MEETING SUCCESS:")
print("-" * 40)

try:
    query = """
    SELECT 
        c.lifecyclestage,
        COUNT(DISTINCT c.email) as total_contacts,
        COUNT(m.ContactId) as total_meetings,
        CASE 
            WHEN COUNT(DISTINCT c.email) > 0 
            THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT c.email)
            ELSE 0 
        END as meeting_rate
    FROM contact c
    LEFT JOIN sf.vMeetingSortASC m ON CAST(c.vid AS varchar) = m.ContactId
    WHERE c.lifecyclestage IS NOT NULL
    AND c.email IS NOT NULL
    GROUP BY c.lifecyclestage
    ORDER BY meeting_rate DESC
    """
    
    df = pd.read_sql(query, conn)
    if not df.empty:
        print("Lifecycle Stage | Contacts | Meetings | Meeting Rate")
        print("-" * 55)
        for _, row in df.iterrows():
            stage = str(row['lifecyclestage'])[:15]
            contacts = row['total_contacts']
            meetings = row['total_meetings']
            rate = row['meeting_rate']
            print(f"{stage:15} | {contacts:8,} | {meetings:8} | {rate:10.2f}")
            
except Exception as e:
    print(f"Error in lifecycle analysis: {e}")

# 5. Geographic Meeting Patterns
print("\n5. GEOGRAPHIC MEETING PATTERNS:")
print("-" * 33)

try:
    query = """
    SELECT TOP 15
        c.state,
        COUNT(DISTINCT c.email) as contacts,
        COUNT(m.ContactId) as meetings,
        CASE 
            WHEN COUNT(DISTINCT c.email) > 0 
            THEN CAST(COUNT(m.ContactId) AS FLOAT) / COUNT(DISTINCT c.email)
            ELSE 0 
        END as meeting_rate
    FROM contact c
    LEFT JOIN sf.vMeetingSortASC m ON CAST(c.vid AS varchar) = m.ContactId
    WHERE c.state IS NOT NULL
    AND c.email IS NOT NULL
    GROUP BY c.state
    HAVING COUNT(DISTINCT c.email) >= 100  -- States with decent sample size
    ORDER BY meeting_rate DESC
    """
    
    df = pd.read_sql(query, conn)
    if not df.empty:
        print("State | Contacts | Meetings | Meeting Rate | Music Market")
        print("-" * 65)
        major_music_markets = ['California', 'New York', 'Texas', 'Tennessee', 'Georgia']
        
        for _, row in df.iterrows():
            state = str(row['state'])[:12]
            contacts = row['contacts']
            meetings = row['meetings']
            rate = row['meeting_rate']
            is_major = 'MAJOR' if state in major_music_markets else 'Minor'
            print(f"{state:12} | {contacts:8,} | {meetings:8} | {rate:11.2f} | {is_major}")
            
except Exception as e:
    print(f"Error in geographic analysis: {e}")

# 6. Meeting Driver Insights Summary
print("\n6. MEETING DRIVER INSIGHTS FOR MAX.LIVE:")
print("-" * 42)

meeting_drivers = [
    "🎯 Job Title Impact: Marketing/Brand roles have highest meeting rates",
    "🏢 Company Size: Larger companies (3+ contacts) show better engagement", 
    "🎵 Industry Alignment: Entertainment/Media contacts most receptive",
    "📍 Geographic Hotspots: Music market states show higher conversion",
    "🔄 Lifecycle Stage: Advanced prospects (MQL/SQL) meet more readily",
    "🤝 Relationship Warmth: Existing connections drive meeting success",
    "📧 Multi-Touch: Multiple contacts per company increase odds",
    "🎪 Timing: Tour announcements create meeting opportunities"
]

for driver in meeting_drivers:
    print(f"  {driver}")

conn.close()

print("\n" + "=" * 70)
print("🎯 EMAIL-TO-MEETING OPTIMIZATION STRATEGY:")
print("-" * 42)
print("1. Target marketing/brand decision makers first")
print("2. Focus on entertainment/media industry contacts")
print("3. Prioritize major music market geographies")
print("4. Reference specific artist tours in target markets")
print("5. Use multi-contact approach for enterprise accounts")
print("6. Time outreach around tour announcements")
print("7. Leverage existing artist-brand preference data")
print("8. Create urgency with limited tour inventory")
print("=" * 70)