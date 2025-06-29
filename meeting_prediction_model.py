#!/usr/bin/env python3
"""
Meeting Prediction Model for MAX.Live
Predicts likelihood of New Business meetings for prospect prioritization
"""

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
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

def create_training_dataset():
    """
    Create comprehensive training dataset with features and target variable
    """
    print("🔄 Creating training dataset...")
    
    query = """
    WITH ContactMeetings AS (
        -- Identify contacts with New Business meetings
        SELECT DISTINCT 
            c.Id as ContactId,
            1 as had_new_business_meeting
        FROM sf.Contact c
        INNER JOIN sf.vMeetingSortASC m ON c.Id = m.ContactId
        WHERE m.Type LIKE 'New Business%'
    ),
    
    BrandMetrics AS (
        -- Aggregate brand metrics by account
        SELECT 
            b.Account__c,
            COUNT(b.Id) as brand_count,
            AVG(b.WM_Brand_Media_Spend__c) as avg_media_spend,
            MAX(b.WM_Brand_Media_Spend__c) as max_media_spend,
            AVG(b.Social_Spend__c) as avg_social_spend,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%beer%' OR 
                             b.WM_Brand_Industries__c LIKE '%wine%' OR
                             b.WM_Brand_Industries__c LIKE '%liquor%' THEN 1 END) as beverage_brand_count,
            COUNT(CASE WHEN b.WM_Brand_Industries__c LIKE '%entertainment%' OR
                             b.WM_Brand_Industries__c LIKE '%media%' THEN 1 END) as entertainment_brand_count,
            COUNT(CASE WHEN b.Audience_Attributes__c IS NOT NULL THEN 1 END) as brands_with_audience_data
        FROM sf.Brands b
        WHERE b.Account__c IS NOT NULL
        GROUP BY b.Account__c
    ),
    
    AccountActivity AS (
        -- Account-level meeting activity (excluding target contact)
        SELECT 
            c.AccountId,
            COUNT(DISTINCT m.ContactId) as other_contacts_with_meetings,
            COUNT(m.ContactId) as total_account_meetings,
            MAX(m.ActivityDate) as last_account_meeting_date,
            COUNT(CASE WHEN m.Type LIKE 'New Business%' THEN 1 END) as account_new_business_meetings
        FROM sf.Contact c
        INNER JOIN sf.vMeetingSortASC m ON c.AccountId = m.AccountId
        GROUP BY c.AccountId
    )
    
    SELECT 
        -- Contact identifiers
        c.Id as ContactId,
        c.AccountId,
        a.Name as AccountName,
        
        -- Target variable
        COALESCE(cm.had_new_business_meeting, 0) as target_had_meeting,
        
        -- Contact-level features
        c.Title,
        CASE 
            WHEN c.Title LIKE '%Marketing%' OR c.Title LIKE '%Brand%' THEN 1 ELSE 0
        END as is_marketing_role,
        CASE 
            WHEN c.Title LIKE '%VP%' OR c.Title LIKE '%Vice President%' THEN 1 ELSE 0
        END as is_vp_level,
        CASE 
            WHEN c.Title LIKE '%Director%' THEN 1 ELSE 0
        END as is_director_level,
        CASE 
            WHEN c.Title LIKE '%Manager%' THEN 1 ELSE 0
        END as is_manager_level,
        CASE 
            WHEN c.Title LIKE '%Music%' THEN 1 ELSE 0
        END as is_music_focused,
        
        -- Account-level features
        COALESCE(bm.brand_count, 0) as account_brand_count,
        COALESCE(bm.avg_media_spend, 0) as account_avg_media_spend,
        COALESCE(bm.max_media_spend, 0) as account_max_media_spend,
        COALESCE(bm.avg_social_spend, 0) as account_avg_social_spend,
        COALESCE(bm.beverage_brand_count, 0) as account_beverage_brands,
        COALESCE(bm.entertainment_brand_count, 0) as account_entertainment_brands,
        COALESCE(bm.brands_with_audience_data, 0) as account_brands_with_audience,
        
        -- Account activity features
        COALESCE(aa.other_contacts_with_meetings, 0) as account_contacts_with_meetings,
        COALESCE(aa.total_account_meetings, 0) as account_total_meetings,
        COALESCE(aa.account_new_business_meetings, 0) as account_new_business_meetings,
        CASE 
            WHEN aa.last_account_meeting_date >= DATEADD(month, -6, GETDATE()) THEN 1 ELSE 0
        END as account_recent_activity,
        
        -- Derived features
        CASE 
            WHEN COALESCE(bm.avg_media_spend, 0) >= 1000000 THEN 'High'
            WHEN COALESCE(bm.avg_media_spend, 0) >= 500000 THEN 'Medium'
            WHEN COALESCE(bm.avg_media_spend, 0) >= 100000 THEN 'Low'
            ELSE 'Minimal'
        END as spend_tier,
        
        CASE 
            WHEN COALESCE(bm.beverage_brand_count, 0) > 0 THEN 1 ELSE 0
        END as has_beverage_brands,
        
        CASE 
            WHEN COALESCE(bm.brand_count, 0) >= 10 THEN 1 ELSE 0
        END as is_multi_brand_account
        
    FROM sf.Contact c
    INNER JOIN sf.Account a ON a.Id = c.AccountId
    LEFT JOIN ContactMeetings cm ON cm.ContactId = c.Id
    LEFT JOIN BrandMetrics bm ON bm.Account__c = c.AccountId
    LEFT JOIN AccountActivity aa ON aa.AccountId = c.AccountId
    WHERE c.Email IS NOT NULL
    AND c.Email != ''
    AND a.Name != 'Music Audience Exchange'  -- Exclude internal accounts
    """
    
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"✅ Training dataset created: {len(df):,} contacts")
    print(f"📊 Positive examples (had meetings): {df['target_had_meeting'].sum():,}")
    print(f"📊 Negative examples (no meetings): {(df['target_had_meeting'] == 0).sum():,}")
    print(f"📊 Meeting rate: {df['target_had_meeting'].mean():.1%}")
    
    return df

def engineer_features(df):
    """
    Engineer and prepare features for modeling
    """
    print("🔧 Engineering features...")
    
    # Create feature matrix
    feature_cols = [
        'is_marketing_role', 'is_vp_level', 'is_director_level', 'is_manager_level', 'is_music_focused',
        'account_brand_count', 'account_avg_media_spend', 'account_max_media_spend', 'account_avg_social_spend',
        'account_beverage_brands', 'account_entertainment_brands', 'account_brands_with_audience',
        'account_contacts_with_meetings', 'account_total_meetings', 'account_new_business_meetings',
        'account_recent_activity', 'has_beverage_brands', 'is_multi_brand_account'
    ]
    
    # Handle categorical features
    le_spend = LabelEncoder()
    df['spend_tier_encoded'] = le_spend.fit_transform(df['spend_tier'])
    feature_cols.append('spend_tier_encoded')
    
    # Create interaction features
    df['marketing_role_x_beverage'] = df['is_marketing_role'] * df['has_beverage_brands']
    df['director_level_x_high_spend'] = df['is_director_level'] * (df['account_avg_media_spend'] >= 1000000).astype(int)
    df['music_role_x_multi_brand'] = df['is_music_focused'] * df['is_multi_brand_account']
    
    feature_cols.extend(['marketing_role_x_beverage', 'director_level_x_high_spend', 'music_role_x_multi_brand'])
    
    # Prepare feature matrix
    X = df[feature_cols].fillna(0)
    y = df['target_had_meeting']
    
    print(f"✅ Feature engineering complete: {X.shape[1]} features")
    
    return X, y, feature_cols, df

def train_models(X, y, feature_cols):
    """
    Train and evaluate multiple models
    """
    print("🤖 Training prediction models...")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Models to test
    models = {
        'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
        'Gradient Boosting': GradientBoostingClassifier(n_estimators=100, random_state=42),
        'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000)
    }
    
    results = {}
    
    for name, model in models.items():
        print(f"\n📈 Training {name}...")
        
        # Use scaled data for Logistic Regression, raw data for tree-based models
        X_train_model = X_train_scaled if name == 'Logistic Regression' else X_train
        X_test_model = X_test_scaled if name == 'Logistic Regression' else X_test
        
        # Train model
        model.fit(X_train_model, y_train)
        
        # Predictions
        y_pred = model.predict(X_test_model)
        y_pred_proba = model.predict_proba(X_test_model)[:, 1]
        
        # Evaluate
        auc_score = roc_auc_score(y_test, y_pred_proba)
        cv_scores = cross_val_score(model, X_train_model, y_train, cv=5, scoring='roc_auc')
        
        results[name] = {
            'model': model,
            'auc_score': auc_score,
            'cv_mean': cv_scores.mean(),
            'cv_std': cv_scores.std(),
            'scaler': scaler if name == 'Logistic Regression' else None
        }
        
        print(f"  AUC Score: {auc_score:.3f}")
        print(f"  CV Score: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
        
        # Feature importance for tree-based models
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print(f"  Top 5 Features:")
            for _, row in feature_importance.head(5).iterrows():
                print(f"    {row['feature']}: {row['importance']:.3f}")
    
    # Select best model
    best_model_name = max(results.keys(), key=lambda k: results[k]['auc_score'])
    best_model_info = results[best_model_name]
    
    print(f"\n🏆 Best Model: {best_model_name} (AUC: {best_model_info['auc_score']:.3f})")
    
    return best_model_info, results

def score_prospects(df, X, model_info, feature_cols):
    """
    Score all prospects and create prioritized list
    """
    print("🎯 Scoring all prospects...")
    
    model = model_info['model']
    scaler = model_info.get('scaler')
    
    # Prepare features for scoring
    X_score = scaler.transform(X) if scaler else X
    
    # Generate predictions
    meeting_probability = model.predict_proba(X_score)[:, 1]
    meeting_score = (meeting_probability * 100).round(0).astype(int)
    
    # Add scores to dataframe
    df_scored = df.copy()
    df_scored['meeting_probability'] = meeting_probability
    df_scored['meeting_score'] = meeting_score
    
    # Create priority segments
    df_scored['priority_segment'] = pd.cut(
        df_scored['meeting_score'], 
        bins=[0, 50, 70, 85, 100], 
        labels=['Low', 'Medium', 'High', 'Very High'],
        include_lowest=True
    )
    
    return df_scored

def generate_prospect_insights(df_scored):
    """
    Generate actionable insights from scored prospects
    """
    print("\n📊 PROSPECT SCORING INSIGHTS:")
    print("-" * 40)
    
    # Overall distribution
    print("Score Distribution:")
    score_dist = df_scored['priority_segment'].value_counts().sort_index()
    for segment, count in score_dist.items():
        pct = count / len(df_scored) * 100
        print(f"  {segment:10}: {count:5,} contacts ({pct:4.1f}%)")
    
    # Top prospects
    print(f"\n🎯 TOP 20 PROSPECTS FOR MAX.LIVE:")
    print("-" * 42)
    
    top_prospects = df_scored.nlargest(20, 'meeting_score')[
        ['AccountName', 'Title', 'meeting_score', 'account_avg_media_spend', 
         'account_beverage_brands', 'is_marketing_role', 'is_music_focused']
    ]
    
    print("Account | Title | Score | Media Spend | Beverage | Marketing | Music")
    print("-" * 80)
    for _, row in top_prospects.iterrows():
        account = str(row['AccountName'])[:15]
        title = str(row['Title'])[:20] if pd.notna(row['Title']) else 'No Title'
        score = int(row['meeting_score'])
        spend = f"${row['account_avg_media_spend']:,.0f}" if row['account_avg_media_spend'] > 0 else "N/A"
        beverage = "✓" if row['account_beverage_brands'] > 0 else "✗"
        marketing = "✓" if row['is_marketing_role'] else "✗"
        music = "✓" if row['is_music_focused'] else "✗"
        
        print(f"{account:15} | {title:20} | {score:3} | {spend:10} | {beverage:8} | {marketing:9} | {music}")
    
    # Segment analysis
    print(f"\n📈 SEGMENT ANALYSIS:")
    print("-" * 20)
    
    for segment in ['Very High', 'High', 'Medium']:
        segment_data = df_scored[df_scored['priority_segment'] == segment]
        if len(segment_data) > 0:
            print(f"\n{segment} Priority Contacts ({len(segment_data):,}):")
            print(f"  Avg Media Spend: ${segment_data['account_avg_media_spend'].mean():,.0f}")
            print(f"  Beverage Brands: {(segment_data['account_beverage_brands'] > 0).mean():.1%}")
            print(f"  Marketing Roles: {segment_data['is_marketing_role'].mean():.1%}")
            print(f"  Music Focused: {segment_data['is_music_focused'].mean():.1%}")
            print(f"  Multi-Brand Accounts: {segment_data['is_multi_brand_account'].mean():.1%}")
    
    return top_prospects

def main():
    """
    Main execution function
    """
    print("🎯 MAX.LIVE MEETING PREDICTION MODEL")
    print("=" * 50)
    
    # Step 1: Create training dataset
    df = create_training_dataset()
    
    # Step 2: Engineer features
    X, y, feature_cols, df_processed = engineer_features(df)
    
    # Step 3: Train models
    best_model_info, all_results = train_models(X, y, feature_cols)
    
    # Step 4: Score all prospects
    df_scored = score_prospects(df_processed, X, best_model_info, feature_cols)
    
    # Step 5: Generate insights
    top_prospects = generate_prospect_insights(df_scored)
    
    # Step 6: Save results
    output_file = 'max_live_prospect_scores.csv'
    df_scored[['ContactId', 'AccountName', 'Title', 'meeting_score', 'priority_segment',
               'account_avg_media_spend', 'account_beverage_brands', 'is_marketing_role',
               'is_music_focused', 'account_brand_count']].to_csv(output_file, index=False)
    
    print(f"\n💾 Results saved to: {output_file}")
    print(f"📊 Model Performance: AUC = {best_model_info['auc_score']:.3f}")
    print("🎪 Ready for MAX.Live email campaign targeting!")

if __name__ == "__main__":
    main()