#!/usr/bin/env python3
"""
Artist-Brand Matching Analysis
Matches artists to brands based on audience demographic similarity
"""

import os
import sys
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from scipy.spatial.distance import cosine
from scipy.stats import chi2_contingency, ttest_ind
import warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

class ArtistBrandMatcher:
    def __init__(self):
        self.artist_data = None
        self.brand_data = None
        self.match_results = None
        self.industry_attribute_map = self.create_industry_attribute_map()
        
    def get_connection(self):
        """Get database connection"""
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
    
    def create_industry_attribute_map(self):
        """Create mapping between consumer attributes and brand industries"""
        return {
            # Direct mappings
            'hard seltzer': ['beer, wine, liquor', 'alcoholic beverages', 'beverage'],
            'coffee houses': ['restaurants', 'coffee', 'qsr', 'starbucks', 'dunkin'],
            'dog owners': ['pet supplies', 'pet food', 'pet care'],
            'movie goers': ['entertainment', 'streaming', 'media', 'theaters'],
            'travelers': ['airlines', 'hotels', 'travel', 'hospitality'],
            'vapers': ['tobacco', 'vaping', 'nicotine'],
            'tea drinkers': ['beverages', 'tea', 'non-alcoholic'],
            
            # Lifestyle mappings
            'moms': ['packaged foods', 'household', 'retail', 'grocery'],
            'dads': ['automotive', 'tools', 'sports', 'beer'],
            'married': ['insurance', 'financial', 'home improvement'],
            'quality conscious': ['premium', 'luxury', 'high-end'],
            'budget conscious': ['discount', 'value', 'walmart', 'dollar'],
            
            # Activity mappings
            'horror tv viewers': ['streaming', 'entertainment'],
            'reality tv viewers': ['entertainment', 'media'],
            'podcast listeners': ['media', 'audio', 'streaming'],
            'streamers': ['streaming', 'entertainment', 'gaming'],
            
            # Product category mappings
            'cosmetics': ['cosmetics', 'beauty', 'personal care'],
            'snack food': ['packaged foods', 'snacks', 'chips'],
            'fast casual': ['restaurants', 'qsr', 'fast food'],
            'hybrid drivers': ['automotive', 'eco-friendly', 'sustainable'],
            'suv drivers': ['automotive', 'trucks', 'vehicles']
        }
    
    def parse_artist_audience(self, audience_text):
        """Parse the structured audience text into usable data"""
        parsed = {
            'gender': {},
            'ethnicity': {},
            'income': {},
            'age': {},
            'attributes': {}
        }
        
        lines = audience_text.split('\n')
        current_section = None
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Identify sections
            if line == 'Gender':
                current_section = 'gender'
            elif line == 'Ethnicity':
                current_section = 'ethnicity'
            elif line == 'Household Income':
                current_section = 'income'
            elif line == 'Age':
                current_section = 'age'
            elif line == 'Consumer Attributes':
                current_section = 'attributes'
            elif line and current_section:
                # Parse data based on section
                if current_section in ['gender', 'ethnicity', 'income', 'age']:
                    # Look for percentage pattern
                    if i + 1 < len(lines) and '%' in lines[i + 1]:
                        value = lines[i + 1].strip().replace('%', '')
                        parsed[current_section][line] = float(value) / 100
                    # Look for index pattern (+ or -)
                    elif i + 1 < len(lines) and (lines[i + 1].startswith('+') or lines[i + 1].startswith('-')):
                        index_str = lines[i + 1].strip().replace(' ', '')
                        index_value = float(index_str)
                        parsed[current_section][line] = index_value
                elif current_section == 'attributes':
                    # Parse consumer attributes with their indices
                    if i + 1 < len(lines) and (lines[i + 1].startswith('+') or lines[i + 1].startswith('-')):
                        index_str = lines[i + 1].strip().replace(' ', '')
                        index_value = float(index_str)
                        parsed[current_section][line.lower()] = index_value
        
        return parsed
    
    def load_artist_data(self):
        """Load and parse artist data from Excel"""
        print("📊 Loading Artist Data...")
        
        file_path = '/home/davidyoung/Projects/FirstProject/artist-brand-score/ArtistsToMatch.xlsx'
        df = pd.read_excel(file_path)
        
        # Parse audience data for each artist
        artist_list = []
        for _, row in df.iterrows():
            artist_info = {
                'name': row['Artist'],
                'genre': row['Genre'],
                'audience_parsed': self.parse_artist_audience(row['Audience']),
                'background': row.get('Background', ''),
                'website': row.get('Website', '')
            }
            artist_list.append(artist_info)
        
        self.artist_data = artist_list
        print(f"✅ Loaded {len(artist_list)} artists")
        
        # Display parsed data sample
        print("\nSample parsed data for", artist_list[0]['name'])
        print("Gender:", artist_list[0]['audience_parsed']['gender'])
        print("Top Attributes:", dict(list(artist_list[0]['audience_parsed']['attributes'].items())[:5]))
    
    def load_brand_data(self):
        """Load high-spend brands from database"""
        print("\n📊 Loading Brand Data...")
        
        conn = self.get_connection()
        
        query = """
        SELECT 
            Id,
            Name,
            Account__c,
            WM_Brand_Media_Spend__c as MediaSpend,
            WM_Brand_Industries__c as Industries,
            WM_Brand_Description__c as Description,
            Audience_Description__c as AudienceDescription,
            Audience_Attributes__c as AudienceAttributes
        FROM sf.Brands
        WHERE WM_Brand_Media_Spend__c > 5000000
        AND Audience_Attributes__c IS NOT NULL
        ORDER BY WM_Brand_Media_Spend__c DESC
        """
        
        self.brand_data = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Loaded {len(self.brand_data)} brands with >$5M spend")
        
        # Parse JSON audience attributes
        self.brand_data['AudienceParsed'] = self.brand_data['AudienceAttributes'].apply(self.parse_brand_audience)
    
    def parse_brand_audience(self, audience_json):
        """Parse brand audience attributes JSON"""
        try:
            # Handle various JSON formats
            if pd.isna(audience_json) or audience_json == "{'': ''}":
                return {}
            
            # Clean and parse JSON
            audience_json = audience_json.replace("'", '"')
            return json.loads(audience_json)
        except:
            return {}
    
    def calculate_demographic_similarity(self, artist_demo, brand_demo):
        """Calculate similarity between artist and brand demographics"""
        scores = {}
        
        # Age similarity
        artist_age = artist_demo.get('age', {})
        brand_age = brand_demo.get('Age', '')
        scores['age'] = self.calculate_age_similarity(artist_age, brand_age)
        
        # Gender similarity
        artist_gender = artist_demo.get('gender', {})
        brand_gender = brand_demo.get('Gender', 'All Genders')
        scores['gender'] = self.calculate_gender_similarity(artist_gender, brand_gender)
        
        # Income similarity
        artist_income = artist_demo.get('income', {})
        brand_income = brand_demo.get('Household Income', '')
        scores['income'] = self.calculate_income_similarity(artist_income, brand_income)
        
        # Ethnicity similarity
        artist_ethnicity = artist_demo.get('ethnicity', {})
        brand_ethnicity = brand_demo.get('Ethnicity', '')
        scores['ethnicity'] = self.calculate_ethnicity_similarity(artist_ethnicity, brand_ethnicity)
        
        return scores
    
    def calculate_age_similarity(self, artist_age, brand_age):
        """Calculate age range overlap"""
        # Map age ranges to numeric values for comparison
        age_map = {
            '16-20': (16, 20),
            '21-29': (21, 29),
            '28-43': (28, 43),
            '30-39': (30, 39),
            '40-49': (40, 49),
            '44-59': (44, 59),
            '50-59': (50, 59),
            '60+': (60, 75)
        }
        
        # Get brand age range
        brand_range = None
        for age_str, age_tuple in age_map.items():
            if age_str in str(brand_age):
                brand_range = age_tuple
                break
        
        if not brand_range:
            return 0.5  # Default neutral score
        
        # Calculate weighted overlap with artist age distribution
        total_score = 0
        total_weight = 0
        
        for artist_age_group, index in artist_age.items():
            if 'Years Old' in artist_age_group:
                # Extract age range from artist data
                for age_str, age_tuple in age_map.items():
                    if any(str(x) in artist_age_group for x in range(age_tuple[0], age_tuple[1]+1)):
                        # Calculate overlap
                        overlap = max(0, min(age_tuple[1], brand_range[1]) - max(age_tuple[0], brand_range[0]))
                        max_overlap = min(age_tuple[1] - age_tuple[0], brand_range[1] - brand_range[0])
                        
                        if max_overlap > 0:
                            overlap_score = overlap / max_overlap
                            # Weight by artist index (positive index = more important)
                            weight = 1 + max(0, index)
                            total_score += overlap_score * weight
                            total_weight += weight
                        break
        
        return total_score / total_weight if total_weight > 0 else 0.5
    
    def calculate_gender_similarity(self, artist_gender, brand_gender):
        """Calculate gender distribution similarity"""
        if brand_gender == 'All Genders' or not artist_gender:
            return 0.75  # Neutral-positive score for all genders
        
        # Calculate based on distribution match
        female_pct = artist_gender.get('Female', 0)
        male_pct = artist_gender.get('Male', 0)
        
        # Simple similarity based on skew
        if female_pct > 0.6 and 'Female' in brand_gender:
            return 0.9
        elif male_pct > 0.6 and 'Male' in brand_gender:
            return 0.9
        elif abs(female_pct - 0.5) < 0.1:  # Balanced audience
            return 0.8
        else:
            return 0.6
    
    def calculate_income_similarity(self, artist_income, brand_income):
        """Calculate income bracket similarity"""
        income_brackets = {
            'Less than $30K': 1,
            '$25,000 - $50,000': 2,
            '$30K-$49K': 2,
            '$50K-$74K': 3,
            '$50,000 - $100,000': 3.5,
            '$75K-$125k': 4,
            '$100,000 - $150,000': 5,
            '$125K or More': 6,
            '$150,000+': 6
        }
        
        # Get brand income level
        brand_level = 3.5  # Default middle
        for bracket, level in income_brackets.items():
            if bracket in str(brand_income):
                brand_level = level
                break
        
        # Calculate weighted average artist income level
        artist_level = 0
        total_weight = 0
        
        for bracket, index in artist_income.items():
            if bracket in income_brackets:
                level = income_brackets[bracket]
                weight = 1 + max(0, index)  # Higher weight for positive indices
                artist_level += level * weight
                total_weight += weight
        
        if total_weight > 0:
            artist_level /= total_weight
        else:
            artist_level = 3.5  # Default
        
        # Calculate similarity (inverse of distance)
        distance = abs(artist_level - brand_level)
        similarity = 1 - (distance / 5)  # Max distance is 5
        
        return max(0, min(1, similarity))
    
    def calculate_ethnicity_similarity(self, artist_ethnicity, brand_ethnicity):
        """Calculate ethnicity alignment"""
        if not artist_ethnicity or not brand_ethnicity:
            return 0.7  # Neutral score
        
        # Simple matching for now
        score = 0.5  # Base score
        
        # Check primary ethnicity match
        if 'White' in str(brand_ethnicity) and artist_ethnicity.get('White', 0) > 0:
            score += 0.3 * (1 + artist_ethnicity.get('White', 0))
        elif 'Hispanic' in str(brand_ethnicity) and artist_ethnicity.get('Hispanic', 0) > 0:
            score += 0.3 * (1 + artist_ethnicity.get('Hispanic', 0))
        elif 'African American' in str(brand_ethnicity) and artist_ethnicity.get('African American', 0) > 0:
            score += 0.3 * (1 + artist_ethnicity.get('African American', 0))
        elif 'Asian' in str(brand_ethnicity) and artist_ethnicity.get('Asian', 0) > 0:
            score += 0.3 * (1 + artist_ethnicity.get('Asian', 0))
        
        return min(1, score)
    
    def calculate_attribute_affinity(self, artist_attrs, brand_industries):
        """Calculate affinity based on consumer attributes and brand industry"""
        if not artist_attrs or not brand_industries:
            return 0, 0
        
        brand_industries_lower = str(brand_industries).lower()
        matches = []
        total_score = 0
        
        # Check each artist attribute against industry mappings
        for attr, index in artist_attrs.items():
            attr_lower = attr.lower()
            
            # Direct industry matching
            for attr_key, industry_list in self.industry_attribute_map.items():
                if attr_key in attr_lower:
                    for industry in industry_list:
                        if industry in brand_industries_lower:
                            # Score based on index strength
                            match_score = (1 + index) / 3 if index > 0 else 0.3
                            matches.append(attr)
                            total_score += match_score
                            break
        
        # Normalize score
        if matches:
            avg_score = total_score / len(artist_attrs)
        else:
            avg_score = 0.2  # Small base score
        
        return min(1, avg_score), len(matches)
    
    def calculate_match_score(self, artist, brand):
        """Calculate comprehensive match score between artist and brand"""
        # Get parsed audiences
        artist_demo = artist['audience_parsed']
        brand_demo = brand['AudienceParsed']
        
        # Handle cases where brand_demo might be a list or empty
        if isinstance(brand_demo, list):
            brand_demo = brand_demo[0] if brand_demo else {}
        elif not isinstance(brand_demo, dict):
            brand_demo = {}
        
        # Calculate demographic similarities
        demo_scores = self.calculate_demographic_similarity(artist_demo, brand_demo)
        
        # Calculate attribute affinity
        attr_score, attr_matches = self.calculate_attribute_affinity(
            artist_demo.get('attributes', {}),
            brand['Industries']
        )
        
        # Geographic bonus (simplified for now)
        geo_score = 0.7  # Default neutral
        if brand_demo.get('Region'):
            geo_score = 0.8
        
        # Calculate weighted composite score
        weights = {
            'age': 0.25,
            'gender': 0.15,
            'income': 0.20,
            'ethnicity': 0.10,
            'attributes': 0.20,
            'geographic': 0.10
        }
        
        composite_score = (
            demo_scores['age'] * weights['age'] +
            demo_scores['gender'] * weights['gender'] +
            demo_scores['income'] * weights['income'] +
            demo_scores['ethnicity'] * weights['ethnicity'] +
            attr_score * weights['attributes'] +
            geo_score * weights['geographic']
        )
        
        # Scale to 0-100
        final_score = composite_score * 100
        
        # Calculate confidence interval (simplified)
        std_dev = np.std(list(demo_scores.values()) + [attr_score, geo_score]) * 10
        ci_lower = max(0, final_score - 1.96 * std_dev)
        ci_upper = min(100, final_score + 1.96 * std_dev)
        
        return {
            'composite_score': final_score,
            'demographic_score': np.mean(list(demo_scores.values())) * 100,
            'attribute_score': attr_score * 100,
            'geographic_score': geo_score * 100,
            'age_similarity': demo_scores['age'],
            'gender_similarity': demo_scores['gender'],
            'income_similarity': demo_scores['income'],
            'ethnicity_similarity': demo_scores['ethnicity'],
            'attribute_matches': attr_matches,
            'ci_lower': ci_lower,
            'ci_upper': ci_upper,
            'std_dev': std_dev
        }
    
    def run_matching(self):
        """Run the complete matching process"""
        print("\n🎯 Running Artist-Brand Matching...")
        
        results = []
        total_matches = len(self.artist_data) * len(self.brand_data)
        processed = 0
        
        for artist in self.artist_data:
            artist_results = []
            
            for _, brand in self.brand_data.iterrows():
                # Calculate match score
                scores = self.calculate_match_score(artist, brand)
                
                # Create result record
                result = {
                    'Artist_Name': artist['name'],
                    'Brand_Name': brand['Name'],
                    'Brand_Industry': brand['Industries'],
                    'Brand_MediaSpend': brand['MediaSpend'],
                    'Composite_Match_Score': scores['composite_score'],
                    'Demographic_Score': scores['demographic_score'],
                    'Attribute_Score': scores['attribute_score'],
                    'Geographic_Score': scores['geographic_score'],
                    'Age_Similarity': scores['age_similarity'],
                    'Gender_Similarity': scores['gender_similarity'],
                    'Income_Similarity': scores['income_similarity'],
                    'Ethnicity_Similarity': scores['ethnicity_similarity'],
                    'Consumer_Attribute_Matches': scores['attribute_matches'],
                    'Confidence_Interval_Lower': scores['ci_lower'],
                    'Confidence_Interval_Upper': scores['ci_upper'],
                    'Score_StdDev': scores['std_dev']
                }
                
                artist_results.append(result)
                processed += 1
                
                if processed % 500 == 0:
                    print(f"  Processed {processed}/{total_matches} matches...")
            
            # Sort by composite score and add rank
            artist_results_df = pd.DataFrame(artist_results)
            artist_results_df = artist_results_df.sort_values('Composite_Match_Score', ascending=False)
            artist_results_df['Rank_Within_Artist'] = range(1, len(artist_results_df) + 1)
            
            results.append(artist_results_df)
        
        # Combine all results
        self.match_results = pd.concat(results, ignore_index=True)
        
        # Add match quality tiers
        self.add_match_tiers()
        
        print(f"✅ Completed {len(self.match_results)} matches")
    
    def add_match_tiers(self):
        """Add statistical match quality tiers"""
        # Calculate distribution statistics
        mean_score = self.match_results['Composite_Match_Score'].mean()
        std_score = self.match_results['Composite_Match_Score'].std()
        
        # Define tiers based on standard deviations
        def assign_tier(score):
            if score > mean_score + 2 * std_score:
                return 'Exceptional'
            elif score > mean_score + std_score:
                return 'Strong'
            elif score > mean_score:
                return 'Good'
            else:
                return 'Fair'
        
        self.match_results['Match_Tier'] = self.match_results['Composite_Match_Score'].apply(assign_tier)
        
        # Add p-value approximation (simplified)
        from scipy import stats
        z_scores = (self.match_results['Composite_Match_Score'] - mean_score) / std_score
        self.match_results['Match_P_Value'] = stats.norm.sf(abs(z_scores)) * 2
    
    def generate_statistics(self):
        """Generate model validation statistics"""
        print("\n📊 Model Statistics:")
        print("-" * 40)
        
        # Overall statistics
        print(f"Total Matches: {len(self.match_results):,}")
        print(f"Mean Match Score: {self.match_results['Composite_Match_Score'].mean():.2f}")
        print(f"Std Dev: {self.match_results['Composite_Match_Score'].std():.2f}")
        
        # Tier distribution
        print("\nMatch Quality Distribution:")
        tier_counts = self.match_results['Match_Tier'].value_counts()
        for tier, count in tier_counts.items():
            pct = count / len(self.match_results) * 100
            print(f"  {tier}: {count:,} ({pct:.1f}%)")
        
        # Top matches per artist
        print("\nTop Match per Artist:")
        for artist in self.match_results['Artist_Name'].unique():
            top_match = self.match_results[self.match_results['Artist_Name'] == artist].iloc[0]
            print(f"  {artist}: {top_match['Brand_Name']} (Score: {top_match['Composite_Match_Score']:.1f})")
        
        # Component correlation
        components = ['Demographic_Score', 'Attribute_Score', 'Geographic_Score']
        corr_matrix = self.match_results[components + ['Composite_Match_Score']].corr()
        
        print("\nComponent Correlations with Composite Score:")
        for comp in components:
            print(f"  {comp}: {corr_matrix.loc[comp, 'Composite_Match_Score']:.3f}")
    
    def export_results(self):
        """Export results to Excel with multiple sheets"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_file = f'/home/davidyoung/Projects/FirstProject/artist-brand-score/artist_brand_matches_{timestamp}.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Full results
            self.match_results.to_excel(writer, sheet_name='All_Matches', index=False)
            
            # Top 50 per artist
            top_matches = []
            for artist in self.match_results['Artist_Name'].unique():
                artist_top = self.match_results[
                    self.match_results['Artist_Name'] == artist
                ].head(50)
                top_matches.append(artist_top)
            
            pd.concat(top_matches).to_excel(writer, sheet_name='Top_50_Per_Artist', index=False)
            
            # Summary statistics
            summary_stats = pd.DataFrame({
                'Metric': ['Total Matches', 'Mean Score', 'Std Dev', 'Max Score', 'Min Score'],
                'Value': [
                    len(self.match_results),
                    self.match_results['Composite_Match_Score'].mean(),
                    self.match_results['Composite_Match_Score'].std(),
                    self.match_results['Composite_Match_Score'].max(),
                    self.match_results['Composite_Match_Score'].min()
                ]
            })
            summary_stats.to_excel(writer, sheet_name='Summary_Stats', index=False)
            
            # Data dictionary
            data_dict = pd.DataFrame({
                'Column': ['Artist_Name', 'Brand_Name', 'Composite_Match_Score', 'Demographic_Score',
                          'Attribute_Score', 'Age_Similarity', 'Gender_Similarity', 'Income_Similarity',
                          'Consumer_Attribute_Matches', 'Match_Tier', 'Match_P_Value'],
                'Description': [
                    'Name of the artist',
                    'Name of the brand',
                    'Overall match score (0-100)',
                    'Demographic alignment score (0-100)',
                    'Consumer attribute affinity score (0-100)',
                    'Age range overlap (0-1)',
                    'Gender distribution similarity (0-1)',
                    'Income bracket alignment (0-1)',
                    'Count of matching consumer attributes',
                    'Quality tier based on statistical distribution',
                    'P-value for match significance'
                ]
            })
            data_dict.to_excel(writer, sheet_name='Data_Dictionary', index=False)
        
        print(f"\n✅ Results exported to: {output_file}")
        
        # Also export CSV for easier analysis
        csv_file = output_file.replace('.xlsx', '.csv')
        self.match_results.to_csv(csv_file, index=False)
        print(f"✅ CSV version saved to: {csv_file}")
    
    def run(self):
        """Execute the complete matching pipeline"""
        print("🎪 ARTIST-BRAND MATCHING ENGINE")
        print("=" * 40)
        
        # Load data
        self.load_artist_data()
        self.load_brand_data()
        
        # Run matching
        self.run_matching()
        
        # Generate statistics
        self.generate_statistics()
        
        # Export results
        self.export_results()
        
        print("\n🎯 MATCHING COMPLETE!")
        print("=" * 20)

def main():
    matcher = ArtistBrandMatcher()
    matcher.run()

if __name__ == "__main__":
    main()