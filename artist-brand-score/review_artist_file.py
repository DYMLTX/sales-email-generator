#!/usr/bin/env python3
"""
Review ArtistsToMatch.xlsx file structure and content
"""

import pandas as pd
import os

def review_artist_file():
    """
    Review the structure and content of the artist file
    """
    file_path = '/home/davidyoung/Projects/FirstProject/artist-brand-score/ArtistsToMatch.xlsx'
    
    print("📁 REVIEWING ARTIST FILE: ArtistsToMatch.xlsx")
    print("=" * 45)
    
    # Check if file exists
    if not os.path.exists(file_path):
        print("❌ File not found!")
        return
    
    # Try to read the Excel file
    try:
        # Get sheet names
        excel_file = pd.ExcelFile(file_path)
        print(f"\nSheets found: {excel_file.sheet_names}")
        
        # Read first sheet (or specify sheet name if multiple)
        df = pd.read_excel(file_path, sheet_name=0)
        
        print(f"\nFile Structure:")
        print(f"- Rows: {len(df)}")
        print(f"- Columns: {len(df.columns)}")
        
        print(f"\nColumn Names:")
        for col in df.columns:
            print(f"  - {col}")
        
        print(f"\nData Types:")
        print(df.dtypes)
        
        print(f"\nFirst Few Rows:")
        print(df.head())
        
        # Check for expected demographic columns
        print(f"\n✅ Expected Demographic Columns Check:")
        expected_cols = ['age', 'gender', 'hhi', 'race', 'affinity', 'income']
        
        for exp in expected_cols:
            found = [col for col in df.columns if exp.lower() in col.lower()]
            if found:
                print(f"  - {exp.upper()}: Found as {found}")
            else:
                print(f"  - {exp.upper()}: Not found directly")
        
        # Check for null values
        print(f"\nNull Values Check:")
        print(df.isnull().sum())
        
        # Preview unique values in key columns
        print(f"\nSample Data from Each Column:")
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_count = df[col].nunique()
                print(f"\n{col}:")
                print(f"  Unique values: {unique_count}")
                if unique_count <= 10:
                    print(f"  Values: {df[col].unique()}")
                else:
                    print(f"  Sample: {df[col].head(3).tolist()}")
            else:
                print(f"\n{col}:")
                print(f"  Range: {df[col].min()} to {df[col].max()}")
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    print("\n🎯 FILE REVIEW COMPLETE")
    print("Ready to proceed with artist-brand matching analysis")

if __name__ == "__main__":
    review_artist_file()