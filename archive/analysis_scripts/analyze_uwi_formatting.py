#!/usr/bin/env python3
"""
Analyze UWI formatting in the integrated dataset
"""

import pandas as pd
import re
from pathlib import Path

def analyze_uwi_formatting():
    """Analyze UWI formatting quality and identify unformatted values"""
    
    # Load the most recent integrated file
    output_dir = Path("core/Output")
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    
    if not integrated_files:
        print("❌ No integrated data files found")
        return
    
    # Use the most recent file
    latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
    print(f"📂 Loading data from: {latest_file.name}")
    
    df = pd.read_excel(latest_file)
    
    print(f"\n📊 UWI Formatting Analysis")
    print("=" * 50)
    print(f"Total records: {len(df):,}")
    
    # Check UWI column presence
    uwi_cols = [col for col in df.columns if 'uwi' in col.lower()]
    print(f"UWI columns found: {uwi_cols}")
    
    if 'uwi_number' in df.columns:
        uwi_unformatted_count = df['uwi_number'].notna().sum()
        print(f"Records with uwi_number (unformatted): {uwi_unformatted_count:,}")
    
    if 'uwi_formatted' in df.columns:
        uwi_formatted_count = df['uwi_formatted'].notna().sum()
        print(f"Records with uwi_formatted: {uwi_formatted_count:,}")
        
        # Analyze formatting patterns in uwi_formatted
        print(f"\n🔍 UWI Formatted Analysis:")
        formatted_uwis = df[df['uwi_formatted'].notna()]['uwi_formatted']
        
        # Check for properly formatted UWIs (with slashes/dashes)
        properly_formatted = formatted_uwis[formatted_uwis.str.contains(r'[/-]', na=False)]
        unformatted_in_formatted = formatted_uwis[~formatted_uwis.str.contains(r'[/-]', na=False)]
        
        print(f"Properly formatted UWIs (with / or -): {len(properly_formatted):,}")
        print(f"Unformatted UWIs in uwi_formatted column: {len(unformatted_in_formatted):,}")
        
        if len(unformatted_in_formatted) > 0:
            print(f"\n⚠️  UNFORMATTED UWIs FOUND IN uwi_formatted COLUMN:")
            print(f"Count: {len(unformatted_in_formatted):,}")
            print(f"Percentage: {len(unformatted_in_formatted) / len(formatted_uwis) * 100:.1f}%")
            
            # Show samples by data source
            print(f"\nSample unformatted UWIs by source:")
            for source in df['data_source'].unique():
                source_unformatted = df[
                    (df['data_source'] == source) & 
                    (df['uwi_formatted'].notna()) & 
                    (~df['uwi_formatted'].str.contains(r'[/-]', na=False))
                ]['uwi_formatted']
                
                if len(source_unformatted) > 0:
                    print(f"\n{source.upper()} ({len(source_unformatted):,} unformatted):")
                    for i, uwi in enumerate(source_unformatted.head(5)):
                        print(f"  {i+1}. {uwi}")
        else:
            print(f"✅ All UWIs in uwi_formatted column are properly formatted!")
        
        # Show samples of properly formatted UWIs
        if len(properly_formatted) > 0:
            print(f"\n✅ Sample properly formatted UWIs:")
            for i, uwi in enumerate(properly_formatted.head(5)):
                print(f"  {i+1}. {uwi}")
    
    # Overall UWI coverage
    if 'uwi_number' in df.columns and 'uwi_formatted' in df.columns:
        has_any_uwi = (df['uwi_number'].notna()) | (df['uwi_formatted'].notna())
        total_with_uwi = has_any_uwi.sum()
        uwi_coverage = total_with_uwi / len(df) * 100
        
        print(f"\n📈 Overall UWI Coverage:")
        print(f"Records with any UWI: {total_with_uwi:,} ({uwi_coverage:.1f}%)")
        print(f"Records missing UWI: {len(df) - total_with_uwi:,} ({100 - uwi_coverage:.1f}%)")
        
        # By source
        print(f"\nUWI Coverage by Source:")
        for source in df['data_source'].unique():
            source_df = df[df['data_source'] == source]
            source_has_uwi = (
                source_df['uwi_number'].notna() | 
                source_df['uwi_formatted'].notna()
            ).sum()
            source_coverage = source_has_uwi / len(source_df) * 100
            
            print(f"  {source}: {source_has_uwi:,}/{len(source_df):,} ({source_coverage:.1f}%)")

if __name__ == "__main__":
    analyze_uwi_formatting()
