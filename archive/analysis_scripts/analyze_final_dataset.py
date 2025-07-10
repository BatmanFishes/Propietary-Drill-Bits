"""
Quick analysis of the integrated dataset with dual UWI fields
"""

import pandas as pd
from pathlib import Path

def analyze_integrated_dataset():
    """Analyze the final integrated dataset"""
    
    # Find the most recent dual UWI file
    output_dir = Path("Output")
    dual_uwi_files = list(output_dir.glob("Integrated_BitData_DualUWI_*.xlsx*"))
    
    if not dual_uwi_files:
        print("❌ No dual UWI integrated file found!")
        return
    
    # Use the most recent file
    latest_file = max(dual_uwi_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 Analyzing: {latest_file.name}")
    
    try:
        # Read the integrated dataset
        df = pd.read_excel(latest_file)
        
        print(f"\n📋 Dataset Overview:")
        print(f"   Total records: {len(df):,}")
        print(f"   Total columns: {len(df.columns)}")
        
        # Data source breakdown
        if 'data_source' in df.columns:
            source_counts = df['data_source'].value_counts()
            print(f"\n🏭 Data Sources:")
            for source, count in source_counts.items():
                print(f"   {source}: {count:,} records")
        
        # Unique wells analysis
        well_identifiers = []
        
        # Check well name
        if 'well_name' in df.columns:
            unique_well_names = df['well_name'].dropna().nunique()
            well_identifiers.append(('well_name', unique_well_names))
            print(f"\n🏭 Unique Wells (by well_name): {unique_well_names:,}")
        
        # Check UWI numbers
        if 'uwi_number' in df.columns:
            unique_uwi_unformatted = df['uwi_number'].dropna().nunique()
            valid_uwi_unformatted = df['uwi_number'].notna().sum()
            well_identifiers.append(('uwi_number', unique_uwi_unformatted))
            print(f"🔢 Unique UWIs (unformatted): {unique_uwi_unformatted:,} ({valid_uwi_unformatted:,} valid)")
        
        if 'uwi_formatted' in df.columns:
            unique_uwi_formatted = df['uwi_formatted'].dropna().nunique()
            valid_uwi_formatted = df['uwi_formatted'].notna().sum()
            well_identifiers.append(('uwi_formatted', unique_uwi_formatted))
            print(f"📝 Unique UWIs (formatted): {unique_uwi_formatted:,} ({valid_uwi_formatted:,} valid)")
        
        # Check license numbers
        if 'license_number' in df.columns:
            unique_licenses = df['license_number'].dropna().nunique()
            valid_licenses = df['license_number'].notna().sum()
            well_identifiers.append(('license_number', unique_licenses))
            print(f"📋 Unique License Numbers: {unique_licenses:,} ({valid_licenses:,} valid)")
        
        # Show the most reliable well count
        if well_identifiers:
            best_identifier = max(well_identifiers, key=lambda x: x[1])
            print(f"\n🎯 Most Comprehensive Well Count: {best_identifier[1]:,} wells (using {best_identifier[0]})")
        
        # Operator analysis
        if 'operator' in df.columns:
            unique_operators = df['operator'].dropna().nunique()
            print(f"🏢 Unique Operators: {unique_operators:,}")
            
            # Top operators
            top_operators = df['operator'].value_counts().head(5)
            print(f"\n📊 Top 5 Operators:")
            for operator, count in top_operators.items():
                print(f"   {operator}: {count:,} records")
        
        # Date range analysis
        date_fields = ['spud_date', 'run_date', 'td_date']
        for date_field in date_fields:
            if date_field in df.columns:
                df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
                date_range = df[date_field].dropna()
                if len(date_range) > 0:
                    print(f"\n📅 {date_field.title()}:")
                    print(f"   Range: {date_range.min().strftime('%Y-%m-%d')} to {date_range.max().strftime('%Y-%m-%d')}")
                    print(f"   Records with dates: {len(date_range):,}")
        
        # Bit size analysis
        if 'bit_size_mm' in df.columns:
            bit_sizes = df['bit_size_mm'].dropna()
            if len(bit_sizes) > 0:
                print(f"\n🔧 Bit Sizes:")
                print(f"   Range: {bit_sizes.min():.0f}mm to {bit_sizes.max():.0f}mm")
                print(f"   Average: {bit_sizes.mean():.1f}mm")
                
                # Common bit sizes
                common_sizes = bit_sizes.value_counts().head(5)
                print(f"   Most common sizes:")
                for size, count in common_sizes.items():
                    print(f"     {size:.0f}mm: {count:,} records")
        
        # Show sample records with both UWI fields
        if 'uwi_number' in df.columns and 'uwi_formatted' in df.columns:
            sample_with_uwi = df[
                df['uwi_number'].notna() & 
                df['uwi_formatted'].notna()
            ][['well_name', 'uwi_number', 'uwi_formatted', 'license_number', 'operator']].head(3)
            
            if not sample_with_uwi.empty:
                print(f"\n📋 Sample Records with Dual UWI Fields:")
                for idx, row in sample_with_uwi.iterrows():
                    print(f"   Well: {row.get('well_name', 'N/A')}")
                    print(f"   UWI (unformatted): {row.get('uwi_number', 'N/A')}")
                    print(f"   UWI (formatted): {row.get('uwi_formatted', 'N/A')}")
                    print(f"   License: {row.get('license_number', 'N/A')}")
                    print(f"   Operator: {row.get('operator', 'N/A')}")
                    print()
        
    except Exception as e:
        print(f"❌ Error analyzing dataset: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_integrated_dataset()
