#!/usr/bin/env python3
"""
Ulterra File Mapping Verification Script
Checks the actual column names in the new Ulterra file against our configured mappings.
"""

import pandas as pd
from pathlib import Path
from data_mapping_config import DataMappingConfig
import glob

def analyze_ulterra_file():
    """Analyze the new Ulterra file structure and verify mappings"""
    print("🔍 ULTERRA FILE MAPPING VERIFICATION")
    print("=" * 50)
    
    # Get Ulterra configuration
    config = DataMappingConfig()
    ulterra_config = config.get_source_config('ulterra')
    
    if not ulterra_config:
        print("❌ Could not find Ulterra configuration")
        return
    
    print(f"📁 Looking in folder: {ulterra_config.folder_path}")
    print(f"📄 File pattern: {ulterra_config.file_pattern}")
    print(f"📋 Sheet name: {ulterra_config.sheet_name}")
    print(f"⏭️  Skip rows: {ulterra_config.skip_rows}")
    
    # Find Ulterra files
    ulterra_path = Path(ulterra_config.folder_path)
    if not ulterra_path.exists():
        print(f"❌ Ulterra folder not found: {ulterra_path}")
        return
    
    ulterra_files = list(ulterra_path.glob(ulterra_config.file_pattern))
    if not ulterra_files:
        print(f"❌ No Ulterra files found matching pattern: {ulterra_config.file_pattern}")
        return
    
    print(f"\n📂 Found {len(ulterra_files)} Ulterra file(s):")
    for file in ulterra_files:
        print(f"  • {file.name}")
    
    # Analyze the first file
    file_path = ulterra_files[0]
    print(f"\n🔬 Analyzing file: {file_path.name}")
    
    try:
        # First, let's see what sheets are available
        excel_file = pd.ExcelFile(file_path)
        print(f"\n📋 Available sheets:")
        for i, sheet in enumerate(excel_file.sheet_names):
            print(f"  {i+1}. {sheet}")
        
        # Try to read the configured sheet
        target_sheet = ulterra_config.sheet_name
        if target_sheet not in excel_file.sheet_names:
            print(f"\n❌ Target sheet '{target_sheet}' not found!")
            print("Available sheets:", excel_file.sheet_names)
            return
        
        print(f"\n📖 Reading sheet: '{target_sheet}'")
        
        # Read with no skip rows first to see raw structure
        print("\n🔍 RAW FILE STRUCTURE (first 5 rows):")
        raw_df = pd.read_excel(file_path, sheet_name=target_sheet, nrows=5)
        print("Columns:", list(raw_df.columns))
        print("\nFirst few rows:")
        print(raw_df.to_string(index=False))
        
        # Now read with configured skip_rows
        print(f"\n📊 READING WITH skip_rows={ulterra_config.skip_rows}:")
        df = pd.read_excel(file_path, 
                          sheet_name=target_sheet, 
                          skiprows=ulterra_config.skip_rows,
                          nrows=3)  # Just get a few rows for verification
        
        actual_columns = list(df.columns)
        print(f"\n✅ Found {len(actual_columns)} columns after skipping {ulterra_config.skip_rows} rows")
        print("\n📋 ACTUAL COLUMNS IN FILE:")
        for i, col in enumerate(actual_columns, 1):
            print(f"  {i:2d}. {col}")
        
        # Compare with configured mappings
        if not ulterra_config.column_mappings:
            print("❌ No column mappings configured!")
            return
            
        print(f"\n🔄 MAPPING VERIFICATION:")
        print("=" * 80)
        print(f"{'Standard Field':<25} {'Configured Mapping':<30} {'Status':<15} {'Actual Match'}")
        print("-" * 80)
        
        mapped_count = 0
        missing_count = 0
        
        for standard_field, source_column in ulterra_config.column_mappings.items():
            if source_column in actual_columns:
                status = "✅ FOUND"
                actual_match = source_column
                mapped_count += 1
            else:
                status = "❌ MISSING"
                # Try to find similar column names
                similar = [col for col in actual_columns if source_column.lower() in col.lower() or col.lower() in source_column.lower()]
                actual_match = f"Similar: {similar[:2]}" if similar else "None found"
                missing_count += 1
            
            print(f"{standard_field:<25} {source_column:<30} {status:<15} {actual_match}")
        
        print("-" * 80)
        print(f"📊 SUMMARY: {mapped_count} mapped, {missing_count} missing out of {len(ulterra_config.column_mappings)} total")
        
        # Show sample data for verification
        if mapped_count > 0 and ulterra_config.column_mappings:
            print(f"\n📋 SAMPLE DATA (first 3 rows):")
            print("=" * 50)
            
            # Show just the mapped columns that exist
            existing_columns = [col for col in ulterra_config.column_mappings.values() if col in actual_columns]
            sample_df = df[existing_columns[:10]]  # Show first 10 existing columns
            print(sample_df.to_string(index=False))
        
        # Check for columns that might be missing from our mapping
        if ulterra_config.column_mappings:
            unmapped_columns = [col for col in actual_columns if col not in ulterra_config.column_mappings.values()]
            if unmapped_columns:
                print(f"\n🔍 COLUMNS IN FILE NOT MAPPED ({len(unmapped_columns)}):")
                for i, col in enumerate(unmapped_columns, 1):
                    print(f"  {i:2d}. {col}")
    
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        import traceback
        traceback.print_exc()

def suggest_fixes():
    """Suggest potential fixes for mapping issues"""
    print(f"\n💡 SUGGESTED NEXT STEPS:")
    print("=" * 50)
    print("1. If many columns are missing, check if skip_rows needs adjustment")
    print("2. Look for similar column names in the 'unmapped columns' list")
    print("3. Update the data_mapping_config.py file with correct column names")
    print("4. Re-run the universal_data_integration.py script after fixes")

if __name__ == "__main__":
    analyze_ulterra_file()
    suggest_fixes()
