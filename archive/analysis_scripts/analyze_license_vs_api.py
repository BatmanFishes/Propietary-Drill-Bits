"""
Analyze License vs API/UWI Numbers
Check if these are the same identifiers or different systems
"""

import pandas as pd
from pathlib import Path

def analyze_license_vs_api():
    """Compare license numbers and API/UWI numbers to see if they're the same"""
    
    print("🔍 Analyzing License Numbers vs API/UWI Numbers")
    print("=" * 60)
    
    # Load Reed data (has both fields)
    reed_file = Path("Input/Reed/All+Montney+since+2020.xlsx")
    
    if not reed_file.exists():
        print(f"❌ Reed file not found: {reed_file}")
        return
    
    # Read Reed data
    df = pd.read_excel(reed_file, sheet_name="All+Montney+since+2020")
    
    print(f"📊 Reed Data Analysis:")
    print(f"   Total records: {len(df):,}")
    
    # Check the fields
    license_col = "Lic #"
    api_col = "API/UWI"
    
    print(f"\n📋 Field Analysis:")
    print(f"   License column: '{license_col}'")
    print(f"   API/UWI column: '{api_col}'")
    
    # Check coverage
    license_coverage = df[license_col].notna().sum()
    api_coverage = df[api_col].notna().sum()
    
    print(f"\n📈 Data Coverage:")
    print(f"   License numbers: {license_coverage:,} ({license_coverage/len(df)*100:.1f}%)")
    print(f"   API/UWI numbers: {api_coverage:,} ({api_coverage/len(df)*100:.1f}%)")
    
    # Sample the data to see formats
    print(f"\n🔍 Sample License Numbers:")
    license_samples = df[license_col].dropna().head(10).tolist()
    for i, sample in enumerate(license_samples, 1):
        print(f"   {i:2d}. {sample}")
    
    print(f"\n🔍 Sample API/UWI Numbers:")
    api_samples = df[api_col].dropna().head(10).tolist()
    for i, sample in enumerate(api_samples, 1):
        print(f"   {i:2d}. {sample}")
    
    # Check if they're the same values
    print(f"\n🔬 Comparison Analysis:")
    
    # Create a subset with both values present
    both_present = df[(df[license_col].notna()) & (df[api_col].notna())].copy()
    print(f"   Records with both values: {len(both_present):,}")
    
    if len(both_present) > 0:
        # Convert to strings for comparison
        both_present['license_str'] = both_present[license_col].astype(str)
        both_present['api_str'] = both_present[api_col].astype(str)
        
        # Direct comparison
        exact_matches = (both_present['license_str'] == both_present['api_str']).sum()
        print(f"   Exact matches: {exact_matches:,} ({exact_matches/len(both_present)*100:.1f}%)")
        
        # Check if one is contained in the other
        license_in_api = both_present.apply(
            lambda row: str(row['license_str']) in str(row['api_str']), axis=1
        ).sum()
        
        api_in_license = both_present.apply(
            lambda row: str(row['api_str']) in str(row['license_str']), axis=1
        ).sum()
        
        print(f"   License contained in API/UWI: {license_in_api:,}")
        print(f"   API/UWI contained in License: {api_in_license:,}")
        
        # Show some examples where they differ
        different = both_present[both_present['license_str'] != both_present['api_str']]
        if len(different) > 0:
            print(f"\n📋 Examples where they differ:")
            for i, row in different.head(5).iterrows():
                print(f"   License: {row['license_str']}")
                print(f"   API/UWI: {row['api_str']}")
                print(f"   Well: {row['Official Well Name']}")
                print()
    
    # Check unique counts
    unique_licenses = df[license_col].nunique()
    unique_apis = df[api_col].nunique()
    
    print(f"🎯 Unique Value Counts:")
    print(f"   Unique License numbers: {unique_licenses:,}")
    print(f"   Unique API/UWI numbers: {unique_apis:,}")
    
    # Check format patterns
    print(f"\n🔍 Format Pattern Analysis:")
    
    # License number patterns
    if license_coverage > 0:
        license_lengths = df[license_col].dropna().astype(str).str.len()
        print(f"   License number lengths: {license_lengths.min()}-{license_lengths.max()} chars")
        print(f"   Most common length: {license_lengths.mode().iloc[0]} chars")
    
    # API/UWI patterns  
    if api_coverage > 0:
        api_lengths = df[api_col].dropna().astype(str).str.len()
        print(f"   API/UWI lengths: {api_lengths.min()}-{api_lengths.max()} chars")
        print(f"   Most common length: {api_lengths.mode().iloc[0]} chars")
    
    # Check for common Canadian UWI format (16 digits)
    if api_coverage > 0:
        api_strings = df[api_col].dropna().astype(str)
        canadian_uwi_pattern = api_strings.str.len() == 16
        canadian_uwis = canadian_uwi_pattern.sum()
        print(f"   16-digit UWIs (Canadian format): {canadian_uwis:,}")
    
    return {
        'license_coverage': license_coverage,
        'api_coverage': api_coverage,
        'unique_licenses': unique_licenses,
        'unique_apis': unique_apis,
        'total_records': len(df)
    }

def check_ulterra_api_format():
    """Check what API format Ulterra uses"""
    
    print(f"\n🔍 Ulterra API Number Analysis")
    print("=" * 40)
    
    # Load one Ulterra file to check API format
    ulterra_files = list(Path("Input/Ulterra").glob("*.xlsx"))
    
    if not ulterra_files:
        print("❌ No Ulterra files found")
        return
    
    # Use the first file
    file_path = ulterra_files[0]
    print(f"📁 Analyzing: {file_path.name}")
    
    try:
        df = pd.read_excel(file_path, sheet_name="Bit Runs Export", nrows=100)  # Sample first 100 rows
        
        api_col = "APINumber"
        if api_col in df.columns:
            api_coverage = df[api_col].notna().sum()
            print(f"   API coverage: {api_coverage}/100 records")
            
            if api_coverage > 0:
                api_samples = df[api_col].dropna().head(10).tolist()
                print(f"   Sample API numbers:")
                for i, sample in enumerate(api_samples, 1):
                    print(f"     {i:2d}. {sample}")
                
                # Check lengths
                api_lengths = df[api_col].dropna().astype(str).str.len()
                print(f"   API lengths: {api_lengths.min()}-{api_lengths.max()} chars")
        else:
            print(f"   ❌ Column '{api_col}' not found")
            print(f"   Available columns: {list(df.columns)}")
    
    except Exception as e:
        print(f"   ❌ Error reading file: {e}")

def main():
    """Main analysis function"""
    
    result = analyze_license_vs_api()
    check_ulterra_api_format()
    
    print(f"\n🎯 CONCLUSION:")
    print("=" * 30)
    
    if result:
        license_cov = result['license_coverage'] / result['total_records']
        api_cov = result['api_coverage'] / result['total_records']
        
        if license_cov > 0.9 and api_cov > 0.9:
            print("✅ Both License and API/UWI have good coverage")
        elif license_cov > api_cov:
            print("📋 License numbers have better coverage")
        else:
            print("🔢 API/UWI numbers have better coverage")
        
        print(f"\n💡 RECOMMENDATION:")
        if result['unique_licenses'] != result['unique_apis']:
            print("   License and API/UWI appear to be DIFFERENT identifier systems")
            print("   Consider using both for comprehensive well identification")
        else:
            print("   License and API/UWI might be the SAME identifier system")
            print("   Could use either one for well counting")

if __name__ == "__main__":
    main()
