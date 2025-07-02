"""
Well Counting Analysis
Analyze how well counts are determined using different identifiers
"""

import pandas as pd
from universal_data_integration import DataIntegrationEngine

def analyze_well_counting():
    """Analyze different methods for counting unique wells"""
    
    print('=== WELL COUNTING ANALYSIS ===')
    print()

    # Load the integrated data
    engine = DataIntegrationEngine()
    integrated_df = engine.integrate_all_sources()

    # Current method (well_name)
    well_name_count = integrated_df['well_name'].nunique()
    print(f'Unique well_name count: {well_name_count:,}')

    # Check UWI/API availability
    api_available = integrated_df['api_number'].notna().sum()
    license_available = integrated_df['license_number'].notna().sum()

    print(f'Records with API/UWI: {api_available:,} ({api_available/len(integrated_df)*100:.1f}%)')
    print(f'Records with License #: {license_available:,} ({license_available/len(integrated_df)*100:.1f}%)')

    # Count by API/UWI where available
    if api_available > 0:
        api_count = integrated_df['api_number'].nunique()
        print(f'Unique API/UWI count: {api_count:,}')

    # Count by license where available
    if license_available > 0:
        license_count = integrated_df['license_number'].nunique()
        print(f'Unique License # count: {license_count:,}')

    print()
    print('=== BY DATA SOURCE ===')
    for source in integrated_df['data_source'].unique():
        source_data = integrated_df[integrated_df['data_source'] == source]
        print(f'\n{source}:')
        print(f'  Well names: {source_data["well_name"].nunique():,}')
        print(f'  API/UWI available: {source_data["api_number"].notna().sum():,}')
        print(f'  License # available: {source_data["license_number"].notna().sum():,}')
        
        if source_data['api_number'].notna().sum() > 0:
            unique_api = source_data['api_number'].nunique()
            print(f'  Unique API/UWI: {unique_api:,}')
        
        if source_data['license_number'].notna().sum() > 0:
            unique_license = source_data['license_number'].nunique()
            print(f'  Unique License #: {unique_license:,}')

    # Sample some data to see the formats
    print('\n=== SAMPLE IDENTIFIERS ===')
    
    # Sample API/UWI numbers
    sample_apis = integrated_df['api_number'].dropna().head(10)
    if len(sample_apis) > 0:
        print('\nSample API/UWI numbers:')
        for api in sample_apis:
            print(f'  {api}')
    
    # Sample license numbers
    sample_licenses = integrated_df['license_number'].dropna().head(10)
    if len(sample_licenses) > 0:
        print('\nSample License numbers:')
        for license_num in sample_licenses:
            print(f'  {license_num}')
    
    # Sample well names
    print('\nSample Well names:')
    sample_wells = integrated_df['well_name'].dropna().head(10)
    for well in sample_wells:
        print(f'  {well}')

    # Check for overlaps between sources
    print('\n=== CROSS-SOURCE ANALYSIS ===')
    
    ulterra_data = integrated_df[integrated_df['data_source'] == 'ulterra']
    reed_data = integrated_df[integrated_df['data_source'] == 'reed']
    
    # Check for common wells by different identifiers
    ulterra_wells = set(ulterra_data['well_name'].dropna())
    reed_wells = set(reed_data['well_name'].dropna())
    common_well_names = ulterra_wells.intersection(reed_wells)
    print(f'Common well names between sources: {len(common_well_names)}')
    
    if len(common_well_names) > 0:
        print('Sample common well names:')
        for well in list(common_well_names)[:5]:
            print(f'  {well}')
    
    # Check API/UWI overlap if available
    ulterra_apis = set(ulterra_data['api_number'].dropna())
    reed_apis = set(reed_data['api_number'].dropna())
    
    if len(ulterra_apis) > 0 and len(reed_apis) > 0:
        common_apis = ulterra_apis.intersection(reed_apis)
        print(f'Common API/UWI numbers between sources: {len(common_apis)}')

if __name__ == "__main__":
    analyze_well_counting()
