#!/usr/bin/env python3
"""
Analyze Contractor and Rig Name Data
Understand current contractor and rig name values to inform standardization
"""

import pandas as pd
import sys
from pathlib import Path

# Add current directory to path
sys.path.append('.')

def analyze_contractor_rig_data():
    """Analyze contractor and rig name fields in the integrated data"""
    
    print("=== CONTRACTOR AND RIG NAME ANALYSIS ===")
    
    try:
        # Import the engine
        from universal_data_integration import DataIntegrationEngine
        
        # Create engine and load data
        engine = DataIntegrationEngine()
        print("✅ Engine created successfully")
        
        # Run integration to get current data
        print("\n🔄 Integrating data sources...")
        integrated_data = engine.integrate_all_sources()
        print(f"   Integrated {len(integrated_data)} total records")
        
        # Check if contractor and rig_name fields exist
        has_contractor = 'contractor' in integrated_data.columns
        has_rig_name = 'rig_name' in integrated_data.columns
        
        print(f"\n📊 Field Availability:")
        print(f"   Contractor field: {'✅ Available' if has_contractor else '❌ Missing'}")
        print(f"   Rig name field: {'✅ Available' if has_rig_name else '❌ Missing'}")
        
        if has_contractor:
            print(f"\n🏢 CONTRACTOR ANALYSIS:")
            print("=" * 50)
            
            # Count non-null contractors
            contractor_data = integrated_data['contractor'].dropna()
            print(f"Records with contractor data: {len(contractor_data):,} of {len(integrated_data):,} ({len(contractor_data)/len(integrated_data)*100:.1f}%)")
            
            if len(contractor_data) > 0:
                # Show unique contractors
                unique_contractors = contractor_data.unique()
                print(f"Unique contractors: {len(unique_contractors)}")
                
                # Show contractor value distribution
                contractor_counts = contractor_data.value_counts()
                print(f"\nTop 20 contractors by record count:")
                for i, (contractor, count) in enumerate(contractor_counts.head(20).items(), 1):
                    print(f"   {i:2d}. {contractor:<40} ({count:,} records)")
                
                # Show sample contractor values for analysis
                print(f"\nSample contractor values (for standardization analysis):")
                sample_contractors = sorted(unique_contractors)[:30]
                for i, contractor in enumerate(sample_contractors, 1):
                    print(f"   {i:2d}. '{contractor}'")
                
                # Check by data source
                print(f"\nContractor by data source:")
                if 'data_source' in integrated_data.columns:
                    contractor_by_source = integrated_data[integrated_data['contractor'].notna()].groupby(['data_source', 'contractor']).size().unstack(fill_value=0)
                    print(contractor_by_source.head(20))
        
        if has_rig_name:
            print(f"\n🏗️ RIG NAME ANALYSIS:")
            print("=" * 50)
            
            # Count non-null rig names
            rig_data = integrated_data['rig_name'].dropna()
            print(f"Records with rig name data: {len(rig_data):,} of {len(integrated_data):,} ({len(rig_data)/len(integrated_data)*100:.1f}%)")
            
            if len(rig_data) > 0:
                # Show unique rig names
                unique_rigs = rig_data.unique()
                print(f"Unique rig names: {len(unique_rigs)}")
                
                # Show rig name value distribution
                rig_counts = rig_data.value_counts()
                print(f"\nTop 20 rig names by record count:")
                for i, (rig, count) in enumerate(rig_counts.head(20).items(), 1):
                    print(f"   {i:2d}. {rig:<40} ({count:,} records)")
                
                # Show sample rig name values
                print(f"\nSample rig name values:")
                sample_rigs = sorted(unique_rigs)[:30]
                for i, rig in enumerate(sample_rigs, 1):
                    print(f"   {i:2d}. '{rig}'")
                
                # Check by data source
                print(f"\nRig names by data source:")
                if 'data_source' in integrated_data.columns:
                    rig_by_source = integrated_data[integrated_data['rig_name'].notna()].groupby(['data_source', 'rig_name']).size().unstack(fill_value=0)
                    print(rig_by_source.head(20))
        
        # Combined analysis - where both contractor and rig name are available
        if has_contractor and has_rig_name:
            print(f"\n🔗 COMBINED CONTRACTOR + RIG NAME ANALYSIS:")
            print("=" * 50)
            
            both_available = integrated_data[
                integrated_data['contractor'].notna() & 
                integrated_data['rig_name'].notna()
            ]
            
            print(f"Records with both contractor and rig name: {len(both_available):,}")
            
            if len(both_available) > 0:
                print(f"\nSample combined contractor + rig name combinations:")
                combined_sample = both_available[['contractor', 'rig_name']].drop_duplicates().head(20)
                for i, (_, row) in enumerate(combined_sample.iterrows(), 1):
                    contractor = row['contractor']
                    rig = row['rig_name']
                    print(f"   {i:2d}. {contractor:<25} | {rig}")
                
                # Show how many unique combinations we have
                unique_combinations = both_available.groupby(['contractor', 'rig_name']).size().reset_index(name='count')
                print(f"\nUnique contractor + rig combinations: {len(unique_combinations)}")
                
                # Show top combinations by frequency
                print(f"\nTop 15 contractor + rig combinations by frequency:")
                top_combinations = unique_combinations.sort_values('count', ascending=False).head(15)
                for i, (_, row) in enumerate(top_combinations.iterrows(), 1):
                    contractor = row['contractor']
                    rig = row['rig_name']
                    count = row['count']
                    print(f"   {i:2d}. {contractor:<25} | {rig:<20} ({count:,} records)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in analysis: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    analyze_contractor_rig_data()
