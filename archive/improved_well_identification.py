"""
Improved Well Identification System
Uses the most appropriate identifier for accurate well counting
"""

import pandas as pd
from universal_data_integration import DataIntegrationEngine

class ImprovedWellCounter:
    """Enhanced well counting using the best available identifiers"""
    
    def __init__(self):
        self.engine = DataIntegrationEngine()
    
    def analyze_well_identifiers(self, df=None):
        """Comprehensive analysis of well identification methods"""
        
        if df is None:
            print("🔄 Loading integrated data...")
            df = self.engine.integrate_all_sources()
        
        print("🎯 Well Identification Analysis")
        print("=" * 50)
        
        total_records = len(df)
        print(f"📊 Total records: {total_records:,}")
        
        # Analyze each identifier type
        identifiers = {
            'well_name': 'Well Names',
            'api_number': 'API/UWI Numbers', 
            'license_number': 'License Numbers'
        }
        
        results = {}
        
        for field, label in identifiers.items():
            if field in df.columns:
                # Coverage analysis
                non_null = df[field].notna()
                coverage = non_null.sum()
                coverage_pct = coverage / total_records * 100
                
                # Unique count
                unique_count = df[field].nunique()
                
                # By source analysis
                source_analysis = df.groupby('data_source')[field].agg(['count', 'nunique']).round(2)
                
                results[field] = {
                    'label': label,
                    'coverage': coverage,
                    'coverage_pct': coverage_pct,
                    'unique_count': unique_count,
                    'source_breakdown': source_analysis
                }
                
                print(f"\n📋 {label}:")
                print(f"   Coverage: {coverage:,} records ({coverage_pct:.1f}%)")
                print(f"   Unique values: {unique_count:,}")
                print(f"   By source:")
                for source, stats in source_analysis.iterrows():
                    print(f"     {source}: {stats['nunique']:,} unique from {stats['count']:,} records")
        
        return results
    
    def create_master_well_list(self, df=None):
        """Create a master well list using the best available identifiers"""
        
        if df is None:
            df = self.engine.integrate_all_sources()
        
        print("\n🔧 Creating Master Well List")
        print("=" * 40)
        
        # Create well master records
        well_records = []
        
        # Process each record to build well identifiers
        for idx, row in df.iterrows():
            well_record = {
                'data_source': row['data_source'],
                'well_name': row.get('well_name'),
                'api_number': row.get('api_number'),
                'license_number': row.get('license_number'),
                'operator': row.get('operator'),
                'field': row.get('field'),
                'spud_date': row.get('spud_date'),
                'record_count': 1
            }
            well_records.append(well_record)
        
        well_df = pd.DataFrame(well_records)
        
        # Strategy: Use API/UWI as primary, fallback to License, then Well Name
        print("🎯 Well Identification Strategy:")
        print("   1. Primary: API/UWI numbers (most standardized)")
        print("   2. Secondary: License numbers (for Canadian wells)")
        print("   3. Fallback: Well names (when no numeric ID available)")
        
        # Create composite well identifier
        def create_well_id(row, idx):
            # Priority 1: Use API/UWI if available
            if pd.notna(row['api_number']) and str(row['api_number']).strip():
                return f"API_{row['api_number']}"
            
            # Priority 2: Use License number if available  
            elif pd.notna(row['license_number']) and str(row['license_number']).strip():
                return f"LIC_{row['license_number']}"
            
            # Priority 3: Use well name as fallback
            elif pd.notna(row['well_name']) and str(row['well_name']).strip():
                return f"NAME_{row['well_name']}"
            
            # Last resort: create unique ID
            else:
                return f"UNK_{idx}"
        
        well_df['composite_well_id'] = [create_well_id(row, idx) for idx, row in well_df.iterrows()]
        
        # Aggregate by composite well ID
        master_wells = well_df.groupby('composite_well_id').agg({
            'well_name': 'first',
            'api_number': 'first', 
            'license_number': 'first',
            'operator': 'first',
            'field': 'first',
            'spud_date': 'first',
            'data_source': lambda x: ', '.join(sorted(x.unique())),
            'record_count': 'sum'
        }).reset_index()
        
        # Analysis by identifier type
        api_wells = master_wells[master_wells['composite_well_id'].str.startswith('API_')]
        license_wells = master_wells[master_wells['composite_well_id'].str.startswith('LIC_')]
        name_wells = master_wells[master_wells['composite_well_id'].str.startswith('NAME_')]
        
        print(f"\n📊 Master Well List Results:")
        print(f"   Total unique wells: {len(master_wells):,}")
        print(f"   - Identified by API/UWI: {len(api_wells):,} ({len(api_wells)/len(master_wells)*100:.1f}%)")
        print(f"   - Identified by License: {len(license_wells):,} ({len(license_wells)/len(master_wells)*100:.1f}%)")
        print(f"   - Identified by Name only: {len(name_wells):,} ({len(name_wells)/len(master_wells)*100:.1f}%)")
        
        # Cross-source analysis
        multi_source_wells = master_wells[master_wells['data_source'].str.contains(',')]
        print(f"   - Wells in multiple sources: {len(multi_source_wells):,}")
        
        return master_wells
    
    def update_integration_config(self):
        """Update the integration system to use improved well counting"""
        
        print("\n🔧 Updating Integration Configuration")
        print("=" * 45)
        
        print("📝 Recommendations for data_mapping_config.py:")
        print("\n1. Make API/UWI and License numbers more prominent:")
        print("   - Consider making api_number required for better well tracking")
        print("   - Add composite_well_id as a derived field")
        
        print("\n2. Update quality reporting to use composite well ID:")
        print("   - Replace well_name.nunique() with composite_well_id.nunique()")
        print("   - Add identifier type breakdown in quality reports")
        
        print("\n3. Add well deduplication logic:")
        print("   - Check for same wells across sources using API/UWI")
        print("   - Flag potential duplicates for review")
        
        # Show the specific changes needed
        print("\n🔄 Specific Code Changes Needed:")
        print("-" * 35)
        print("In universal_data_integration.py, update _add_derived_fields():")
        print("""
# Add composite well identifier
def create_composite_well_id(row, idx):
    if pd.notna(row['api_number']) and str(row['api_number']).strip():
        return f"API_{row['api_number']}"
    elif pd.notna(row['license_number']) and str(row['license_number']).strip():
        return f"LIC_{row['license_number']}"
    elif pd.notna(row['well_name']) and str(row['well_name']).strip():
        return f"NAME_{row['well_name']}"
    else:
        return f"UNK_{idx}"

df['composite_well_id'] = df.apply(lambda row: create_composite_well_id(row, row.name), axis=1)
        """)
        
        print("\nIn quality reports, replace:")
        print("   unique_wells = df['well_name'].nunique()")
        print("With:")
        print("   unique_wells = df['composite_well_id'].nunique()")

def main():
    """Run improved well identification analysis"""
    
    print("🚀 Improved Well Identification System")
    print("=" * 55)
    
    counter = ImprovedWellCounter()
    
    # Load and analyze current data
    df = counter.engine.integrate_all_sources()
    
    # Analyze identifier quality
    results = counter.analyze_well_identifiers(df)
    
    # Create master well list
    master_wells = counter.create_master_well_list(df)
    
    # Save master well list
    output_path = counter.engine.base_path / "Output" / "Master_Well_List.xlsx"
    master_wells.to_excel(output_path, index=False)
    print(f"\n💾 Master well list saved: {output_path}")
    
    # Configuration recommendations
    counter.update_integration_config()
    
    print(f"\n✅ Analysis Complete!")
    print(f"\n🎯 KEY FINDINGS:")
    print(f"   • License and API/UWI are DIFFERENT identifier systems")
    print(f"   • Reed uses 16-digit UWI format (Canadian standard)")
    print(f"   • Ulterra uses 6-digit API numbers") 
    print(f"   • License numbers provide good coverage for Reed data")
    print(f"   • Composite identifier approach gives most accurate well count")

if __name__ == "__main__":
    main()
