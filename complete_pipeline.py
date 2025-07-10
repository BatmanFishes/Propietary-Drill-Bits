#!/usr/bin/env python3
"""
Complete Pipeline with Enhanced Output Generation
Runs the full integration pipeline and generates the final enhanced output with GDC well data.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Add current directory to path
sys.path.append('core')

def run_complete_pipeline():
    """Run the complete pipeline and generate enhanced output"""
    
    print("🚀 COMPLETE PIPELINE WITH ENHANCED OUTPUT GENERATION")
    print("=" * 60)
    
    try:
        # Step 1: Run fresh integration with contractor standardization
        print("\n📊 Step 1: Running Fresh Integration with Contractor Standardization")
        from universal_data_integration import DataIntegrationEngine
        
        engine = DataIntegrationEngine()
        df = engine.integrate_all_sources()
        print(f"   ✅ Integrated {len(df)} records, {len(df.columns)} columns")
        print(f"   � Fresh integration with latest standardizations")
        
        # Step 2: Run GDC Enhancement
        print(f"\n🎯 Step 2: Running GDC Enhancement with Well Data")
        from gdc_enhancement import enhance_with_gdc
        
        enhanced_df, stats = enhance_with_gdc(df, Path('core'))
        
        print(f"\n📈 GDC Enhancement Results:")
        print(f"   🔢 Total records processed: {stats.get('total_records', 0)}")
        print(f"   🎯 GDC matches found: {stats.get('gdc_matches_found', 0)}")
        print(f"   🔄 License numbers enhanced: {stats.get('license_enhanced', 0)}")
        print(f"   🆔 UWI numbers enhanced: {stats.get('uwi_enhanced', 0)}")
        print(f"   📝 UWI formatted enhanced: {stats.get('uwi_formatted_enhanced', 0)}")
        print(f"   🌍 Province enhanced: {stats.get('province_enhanced', 0)}")
        print(f"   🏗️  GDC well fields enhanced: {stats.get('gdc_well_fields_enhanced', 0)}")
        
        # Step 3: Verify GDC Well Data Fields
        print(f"\n🏗️  Step 3: Verifying GDC Well Data Fields")
        gdc_fields = [col for col in enhanced_df.columns if col.startswith('gdc_')]
        
        if gdc_fields:
            print(f"   ✅ Found {len(gdc_fields)} GDC well data fields:")
            for i, field in enumerate(gdc_fields, 1):
                count = enhanced_df[field].notna().sum()
                print(f"   {i:2d}. {field}: {count} records with data")
        else:
            print(f"   ⚠️  No GDC well data fields found")
        
        # Step 4: Generate Enhanced Output File
        print(f"\n💾 Step 4: Generating Enhanced Output File")
        
        # Ensure key identifier fields are properly formatted as strings
        print(f"   🔧 Ensuring key fields are exported as strings...")
        string_fields = ['license_number', 'bit_serial_number', 'uwi_number', 'uwi_formatted', 'well_name']
        for field in string_fields:
            if field in enhanced_df.columns:
                # Convert to string, handling NaN values appropriately
                enhanced_df[field] = enhanced_df[field].astype('object').fillna('').astype(str)
                # Replace 'nan' strings with actual NaN for clean output
                enhanced_df[field] = enhanced_df[field].replace('nan', pd.NA)
                print(f"      ✅ {field}: converted to string format")
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"Output/Enhanced_BitData_Complete_{timestamp}.csv"
        
        # Save enhanced dataset as CSV with proper string formatting
        # For CSV, we need to ensure bit serial numbers and other critical strings are quoted
        # to preserve their string nature and prevent scientific notation
        print(f"   🔧 Preparing CSV export with string preservation...")
        
        # Create a copy for CSV export with explicit string formatting
        csv_df = enhanced_df.copy()
        
        # For critical numeric strings like bit_serial_number, add leading zeros or quotes if needed
        # to ensure they're treated as strings in CSV readers
        if 'bit_serial_number' in csv_df.columns:
            # Ensure bit serial numbers are treated as strings by adding a leading apostrophe
            # This will be preserved in CSV and prevent scientific notation
            csv_df['bit_serial_number'] = csv_df['bit_serial_number'].apply(
                lambda x: f"'{x}" if pd.notna(x) and str(x) != '' and str(x) != 'nan' else x
            )
            print(f"      📋 bit_serial_number: added string preservation formatting")
        
        # Save as CSV with quoting to preserve string formatting
        csv_df.to_csv(output_file, index=False, quoting=1)  # quoting=1 means QUOTE_ALL
        
        print(f"   ✅ Enhanced dataset saved: {output_file}")
        print(f"   📊 Final dataset: {len(enhanced_df)} records, {len(enhanced_df.columns)} columns")
        print(f"   📋 CSV format: All fields quoted to preserve string formatting")
        
        # Step 5: Generate Summary Report
        print(f"\n📋 Step 5: Final Dataset Summary")
        
        # Data source breakdown
        if 'data_source' in enhanced_df.columns:
            source_counts = enhanced_df['data_source'].value_counts()
            print(f"   📊 Data Sources:")
            for source, count in source_counts.items():
                print(f"      {source}: {count:,} records")
        
        # Field categories
        from data_mapping_config import DataMappingConfig
        config = DataMappingConfig()
        
        # Check field categories
        categories = ['well_identification', 'bits', 'performance_metrics', 'gdc_well_data']
        for category in categories:
            cat_fields = config.get_category_fields(category)
            available_fields = [f for f in cat_fields if f in enhanced_df.columns]
            print(f"   📂 {category}: {len(available_fields)}/{len(cat_fields)} fields available")
        
        # Sample enhanced record
        if stats.get('gdc_matches_found', 0) > 0:
            print(f"\n🔍 Sample Enhanced Record:")
            sample = enhanced_df[enhanced_df['license_number'].notna()].iloc[0]
            
            key_fields = ['license_number', 'well_name', 'operator', 'bit_manufacturer']
            gdc_sample_fields = ['gdc_surface_latitude', 'gdc_surface_longitude', 'gdc_spud_date']
            
            print(f"   📍 Well Information:")
            for field in key_fields:
                if field in enhanced_df.columns:
                    value = sample.get(field, 'N/A')
                    print(f"      {field}: {value}")
            
            print(f"   🏗️  GDC Well Data:")
            for field in gdc_sample_fields:
                if field in enhanced_df.columns:
                    value = sample.get(field, 'N/A')
                    print(f"      {field}: {value}")
        
        print(f"\n🎉 COMPLETE PIPELINE SUCCESS!")
        print(f"   📁 Enhanced Output: {output_file}")
        print(f"   📊 Total Fields: {len(enhanced_df.columns)}")
        print(f"   🏗️  GDC Well Data: {len(gdc_fields)} additional fields")
        print(f"   ✅ Ready for Analysis!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_complete_pipeline()
    if success:
        print(f"\n🚀 PIPELINE COMPLETED SUCCESSFULLY!")
    else:
        print(f"\n⚠️  PIPELINE ENCOUNTERED ERRORS")
