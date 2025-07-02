"""
Demo: Adding New Data Sources
This script demonstrates how easy it is to add new data sources to the integration system.
"""

from universal_data_integration import DataIntegrationEngine
from data_mapping_config import DataMappingConfig, SourceConfig
from source_config_tool import SourceConfigurationTool

def demo_add_new_source():
    """Demonstrate how to add a new data source"""
    
    print("🎯 Demo: Adding New Data Sources")
    print("=" * 50)
    
    # Example: Adding a hypothetical "Baker Hughes" data source
    print("\n📝 Example: Adding a 'Baker Hughes' data source")
    print("-" * 40)
    
    # Create configuration
    baker_hughes_config = SourceConfig(
        name='Baker Hughes',
        description='Baker Hughes bit performance data',
        folder_path='Input/BakerHughes',  # Would be where their files are stored
        file_pattern='*.xlsx',
        sheet_name='Performance Data',   # Their sheet name
        column_mappings={
            'well_name': 'Well_Name',
            'operator': 'Operator_Company',
            'bit_manufacturer': 'Bit_Manufacturer',
            'bit_serial_number': 'Bit_Serial',
            'bit_size_mm': 'Bit_Diameter_mm',
            'bit_type': 'Bit_Model',
            'depth_in_m': 'Start_Depth_m',
            'depth_out_m': 'End_Depth_m',
            'drilling_hours': 'Drilling_Time_hrs',
            'rop_mhr': 'ROP_m_per_hr',
            'spud_date': 'Spud_Date',
            'dull_reason': 'Pull_Reason',
            # ... more mappings as needed
        }
    )
    
    print("🔧 Configuration created:")
    print(f"   Name: {baker_hughes_config.name}")
    print(f"   Folder: {baker_hughes_config.folder_path}")
    mappings_count = len(baker_hughes_config.column_mappings) if baker_hughes_config.column_mappings else 0
    print(f"   Mappings: {mappings_count} fields")
    
    # Show how to add it to the system
    print(f"\n🔌 To add this to the system:")
    print("1. Add the configuration to data_mapping_config.py:")
    print(f"   In _define_data_sources(), add:")
    print(f"   'baker_hughes': baker_hughes_config")
    
    print(f"\n2. The integration engine will automatically:")
    print("   ✅ Discover files in Input/BakerHughes/")
    print("   ✅ Map columns to standard format")
    print("   ✅ Apply data type conversions")
    print("   ✅ Include in unified reports")

def demo_analyze_existing_source():
    """Demonstrate analyzing an existing source structure"""
    
    print("\n🔍 Demo: Analyzing Existing Source Structure")
    print("=" * 50)
    
    # Analyze the Reed data structure
    tool = SourceConfigurationTool()
    
    reed_file = "Input/Reed/All+Montney+since+2020.xlsx"
    print(f"📊 Analyzing: {reed_file}")
    
    try:
        analysis = tool.analyze_new_source(reed_file, "All+Montney+since+2020")
        sheet_data = analysis['sheets']['All+Montney+since+2020']
        
        print(f"\n📋 Analysis Results:")
        print(f"   Total columns: {sheet_data['total_columns']}")
        print(f"   Sample columns:")
        for col in sheet_data['columns'][:10]:
            print(f"     - {col}")
        if len(sheet_data['columns']) > 10:
            print(f"     ... and {len(sheet_data['columns']) - 10} more")
            
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")

def demo_data_categories():
    """Demonstrate working with data categories"""
    
    print("\n📂 Demo: Data Categories")
    print("=" * 30)
    
    config = DataMappingConfig()
    
    print("🗂️  Available data categories:")
    for category in config.get_all_categories():
        fields = config.get_category_fields(category)
        print(f"   {category}: {len(fields)} fields")
        
        # Show a few example fields
        example_fields = fields[:3]
        if example_fields:
            print(f"     Examples: {', '.join(example_fields)}")
        if len(fields) > 3:
            print(f"     ... and {len(fields) - 3} more")
    
    print(f"\n🔍 You can analyze data by category:")
    print(f"   # Get all performance fields")
    print(f"   performance_fields = config.get_category_fields('performance_metrics')")
    print(f"   ")
    print(f"   # Filter integrated data for performance analysis")
    print(f"   performance_data = integrated_df[performance_fields]")

def demo_quality_validation():
    """Demonstrate data quality validation"""
    
    print("\n✅ Demo: Data Quality & Validation")
    print("=" * 40)
    
    engine = DataIntegrationEngine()
    
    # Load current integrated data to show quality metrics
    print("📊 Loading current integrated data for quality analysis...")
    
    try:
        integrated_df = engine.integrate_all_sources()
        quality_report = engine.get_data_quality_report()
        
        print(f"\n📈 Data Quality Metrics:")
        print(f"   Total Records: {quality_report['total_records']:,}")
        print(f"   Data Sources: {len(quality_report['source_distribution'])}")
        print(f"   Unique Wells: {quality_report['unique_wells']:,}")
        print(f"   Date Range: {min(quality_report['date_ranges']['spud_date']['min'], quality_report['date_ranges']['spud_date']['min'])} to {max(quality_report['date_ranges']['spud_date']['max'], quality_report['date_ranges']['spud_date']['max'])}")
        
        print(f"\n🎯 Top Completion Rates:")
        sorted_completion = sorted(quality_report['completion_rates'].items(), 
                                 key=lambda x: x[1], reverse=True)
        for field, rate in sorted_completion[:5]:
            print(f"   {field}: {rate:.1f}%")
            
    except Exception as e:
        print(f"❌ Error in quality analysis: {e}")

def main():
    """Run all demonstrations"""
    
    print("🚀 Universal Data Integration System")
    print("📚 Comprehensive Demo & Tutorial")
    print("=" * 60)
    
    # Run demonstrations
    demo_add_new_source()
    demo_analyze_existing_source()
    demo_data_categories()
    demo_quality_validation()
    
    print(f"\n🎉 Demo Complete!")
    print(f"\n📖 Key Benefits:")
    print(f"   ✅ Unified data format across all sources")
    print(f"   ✅ Automatic unit conversions (imperial ↔ metric)")
    print(f"   ✅ Standardized field names and types")
    print(f"   ✅ Built-in data quality validation")
    print(f"   ✅ Easy addition of new data sources")
    print(f"   ✅ Category-based analysis and reporting")
    print(f"   ✅ Comprehensive metadata tracking")
    
    print(f"\n🔧 Next Steps:")
    print(f"   1. Use source_config_tool.py to add new sources interactively")
    print(f"   2. Modify data_mapping_config.py to add custom field mappings")
    print(f"   3. Use universal_data_integration.py for routine data processing")
    print(f"   4. Build custom analysis scripts using the integrated data")

if __name__ == "__main__":
    main()
