"""
Data Source Configuration Tool
Interactive tool to help add and configure new data sources for the integration engine.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import json

class SourceConfigurationTool:
    """Interactive tool for configuring new data sources"""
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).parent
    
    def analyze_new_source(self, file_path: str, sheet_name: Optional[str] = None) -> Dict:
        """Analyze a new data file to understand its structure"""
        path = Path(file_path)
        
        if not path.exists():
            # Try relative to base path
            path = self.base_path / file_path
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        print(f"🔍 Analyzing file: {path.name}")
        
        # Read Excel file structure
        excel_file = pd.ExcelFile(path)
        sheet_names = excel_file.sheet_names
        
        print(f"📊 Sheets found: {sheet_names}")
        
        # Analyze each sheet or specified sheet
        analysis = {
            'file_path': str(path),
            'file_name': path.name,
            'sheets': {}
        }
        
        sheets_to_analyze = [sheet_name] if sheet_name else sheet_names
        
        for sheet in sheets_to_analyze:
            if sheet in sheet_names:
                df = pd.read_excel(path, sheet_name=sheet, nrows=10)  # Sample first 10 rows
                
                analysis['sheets'][sheet] = {
                    'total_columns': len(df.columns),
                    'columns': list(df.columns),
                    'sample_data': df.head(3).to_dict('records'),
                    'data_types': df.dtypes.astype(str).to_dict(),
                    'null_counts': df.isnull().sum().to_dict()
                }
                
                print(f"\n📋 Sheet: {sheet}")
                print(f"   Columns: {len(df.columns)}")
                print("   Column Names:")
                for i, col in enumerate(df.columns):
                    print(f"     {i+1:2d}. {col}")
        
        return analysis
    
    def suggest_field_mappings(self, analysis: Dict, sheet_name: str) -> Dict[str, str]:
        """Suggest field mappings based on column names"""
        
        if sheet_name not in analysis['sheets']:
            raise ValueError(f"Sheet '{sheet_name}' not found in analysis")
        
        columns = analysis['sheets'][sheet_name]['columns']
        
        # Common mapping patterns
        mapping_patterns = {
            # Well identification
            'well_name': ['well name', 'wellname', 'well_name', 'official well name'],
            'well_number': ['well number', 'wellnumber', 'well_number', 'well num'],
            'api_number': ['api', 'uwi', 'api number', 'api_number', 'api/uwi'],
            'license_number': ['license', 'lic', 'lic #', 'license number'],
            'operator': ['operator', 'operator name', 'operatorname', 'company'],
            'contractor': ['contractor', 'drilling contractor', 'rig contractor'],
            'rig_name': ['rig', 'rig name', 'rig number', 'rigname', 'rignumber'],
            
            # Location
            'field': ['field', 'field name'],
            'county': ['county', 'area'],
            'state_province': ['state', 'province', 'state/province'],
            'section': ['section', 'sect'],
            'township': ['township', 'twn'],
            'range': ['range', 'rng'],
            'lsd': ['lsd'],
            
            # Bit information
            'bit_manufacturer': ['bit mfg', 'bit manufacturer', 'manufacturer', 'mfgr', 'bitmfgr'],
            'bit_serial_number': ['serial', 'serial number', 'bit serial', 'serialno'],
            'bit_size_mm': ['bit size', 'size', 'bitsize'],
            'bit_type': ['bit type', 'type', 'bittype', 'model'],
            'iadc_code': ['iadc', 'iadc code'],
            
            # Run information
            'run_number': ['run', 'run number', 'run seq', 'sequence'],
            'spud_date': ['spud', 'spud date'],
            'td_date': ['td date', 'total depth date'],
            'depth_in_m': ['depth in', 'depthin', 'in', 'start depth'],
            'depth_out_m': ['depth out', 'depthout', 'out', 'end depth'],
            'distance_drilled_m': ['distance', 'drilled', 'footage'],
            
            # Performance
            'drilling_hours': ['hours', 'hrs', 'drilling hours', 'total hours'],
            'rop_mhr': ['rop', 'rate of penetration', 'penetration rate'],
            
            # Dull grading
            'dull_inner_row': ['inner', 'i'],
            'dull_outer_row': ['outer', 'o'],
            'dull_location': ['location', 'loc'],
            'dull_bearing_seals': ['bearing', 'b'],
            'dull_gauge': ['gauge', 'g'],
            'dull_reason': ['reason', 'rp', 'pull reason'],
        }
        
        suggested_mappings = {}
        
        for standard_field, patterns in mapping_patterns.items():
            for column in columns:
                column_lower = column.lower().strip()
                for pattern in patterns:
                    if pattern in column_lower or column_lower in pattern:
                        suggested_mappings[standard_field] = column
                        break
                if standard_field in suggested_mappings:
                    break
        
        return suggested_mappings
    
    def interactive_mapping_session(self, file_path: str, sheet_name: Optional[str] = None):
        """Run an interactive session to create field mappings"""
        
        print("🛠️  Interactive Data Source Configuration")
        print("=" * 50)
        
        # Analyze the file
        analysis = self.analyze_new_source(file_path, sheet_name)
        
        if len(analysis['sheets']) > 1 and sheet_name is None:
            print("\n📋 Multiple sheets found. Please specify which sheet to use:")
            for i, sheet in enumerate(analysis['sheets'].keys()):
                print(f"  {i+1}. {sheet}")
            
            choice = input("\nEnter sheet number or name: ").strip()
            try:
                if choice.isdigit():
                    sheet_name = list(analysis['sheets'].keys())[int(choice) - 1]
                else:
                    sheet_name = choice
            except (IndexError, ValueError):
                print("Invalid choice. Using first sheet.")
                sheet_name = list(analysis['sheets'].keys())[0]
        
        elif sheet_name is None:
            sheet_name = list(analysis['sheets'].keys())[0]
        
        print(f"\n🎯 Configuring sheet: {sheet_name}")
        
        # Ensure sheet_name is valid
        if not sheet_name or sheet_name not in analysis['sheets']:
            raise ValueError(f"Invalid sheet name: {sheet_name}")
        
        # Get suggested mappings
        suggested_mappings = self.suggest_field_mappings(analysis, sheet_name)
        
        print(f"\n🤖 Auto-suggested mappings ({len(suggested_mappings)} found):")
        for standard_field, source_column in suggested_mappings.items():
            print(f"  {standard_field} ← {source_column}")
        
        # Interactive refinement
        print("\n✏️  Review and modify mappings (press Enter to accept, type new column name to change):")
        
        final_mappings = {}
        available_columns = analysis['sheets'][sheet_name]['columns']
        
        # Show all available columns for reference
        print(f"\n📝 Available columns in {sheet_name}:")
        for i, col in enumerate(available_columns):
            print(f"  {i+1:2d}. {col}")
        
        print("\n" + "="*60)
        
        for standard_field, suggested_column in suggested_mappings.items():
            user_input = input(f"{standard_field:25} [{suggested_column}]: ").strip()
            
            if user_input == "":
                final_mappings[standard_field] = suggested_column
            elif user_input.lower() in ['skip', 'none', 'n/a']:
                continue
            elif user_input in available_columns:
                final_mappings[standard_field] = user_input
            else:
                # Try to find close match
                matches = [col for col in available_columns if user_input.lower() in col.lower()]
                if matches:
                    print(f"    Did you mean: {matches[0]}? (y/n)")
                    if input("    ").lower().startswith('y'):
                        final_mappings[standard_field] = matches[0]
                else:
                    print(f"    Column '{user_input}' not found. Skipping.")
        
        # Ask about additional mappings
        print(f"\n➕ Add additional mappings? Available unmapped columns:")
        mapped_columns = set(final_mappings.values())
        unmapped_columns = [col for col in available_columns if col not in mapped_columns]
        
        for i, col in enumerate(unmapped_columns[:10]):  # Show first 10
            print(f"  {i+1:2d}. {col}")
        
        if len(unmapped_columns) > 10:
            print(f"  ... and {len(unmapped_columns) - 10} more")
        
        while True:
            add_more = input("\nAdd mapping (standard_field=source_column) or 'done': ").strip()
            if add_more.lower() in ['done', 'finish', 'exit', '']:
                break
            
            if '=' in add_more:
                try:
                    standard_field, source_column = add_more.split('=', 1)
                    standard_field = standard_field.strip()
                    source_column = source_column.strip()
                    
                    if source_column in available_columns:
                        final_mappings[standard_field] = source_column
                        print(f"    ✅ Added: {standard_field} ← {source_column}")
                    else:
                        print(f"    ❌ Column '{source_column}' not found")
                except ValueError:
                    print("    ❌ Invalid format. Use: standard_field=source_column")
        
        # Generate source configuration
        print(f"\n📤 Generating configuration...")
        
        source_name = input(f"Enter source name (default: {Path(file_path).stem}): ").strip()
        if not source_name:
            source_name = Path(file_path).stem.lower().replace(' ', '_')
        
        description = input(f"Enter description (default: {source_name} data): ").strip()
        if not description:
            description = f"{source_name} drilling bit data"
        
        config = {
            'name': source_name,
            'description': description,
            'folder_path': str(Path(file_path).parent.relative_to(self.base_path)),
            'file_pattern': f"*{Path(file_path).suffix}",
            'sheet_name': sheet_name,
            'column_mappings': final_mappings
        }
        
        print(f"\n✅ Configuration complete!")
        print(f"   Source: {config['name']}")
        print(f"   Mappings: {len(final_mappings)} fields")
        
        # Save configuration
        config_file = self.base_path / f"config_{source_name}.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"   Saved to: {config_file}")
        
        # Generate Python code snippet
        print(f"\n🐍 Python configuration code:")
        print("-" * 40)
        self._generate_python_config(config)
        
        return config
    
    def _generate_python_config(self, config: Dict):
        """Generate Python code for the configuration"""
        
        print(f"'{config['name']}': SourceConfig(")
        print(f"    name='{config['name']}',")
        print(f"    description='{config['description']}',")
        print(f"    folder_path='{config['folder_path']}',")
        print(f"    file_pattern='{config['file_pattern']}',")
        print(f"    sheet_name='{config['sheet_name']}',")
        print("    column_mappings={")
        
        for standard_field, source_column in config['column_mappings'].items():
            print(f"        '{standard_field}': '{source_column}',")
        
        print("    }")
        print("),")

def main():
    """Main function for interactive configuration"""
    tool = SourceConfigurationTool()
    
    print("🛠️  Data Source Configuration Tool")
    print("=" * 50)
    print("This tool helps you configure new data sources for integration.")
    print()
    
    file_path = input("Enter path to data file: ").strip()
    sheet_name = input("Enter sheet name (or press Enter to detect): ").strip()
    
    if not sheet_name:
        sheet_name = None
    
    try:
        config = tool.interactive_mapping_session(file_path, sheet_name)
        print(f"\n🎉 Successfully configured source: {config['name']}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
