"""
Universal Data Integration Engine
Seamlessly merges and standardizes drilling bit data from multiple sources.

This engine uses the data mapping configuration to automatically load,
transform, and integrate data from various sources into a unified format.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import warnings
from data_mapping_config import DataMappingConfig, SourceConfig

class DataIntegrationEngine:
    """Main engine for loading and integrating multi-source drilling data"""
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).parent
        self.config = DataMappingConfig()
        self.loaded_data = {}
        self.integrated_data = None
        
    def discover_sources(self) -> Dict[str, List[Path]]:
        """Discover available data files for each configured source"""
        discovered = {}
        
        for source_name, source_config in self.config.data_sources.items():
            source_path = self.base_path / source_config.folder_path
            if source_path.exists():
                files = list(source_path.glob(source_config.file_pattern))
                discovered[source_name] = files
                print(f"🔍 {source_name}: Found {len(files)} files")
                for file in files:
                    print(f"   - {file.name}")
            else:
                discovered[source_name] = []
                print(f"⚠️  {source_name}: Folder not found - {source_path}")
        
        return discovered
    
    def load_source_data(self, source_name: str, file_paths: Optional[List[Path]] = None) -> pd.DataFrame:
        """Load and combine data from a specific source"""
        source_config = self.config.get_source_config(source_name)
        if not source_config:
            raise ValueError(f"Unknown source: {source_name}")
        
        if file_paths is None:
            # Auto-discover files
            discovered = self.discover_sources()
            file_paths = discovered.get(source_name, [])
        
        if not file_paths:
            print(f"❌ No files found for source: {source_name}")
            return pd.DataFrame()
        
        print(f"📊 Loading {source_name} data from {len(file_paths)} files...")
        
        dataframes = []
        for file_path in file_paths:
            try:
                # Determine sheet name
                sheet_name = source_config.sheet_name
                if sheet_name is None:
                    # Use first sheet
                    excel_file = pd.ExcelFile(file_path)
                    sheet_name = excel_file.sheet_names[0]
                
                # Load data
                df = pd.read_excel(
                    file_path, 
                    sheet_name=sheet_name,
                    skiprows=source_config.skip_rows
                )
                
                # Add metadata
                df['_source_file'] = file_path.name
                df['_file_modified'] = datetime.fromtimestamp(file_path.stat().st_mtime)
                df['_data_source'] = source_name
                df['_record_id'] = f"{source_name}_{file_path.stem}_{df.index}"
                
                dataframes.append(df)
                print(f"   ✅ {file_path.name}: {len(df)} rows")
                
            except Exception as e:
                print(f"   ❌ {file_path.name}: Error - {str(e)}")
                continue
        
        if not dataframes:
            return pd.DataFrame()
        
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
        print(f"   📈 Combined: {len(combined_df)} total rows")
        
        # Validate column mapping
        validation = self.config.validate_mapping(source_name, combined_df.columns.tolist())
        if not validation['valid']:
            print(f"   ⚠️  Mapping validation warnings for {source_name}:")
            if validation.get('missing_source_columns'):
                print(f"      Missing columns: {validation['missing_source_columns']}")
            if validation.get('missing_required_fields'):
                print(f"      Missing required fields: {validation['missing_required_fields']}")
        
        self.loaded_data[source_name] = combined_df
        return combined_df
    
    def standardize_data(self, source_name: str, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Transform source data to standardized format"""
        if df is None:
            df = self.loaded_data.get(source_name)
            if df is None:
                raise ValueError(f"No data loaded for source: {source_name}")
        
        source_config = self.config.get_source_config(source_name)
        if not source_config or not source_config.column_mappings:
            raise ValueError(f"No column mappings defined for source: {source_name}")
        
        print(f"🔄 Standardizing {source_name} data...")
        
        # Create standardized dataframe
        standardized_df = pd.DataFrame()
        
        # Map columns according to configuration
        for standard_field, source_column in source_config.column_mappings.items():
            if source_column in df.columns:
                standardized_df[standard_field] = df[source_column].copy()
            else:
                # Create empty column for missing fields
                standardized_df[standard_field] = np.nan
                print(f"   ⚠️  Missing column '{source_column}' for field '{standard_field}'")
        
        # Add metadata fields
        standardized_df['data_source'] = df['_data_source']
        standardized_df['source_file'] = df['_source_file']
        standardized_df['file_modified_date'] = df['_file_modified']
        standardized_df['record_id'] = df['_record_id']
        
        # Apply data type conversions and cleaning
        standardized_df = self._apply_data_conversions(standardized_df, source_name)
        
        print(f"   ✅ Standardized: {len(standardized_df)} rows, {len(standardized_df.columns)} columns")
        return standardized_df
    
    def _apply_data_conversions(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Apply data type conversions and unit standardizations"""
        df = df.copy()
        
        # Date conversions
        date_fields = ['run_date', 'spud_date', 'td_date', 'file_modified_date']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        # Numeric conversions
        numeric_fields = [
            'bit_size_mm', 'run_number', 'depth_in_m', 'depth_out_m', 
            'distance_drilled_m', 'total_depth_m', 'drilling_hours', 
            'on_bottom_hours', 'rop_mhr', 'latitude', 'longitude'
        ]
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Source-specific conversions
        if source_name == 'reed':
            # Reed data often comes in imperial units - convert to metric
            df = self._convert_reed_units(df)
        elif source_name == 'ulterra':
            # Ulterra data is typically already in metric
            pass
        
        return df
    
    def _convert_reed_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert Reed data from imperial to metric units"""
        df = df.copy()
        
        # Convert bit size from inches to mm (if needed)
        if 'bit_size_mm' in df.columns:
            # Check if values look like inches (< 50) vs mm (> 50)
            sample_values = df['bit_size_mm'].dropna()
            if len(sample_values) > 0 and sample_values.mean() < 50:
                print("   🔄 Converting bit size from inches to mm")
                df['bit_size_mm'] = df['bit_size_mm'] * 25.4
        
        # Convert depths from feet to meters (if needed)
        depth_fields = ['depth_in_m', 'depth_out_m', 'distance_drilled_m']
        for field in depth_fields:
            if field in df.columns:
                sample_values = df[field].dropna()
                if len(sample_values) > 0 and sample_values.mean() > 1000:  # Likely in feet
                    print(f"   🔄 Converting {field} from feet to meters")
                    df[field] = df[field] * 0.3048
        
        # Convert ROP from ft/hr to m/hr (if needed)
        if 'rop_mhr' in df.columns:
            sample_values = df['rop_mhr'].dropna()
            if len(sample_values) > 0 and sample_values.mean() > 10:  # Likely in ft/hr
                print("   🔄 Converting ROP from ft/hr to m/hr")
                df['rop_mhr'] = df['rop_mhr'] * 0.3048
        
        return df
    
    def integrate_all_sources(self, sources: Optional[List[str]] = None) -> pd.DataFrame:
        """Load and integrate data from all or specified sources"""
        if sources is None:
            sources = list(self.config.data_sources.keys())
        
        print(f"🚀 Starting integration of sources: {sources}")
        
        standardized_dataframes = []
        
        for source_name in sources:
            try:
                # Load source data
                raw_df = self.load_source_data(source_name)
                if raw_df.empty:
                    print(f"   ⚠️  Skipping {source_name} - no data loaded")
                    continue
                
                # Standardize data
                standardized_df = self.standardize_data(source_name, raw_df)
                standardized_dataframes.append(standardized_df)
                
            except Exception as e:
                print(f"   ❌ Error processing {source_name}: {str(e)}")
                continue
        
        if not standardized_dataframes:
            print("❌ No data successfully integrated")
            return pd.DataFrame()
        
        # Combine all standardized data
        print("🔗 Combining standardized data...")
        integrated_df = pd.concat(standardized_dataframes, ignore_index=True, sort=False)
        
        # Add derived fields
        integrated_df = self._add_derived_fields(integrated_df)
        
        # Sort by source and date
        sort_columns = ['data_source', 'spud_date', 'run_date']
        available_sort_columns = [col for col in sort_columns if col in integrated_df.columns]
        if available_sort_columns:
            integrated_df = integrated_df.sort_values(available_sort_columns)
        
        self.integrated_data = integrated_df
        
        print(f"✅ Integration complete!")
        print(f"   📊 Total records: {len(integrated_df)}")
        print(f"   📈 Total columns: {len(integrated_df.columns)}")
        print(f"   🏭 Data sources: {integrated_df['data_source'].value_counts().to_dict()}")
        
        return integrated_df
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add calculated and derived fields"""
        df = df.copy()
        
        # Calculate distance drilled if missing
        if 'distance_drilled_m' not in df.columns or df['distance_drilled_m'].isna().all():
            if 'depth_in_m' in df.columns and 'depth_out_m' in df.columns:
                df['distance_drilled_m'] = df['depth_out_m'] - df['depth_in_m']
        
        # Extract year from dates
        for date_field in ['spud_date', 'run_date', 'td_date']:
            if date_field in df.columns:
                year_field = date_field.replace('_date', '_year')
                df[year_field] = df[date_field].dt.year
        
        # Create bit size categories
        if 'bit_size_mm' in df.columns:
            df['bit_size_category'] = pd.cut(
                df['bit_size_mm'],
                bins=[0, 100, 150, 200, 250, 300, 400, float('inf')],
                labels=['<100mm', '100-150mm', '150-200mm', '200-250mm', 
                       '250-300mm', '300-400mm', '>400mm'],
                include_lowest=True
            )
        
        # Performance efficiency metrics
        if 'rop_mhr' in df.columns and 'drilling_hours' in df.columns:
            df['total_penetration'] = df['rop_mhr'] * df['drilling_hours']
        
        # Add composite well identifier for accurate well counting
        def create_composite_well_id(row, idx):
            # Priority 1: Use API/UWI if available (most standardized)
            if pd.notna(row.get('api_number')) and str(row.get('api_number', '')).strip():
                return f"API_{row['api_number']}"
            
            # Priority 2: Use License number if available (Canadian wells)
            elif pd.notna(row.get('license_number')) and str(row.get('license_number', '')).strip():
                return f"LIC_{row['license_number']}"
            
            # Priority 3: Use well name as fallback
            elif pd.notna(row.get('well_name')) and str(row.get('well_name', '')).strip():
                return f"NAME_{row['well_name']}"
            
            # Last resort: create unique ID
            else:
                return f"UNK_{idx}"
        
        # Create composite well identifier
        df['composite_well_id'] = [create_composite_well_id(row, idx) for idx, row in df.iterrows()]
        
        return df
    
    def save_integrated_data(self, filename: Optional[str] = None, format: str = 'excel') -> Path:
        """Save integrated data to file"""
        if self.integrated_data is None:
            raise ValueError("No integrated data available. Run integrate_all_sources() first.")
        
        output_folder = self.base_path / "Output"
        output_folder.mkdir(exist_ok=True)
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"Integrated_BitData_{timestamp}"
        
        if format.lower() == 'excel':
            output_path = output_folder / f"{filename}.xlsx"
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Main integrated data
                self.integrated_data.to_excel(writer, sheet_name='Integrated_Data', index=False)
                
                # Summary by source
                source_summary = self._create_source_summary()
                source_summary.to_excel(writer, sheet_name='Source_Summary', index=False)
                
                # Field mappings reference
                mapping_ref = self._create_mapping_reference()
                mapping_ref.to_excel(writer, sheet_name='Field_Mappings', index=False)
                
        elif format.lower() == 'csv':
            output_path = output_folder / f"{filename}.csv"
            self.integrated_data.to_csv(output_path, index=False)
        
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        print(f"💾 Integrated data saved: {output_path}")
        return output_path
    
    def _create_source_summary(self) -> pd.DataFrame:
        """Create summary statistics by data source"""
        if self.integrated_data is None:
            return pd.DataFrame()
        
        summary_data = []
        
        for source in self.integrated_data['data_source'].unique():
            source_data = self.integrated_data[self.integrated_data['data_source'] == source]
            
            summary = {
                'data_source': source,
                'total_records': len(source_data),
                'unique_wells': source_data['composite_well_id'].nunique(),
                'unique_operators': source_data['operator'].nunique(),
                'unique_bit_types': source_data['bit_type'].nunique(),
                'date_range_start': source_data['spud_date'].min(),
                'date_range_end': source_data['spud_date'].max(),
                'avg_rop': source_data['rop_mhr'].mean(),
                'avg_drilling_hours': source_data['drilling_hours'].mean(),
                'total_meters_drilled': source_data['distance_drilled_m'].sum(),
            }
            summary_data.append(summary)
        
        return pd.DataFrame(summary_data)
    
    def _create_mapping_reference(self) -> pd.DataFrame:
        """Create reference table of field mappings"""
        mapping_data = []
        
        for source_name, source_config in self.config.data_sources.items():
            if source_config.column_mappings:
                for standard_field, source_column in source_config.column_mappings.items():
                    field_info = self.config.standard_fields.get(standard_field)
                    mapping_data.append({
                        'source': source_name,
                        'standard_field': standard_field,
                        'source_column': source_column,
                        'description': field_info.description if field_info else '',
                        'data_type': field_info.data_type if field_info else '',
                        'required': field_info.required if field_info else False
                    })
        
        return pd.DataFrame(mapping_data)
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report"""
        if self.integrated_data is None:
            return {'error': 'No integrated data available'}
        
        df = self.integrated_data
        
        # Calculate completion rates for key fields
        key_fields = [
            'well_name', 'operator', 'bit_manufacturer', 'bit_size_mm',
            'bit_type', 'depth_in_m', 'depth_out_m', 'rop_mhr'
        ]
        
        completion_rates = {}
        for field in key_fields:
            if field in df.columns:
                completion_rates[field] = (1 - df[field].isna().mean()) * 100
        
        # Data range analysis
        date_fields = ['spud_date', 'run_date', 'td_date']
        date_ranges = {}
        for field in date_fields:
            if field in df.columns:
                date_ranges[field] = {
                    'min': df[field].min(),
                    'max': df[field].max(),
                    'count': df[field].notna().sum()
                }
        
        # Source distribution
        source_dist = df['data_source'].value_counts().to_dict()
        
        return {
            'total_records': len(df),
            'total_fields': len(df.columns),
            'completion_rates': completion_rates,
            'date_ranges': date_ranges,
            'source_distribution': source_dist,
            'unique_operators': df['operator'].nunique(),
            'unique_wells': df['composite_well_id'].nunique(),
            'well_id_breakdown': {
                'api_identified': (df['composite_well_id'].str.startswith('API_')).sum(),
                'license_identified': (df['composite_well_id'].str.startswith('LIC_')).sum(),
                'name_identified': (df['composite_well_id'].str.startswith('NAME_')).sum(),
                'unknown_identified': (df['composite_well_id'].str.startswith('UNK_')).sum(),
            },
            'bit_manufacturers': df['bit_manufacturer'].value_counts().head(10).to_dict(),
            'avg_rop': df['rop_mhr'].mean(),
            'total_distance_drilled': df['distance_drilled_m'].sum()
        }

def main():
    """Main function to demonstrate the integration engine"""
    print("🔧 Universal Data Integration Engine")
    print("=" * 50)
    
    # Initialize engine
    engine = DataIntegrationEngine()
    
    # Discover available sources
    print("\n📂 Discovering data sources...")
    discovered = engine.discover_sources()
    
    if not any(discovered.values()):
        print("❌ No data files found!")
        return
    
    # Integrate all available sources
    print("\n🚀 Integrating all sources...")
    integrated_df = engine.integrate_all_sources()
    
    if integrated_df.empty:
        print("❌ No data was successfully integrated!")
        return
    
    # Save integrated data
    print("\n💾 Saving integrated data...")
    output_path = engine.save_integrated_data()
    
    # Print quality report
    print("\n📊 Data Quality Report:")
    print("-" * 30)
    quality_report = engine.get_data_quality_report()
    
    print(f"Total Records: {quality_report['total_records']:,}")
    print(f"Total Fields: {quality_report['total_fields']}")
    print(f"Unique Wells: {quality_report['unique_wells']:,}")
    print(f"Unique Operators: {quality_report['unique_operators']}")
    
    print("\nSource Distribution:")
    for source, count in quality_report['source_distribution'].items():
        print(f"  {source}: {count:,} records")
    
    print("\nField Completion Rates:")
    for field, rate in quality_report['completion_rates'].items():
        print(f"  {field}: {rate:.1f}%")
    
    print(f"\n✅ Integration complete! Output saved to: {output_path}")

if __name__ == "__main__":
    main()
