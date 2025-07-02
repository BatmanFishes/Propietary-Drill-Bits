"""
Data Source Mapping Configuration
Defines how to map and integrate data from multiple drilling bit data sources.

This configuration allows for seamless integration of different data sources
by defining standard field names and their mappings from source-specific columns.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path

@dataclass
class FieldMapping:
    """Defines how a standard field maps to source-specific columns"""
    standard_name: str
    description: str
    data_type: str  # 'string', 'numeric', 'date', 'boolean'
    required: bool = False
    
@dataclass
class SourceConfig:
    """Configuration for a specific data source"""
    name: str
    description: str
    folder_path: str
    file_pattern: str  # glob pattern to match files
    sheet_name: Optional[str] = None  # None means use first sheet or detect
    skip_rows: int = 0
    column_mappings: Optional[Dict[str, str]] = None  # standard_name -> source_column_name
    
class DataMappingConfig:
    """Central configuration for data source mapping and integration"""
    
    def __init__(self):
        self.standard_fields = self._define_standard_fields()
        self.data_sources = self._define_data_sources()
        self.data_categories = self._define_data_categories()
    
    def _define_standard_fields(self) -> Dict[str, FieldMapping]:
        """Define the standardized field schema"""
        return {
            # === WELL IDENTIFICATION ===
            'well_name': FieldMapping('well_name', 'Well name or identifier', 'string', required=True),
            'well_number': FieldMapping('well_number', 'Well number', 'string'),
            'uwi_number': FieldMapping('uwi_number', 'Unique Well Identifier (UWI)', 'string'),
            'license_number': FieldMapping('license_number', 'Drilling license number', 'string'),
            'operator': FieldMapping('operator', 'Operating company', 'string', required=True),
            'contractor': FieldMapping('contractor', 'Drilling contractor', 'string'),
            'rig_name': FieldMapping('rig_name', 'Rig name or number', 'string'),
            
            # === LOCATION INFORMATION ===
            'field': FieldMapping('field', 'Field name', 'string'),
            'county': FieldMapping('county', 'County or area', 'string'),
            'state_province': FieldMapping('state_province', 'State or province', 'string'),
            'country': FieldMapping('country', 'Country', 'string'),
            'latitude': FieldMapping('latitude', 'Latitude coordinate', 'numeric'),
            'longitude': FieldMapping('longitude', 'Longitude coordinate', 'numeric'),
            'lsd': FieldMapping('lsd', 'Legal subdivision', 'string'),
            'section': FieldMapping('section', 'Section', 'string'),
            'township': FieldMapping('township', 'Township', 'string'),
            'range': FieldMapping('range', 'Range', 'string'),
            
            # === BIT INFORMATION ===
            'bit_manufacturer': FieldMapping('bit_manufacturer', 'Bit manufacturer', 'string', required=True),
            'bit_serial_number': FieldMapping('bit_serial_number', 'Bit serial number', 'string'),
            'bit_size_mm': FieldMapping('bit_size_mm', 'Bit size in millimeters', 'numeric', required=True),
            'bit_type': FieldMapping('bit_type', 'Bit type or model', 'string', required=True),
            'iadc_code': FieldMapping('iadc_code', 'IADC classification code', 'string'),
            'bit_style': FieldMapping('bit_style', 'Bit style description', 'string'),
            'blade_count': FieldMapping('blade_count', 'Number of blades', 'numeric'),
            'cutter_size': FieldMapping('cutter_size', 'Cutter size', 'string'),
            'tfa': FieldMapping('tfa', 'Total flow area', 'numeric'),
            
            # === RUN INFORMATION ===
            'run_number': FieldMapping('run_number', 'Run sequence number', 'numeric'),
            'run_date': FieldMapping('run_date', 'Run start date', 'date'),
            'spud_date': FieldMapping('spud_date', 'Well spud date', 'date'),
            'td_date': FieldMapping('td_date', 'Total depth date', 'date'),
            'depth_in_m': FieldMapping('depth_in_m', 'Depth in (meters)', 'numeric', required=True),
            'depth_out_m': FieldMapping('depth_out_m', 'Depth out (meters)', 'numeric', required=True),
            'distance_drilled_m': FieldMapping('distance_drilled_m', 'Distance drilled (meters)', 'numeric'),
            'total_depth_m': FieldMapping('total_depth_m', 'Total well depth (meters)', 'numeric'),
            
            # === PERFORMANCE METRICS ===
            'drilling_hours': FieldMapping('drilling_hours', 'Total drilling hours', 'numeric'),
            'on_bottom_hours': FieldMapping('on_bottom_hours', 'On bottom hours', 'numeric'),
            'rop_mhr': FieldMapping('rop_mhr', 'Rate of penetration (m/hr)', 'numeric'),
            'on_bottom_rop_mhr': FieldMapping('on_bottom_rop_mhr', 'On bottom ROP (m/hr)', 'numeric'),
            'rotating_rop_mhr': FieldMapping('rotating_rop_mhr', 'Rotating ROP (m/hr)', 'numeric'),
            'sliding_rop_mhr': FieldMapping('sliding_rop_mhr', 'Sliding ROP (m/hr)', 'numeric'),
            'sliding_percent': FieldMapping('sliding_percent', 'Sliding percentage', 'numeric'),
            
            # === DRILLING PARAMETERS ===
            'wob_low_dan': FieldMapping('wob_low_dan', 'Weight on bit low (daN)', 'numeric'),
            'wob_high_dan': FieldMapping('wob_high_dan', 'Weight on bit high (daN)', 'numeric'),
            'torque_low_ftlb': FieldMapping('torque_low_ftlb', 'Torque low (ft-lb)', 'numeric'),
            'torque_high_ftlb': FieldMapping('torque_high_ftlb', 'Torque high (ft-lb)', 'numeric'),
            'rpm_low': FieldMapping('rpm_low', 'RPM low', 'numeric'),
            'rpm_high': FieldMapping('rpm_high', 'RPM high', 'numeric'),
            'flow_low_gpm': FieldMapping('flow_low_gpm', 'Flow rate low (gpm)', 'numeric'),
            'flow_high_gpm': FieldMapping('flow_high_gpm', 'Flow rate high (gpm)', 'numeric'),
            
            # === DULL GRADING (IADC) ===
            'dull_inner_row': FieldMapping('dull_inner_row', 'Dull grade inner row', 'string'),
            'dull_outer_row': FieldMapping('dull_outer_row', 'Dull grade outer row', 'string'),
            'dull_location': FieldMapping('dull_location', 'Dull location code', 'string'),
            'dull_bearing_seals': FieldMapping('dull_bearing_seals', 'Bearing/seals condition', 'string'),
            'dull_gauge': FieldMapping('dull_gauge', 'Gauge condition', 'string'),
            'dull_reason': FieldMapping('dull_reason', 'Reason for pulling bit', 'string'),
            'dull_characteristics': FieldMapping('dull_characteristics', 'Dull characteristics', 'string'),
            
            # === FORMATION ===
            'formation': FieldMapping('formation', 'Formation drilled', 'string'),
            'td_formation': FieldMapping('td_formation', 'Total depth formation', 'string'),
            
            # === METADATA ===
            'data_source': FieldMapping('data_source', 'Source of the data', 'string', required=True),
            'source_file': FieldMapping('source_file', 'Source file name', 'string'),
            'file_modified_date': FieldMapping('file_modified_date', 'File modification date', 'date'),
            'record_id': FieldMapping('record_id', 'Unique record identifier', 'string'),
        }
    
    def _define_data_sources(self) -> Dict[str, SourceConfig]:
        """Define configuration for each data source"""
        return {
            'ulterra': SourceConfig(
                name='Ulterra',
                description='Ulterra bit performance data',
                folder_path='Input/Ulterra',
                file_pattern='*.xlsx',
                sheet_name='Bit Runs Export',
                skip_rows=1,  # Skip the first row (category headers) to get to actual column names
                column_mappings={
                    'well_name': 'WellName',
                    'well_number': 'WellNumber',
                    'license_number': 'APINumber',  # Ulterra's "API numbers" are actually license numbers
                    'operator': 'OperatorName',
                    'contractor': 'ContractorName',
                    'rig_name': 'RigNumber',
                    'field': 'Field',
                    # Note: County, StateProvince, Country not available in new format
                    'latitude': 'Latitude',
                    'longitude': 'Longitude',
                    'section': 'SEC',  # Updated from 'Section' to 'SEC'
                    'township': 'TWP',  # Updated from 'TownShip' to 'TWP'
                    'range': 'Rge',     # Updated from 'Range' to 'Rge'
                    'lsd': 'LSD',       # New field available
                    'bit_manufacturer': 'BitMfgr',
                    'bit_serial_number': 'SerialNo',
                    'bit_size_mm': 'BitSize (mm)',
                    'bit_type': 'BitType',
                    'iadc_code': 'IADC',
                    'bit_style': 'BitStyle',
                    'blade_count': 'BladeCount',
                    'cutter_size': 'CutterSize',
                    'tfa': 'TFA (mm²)',
                    # Note: RunNumber not available in new format
                    'run_date': 'RunDate',
                    'spud_date': 'SpudDate',
                    # Note: TotalDepthDate not available in new format
                    'depth_in_m': 'Depth In (m)',       # Updated mapping
                    'depth_out_m': 'Depth Out (m)',     # Updated mapping
                    'distance_drilled_m': 'Depth Drilled (m)',  # Updated mapping
                    # Note: TotalDepth not available in new format
                    'drilling_hours': 'Drilling Hours',  # Updated mapping
                    # Note: OnBottomHours not available in new format
                    'rop_mhr': 'ROP (m/hr)',
                    # Note: Specific ROP breakdowns not available in new format
                    'wob_low_dan': 'WOB_Low (daN)',
                    'wob_high_dan': 'WOB_High (daN)',
                    # Note: Torque not available in new format
                    'rpm_low': 'SurfaceRPM_Low',
                    'rpm_high': 'SurfaceRPM_High',
                    'flow_low_gpm': 'Flow_Low (gpm)',
                    'flow_high_gpm': 'Flow_High (gpm)',
                    'dull_inner_row': 'Inner',          # Updated mapping
                    'dull_outer_row': 'Outer',          # Updated mapping
                    'dull_location': 'Location',        # Updated mapping
                    'dull_gauge': 'Gauge',              # Updated mapping
                    'dull_reason': 'Reason Pulled',     # Updated mapping
                    # Note: DullGrade_DullChar not available, using 'Dull' instead
                    'dull_characteristics': 'Dull',     # Updated mapping
                    'td_formation': 'TDFormation',
                }
            ),
            
            'reed': SourceConfig(
                name='Reed Hycalog',
                description='Reed Hycalog bit performance data',
                folder_path='Input/Reed',
                file_pattern='*.xlsx',
                sheet_name='All+Montney+since+2020',
                column_mappings={
                    'well_name': 'Official Well Name',
                    'license_number': 'Lic #',  # Canadian drilling license
                    'uwi_number': 'API/UWI',    # 16-digit Canadian UWI format
                    'operator': 'Operator',
                    'contractor': 'Rig Contractor',
                    'rig_name': 'Rig Name',
                    'field': 'Field',
                    'lsd': 'LSD',
                    'section': 'Sect',
                    'township': 'TWN',
                    'range': 'RNG',
                    'bit_manufacturer': 'Bit Mfg',
                    'bit_serial_number': 'Bit Serial Number',
                    'bit_size_mm': 'Bit Size',  # Note: may need conversion from inches
                    'bit_type': 'Bit Type',
                    'tfa': 'Bit TFA',
                    'run_number': 'Run Seq #',
                    'spud_date': 'Spud',
                    'td_date': 'TD Date',
                    'depth_in_m': 'Depth In',  # Note: may need conversion from feet
                    'depth_out_m': 'Depth Out',  # Note: may need conversion from feet
                    'distance_drilled_m': 'Distance',  # Note: may need conversion from feet
                    'drilling_hours': 'Hrs',
                    'rop_mhr': 'ROP',  # Note: may need conversion from ft/hr
                    'dull_inner_row': 'I',
                    'dull_outer_row': 'O',
                    'dull_location': 'LOC',
                    'dull_bearing_seals': 'B',
                    'dull_gauge': 'G',
                    'dull_reason': 'RP',
                }
            ),
        }
    
    def _define_data_categories(self) -> Dict[str, List[str]]:
        """Define logical groupings of fields for analysis and reporting"""
        return {
            'well_identification': [
                'well_name', 'well_number', 'uwi_number', 'license_number',
                'operator', 'contractor', 'rig_name'
            ],
            'location': [
                'field', 'county', 'state_province', 'country',
                'latitude', 'longitude', 'lsd', 'section', 'township', 'range'
            ],
            'bit_specifications': [
                'bit_manufacturer', 'bit_serial_number', 'bit_size_mm',
                'bit_type', 'iadc_code', 'bit_style', 'blade_count',
                'cutter_size', 'tfa'
            ],
            'run_details': [
                'run_number', 'run_date', 'spud_date', 'td_date',
                'depth_in_m', 'depth_out_m', 'distance_drilled_m', 'total_depth_m'
            ],
            'performance_metrics': [
                'drilling_hours', 'on_bottom_hours', 'rop_mhr',
                'on_bottom_rop_mhr', 'rotating_rop_mhr', 'sliding_rop_mhr',
                'sliding_percent'
            ],
            'drilling_parameters': [
                'wob_low_dan', 'wob_high_dan', 'torque_low_ftlb',
                'torque_high_ftlb', 'rpm_low', 'rpm_high',
                'flow_low_gpm', 'flow_high_gpm'
            ],
            'dull_grading': [
                'dull_inner_row', 'dull_outer_row', 'dull_location',
                'dull_bearing_seals', 'dull_gauge', 'dull_reason',
                'dull_characteristics'
            ],
            'formation': [
                'formation', 'td_formation'
            ],
            'metadata': [
                'data_source', 'source_file', 'file_modified_date', 'record_id'
            ]
        }
    
    def get_source_config(self, source_name: str) -> Optional[SourceConfig]:
        """Get configuration for a specific source"""
        return self.data_sources.get(source_name)
    
    def get_category_fields(self, category: str) -> List[str]:
        """Get all fields in a specific category"""
        return self.data_categories.get(category, [])
    
    def get_all_categories(self) -> List[str]:
        """Get list of all available categories"""
        return list(self.data_categories.keys())
    
    def get_required_fields(self) -> List[str]:
        """Get list of required fields"""
        return [name for name, field in self.standard_fields.items() if field.required]
    
    def validate_mapping(self, source_name: str, df_columns: List[str]) -> Dict[str, Any]:
        """Validate that a source's column mapping is complete and accurate"""
        source_config = self.get_source_config(source_name)
        if not source_config:
            return {'valid': False, 'error': f'Unknown source: {source_name}'}
        
        validation_result = {
            'valid': True,
            'mapped_fields': [],
            'missing_source_columns': [],
            'missing_required_fields': [],
            'warnings': []
        }
        
        # Check which fields can be mapped
        if source_config.column_mappings:
            for standard_field, source_column in source_config.column_mappings.items():
                if source_column in df_columns:
                    validation_result['mapped_fields'].append(standard_field)
                else:
                    validation_result['missing_source_columns'].append(source_column)
        
        # Check for missing required fields
        required_fields = self.get_required_fields()
        for field in required_fields:
            if field not in validation_result['mapped_fields']:
                validation_result['missing_required_fields'].append(field)
        
        # Set validation status
        if validation_result['missing_required_fields']:
            validation_result['valid'] = False
            validation_result['error'] = f"Missing required fields: {validation_result['missing_required_fields']}"
        
        return validation_result
