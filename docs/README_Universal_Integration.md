# Universal Data Integration System for Drilling Bit Analysis

## Overview

This system provides a seamless way to integrate and analyze drilling bit data from multiple sources (Ulterra, Reed Hycalog, and easily extensible to others). It automatically handles different data formats, units, and column names to create a unified analysis platform.

## 🚀 Key Features

- **Multi-Source Integration**: Automatically merge data from different manufacturers
- **Smart Unit Conversion**: Converts imperial to metric units automatically  
- **Standardized Schema**: Maps different column names to consistent field names
- **Data Quality Validation**: Built-in validation and quality reporting
- **Category-Based Analysis**: Organized field groupings for focused analysis
- **Easy Source Addition**: Interactive tools for adding new data sources
- **Comprehensive Metadata**: Tracks data lineage and source information

## 📁 System Components

### Core Files

1. **`data_mapping_config.py`** - Central configuration defining:
   - Standard field schema (66+ fields)
   - Source-specific column mappings
   - Data categories (well info, bit specs, performance, etc.)
   - Validation rules

2. **`universal_data_integration.py`** - Main integration engine:
   - Discovers and loads source files
   - Applies data transformations
   - Standardizes formats and units
   - Generates quality reports
   - Exports unified datasets

3. **`source_config_tool.py`** - Interactive configuration tool:
   - Analyzes new data files
   - Suggests field mappings
   - Generates configuration code
   - Simplifies adding new sources

### Support Files

4. **`demo_universal_integration.py`** - Comprehensive demonstrations
5. **Legacy scripts** - Your existing analysis tools still work with the unified data

## 🎯 Data Categories

The system organizes data into logical categories:

| Category | Fields | Purpose |
|----------|--------|---------|
| **Well Identification** | well_name, operator, api_number, etc. | Well and operator info |
| **Location** | field, county, coordinates, legal descriptions | Geographic information |
| **Bit Specifications** | manufacturer, size, type, IADC code | Bit technical details |
| **Run Details** | dates, depths, run numbers | Run-specific information |
| **Performance Metrics** | ROP, drilling hours, efficiency | Performance analysis |
| **Drilling Parameters** | WOB, torque, RPM, flow rates | Operating conditions |
| **Dull Grading** | IADC dull codes and reasons | Bit condition assessment |
| **Formation** | formation names and types | Geological context |
| **Metadata** | source files, quality flags | Data lineage tracking |

## 📊 Current Data Sources

### Ulterra Data
- **Files**: 4 Excel files (156mm, 159mm, 171mm, 222mm searches)
- **Records**: 2,452 bit runs
- **Columns**: 205 detailed fields
- **Format**: Metric units, comprehensive drilling parameters

### Reed Hycalog Data  
- **Files**: 1 Excel file (All Montney since 2020)
- **Records**: 18,618 bit runs
- **Columns**: 36 core fields
- **Format**: Imperial units (auto-converted to metric)

### Integrated Dataset
- **Total Records**: 21,070 bit runs
- **Unique Wells**: 4,476 wells
- **Operators**: 167 companies
- **Date Range**: 2012-2025
- **Completion Rates**: 97-100% for key fields

## 🛠️ How to Use

### Basic Integration

```python
from universal_data_integration import DataIntegrationEngine

# Initialize the engine
engine = DataIntegrationEngine()

# Integrate all available sources
integrated_data = engine.integrate_all_sources()

# Save unified dataset
output_path = engine.save_integrated_data()

# Get quality report
quality_report = engine.get_data_quality_report()
```

### Category-Based Analysis

```python
from data_mapping_config import DataMappingConfig

config = DataMappingConfig()

# Get performance metrics for analysis
performance_fields = config.get_category_fields('performance_metrics')
performance_data = integrated_data[performance_fields]

# Analyze bit specifications
bit_fields = config.get_category_fields('bit_specifications')
bit_analysis = integrated_data[bit_fields]
```

### Adding New Data Sources

#### Method 1: Interactive Tool
```bash
python source_config_tool.py
```
Follow the prompts to analyze and configure new files.

#### Method 2: Manual Configuration
Add to `data_mapping_config.py`:

```python
'new_source': SourceConfig(
    name='New Source Name',
    description='Description of the data source',
    folder_path='Input/NewSource',
    file_pattern='*.xlsx',
    sheet_name='Data Sheet',
    column_mappings={
        'well_name': 'Their_Well_Column',
        'operator': 'Their_Operator_Column',
        'bit_size_mm': 'Their_Size_Column',
        # ... more mappings
    }
)
```

## 📈 Analysis Examples

### Performance Comparison by Source
```python
# Compare ROP by manufacturer
rop_by_source = integrated_data.groupby(['data_source', 'bit_manufacturer'])['rop_mhr'].mean()

# TD bit analysis across sources
td_bits = integrated_data[integrated_data['dull_reason'] == 'TD']
td_comparison = td_bits.groupby('data_source').agg({
    'rop_mhr': 'mean',
    'drilling_hours': 'mean',
    'distance_drilled_m': 'sum'
})
```

### Operator Performance Analysis
```python
# Top operators by total footage
operator_performance = integrated_data.groupby('operator').agg({
    'distance_drilled_m': 'sum',
    'rop_mhr': 'mean',
    'well_name': 'nunique'
}).sort_values('distance_drilled_m', ascending=False)
```

### Bit Type Effectiveness
```python
# Best performing bit types by formation
bit_performance = integrated_data.groupby(['bit_type', 'formation'])['rop_mhr'].mean()
```

## 🔧 Data Transformations

The system automatically handles:

### Unit Conversions
- **Depths**: Feet → Meters (×0.3048)
- **Bit Sizes**: Inches → Millimeters (×25.4)  
- **ROP**: ft/hr → m/hr (×0.3048)
- **Pressures**: PSI → KPa (various fields)

### Data Standardization
- **Date Formats**: All dates converted to pandas datetime
- **Numeric Fields**: Consistent numeric types with error handling
- **Text Fields**: Standardized capitalization and encoding
- **Missing Values**: Consistent NaN handling across sources

### Derived Fields
- **Distance Drilled**: Calculated from depth in/out when missing
- **Year Fields**: Extracted from all date fields
- **Bit Size Categories**: Grouped into standard size ranges
- **Performance Ratios**: Efficiency and productivity metrics

## 📋 Data Quality Features

### Validation
- **Required Field Checks**: Ensures critical fields are present
- **Column Mapping Validation**: Verifies source columns exist
- **Data Type Validation**: Checks numeric/date field formats
- **Range Validation**: Flags outliers and impossible values

### Quality Reporting
- **Completion Rates**: Percentage of non-null values per field
- **Date Range Analysis**: Temporal coverage by source
- **Source Distribution**: Record counts by source
- **Uniqueness Metrics**: Unique wells, operators, bit types

### Metadata Tracking
- **Source Information**: Original file names and modification dates
- **Processing History**: Transformation and conversion logs
- **Record Lineage**: Unique identifiers linking back to sources

## 🎯 Benefits for Analysis

### Unified Reporting
- Compare performance across manufacturers
- Analyze trends using consistent metrics
- Generate comprehensive dashboards
- Create standardized KPI reports

### Enhanced Analytics
- Cross-source correlation analysis
- Multi-manufacturer benchmarking
- Comprehensive formation analysis
- Operator performance comparison

### Operational Efficiency
- Automated data integration
- Consistent data quality
- Reduced manual data preparation
- Streamlined reporting workflows

## 🔮 Future Enhancements

### Planned Features
- **Tableau Integration**: Direct .hyper file generation
- **Real-time Updates**: Automated file monitoring and integration
- **Advanced Analytics**: Machine learning models for bit selection
- **Web Dashboard**: Interactive visualization interface

### Easy Extensibility
- **New Sources**: Simple configuration-based addition
- **Custom Fields**: Flexible field mapping system
- **Data Validation**: Configurable business rules
- **Export Formats**: Multiple output format support

## 📞 Support

The system is designed to be self-documenting and user-friendly. Key resources:

1. **Interactive Tools**: Use `source_config_tool.py` for guided setup
2. **Demo Scripts**: Run `demo_universal_integration.py` for examples
3. **Quality Reports**: Built-in validation and completeness metrics
4. **Configuration**: Well-documented mapping files with examples

## 🎉 Getting Started

1. **Run the integration**: `python universal_data_integration.py`
2. **Review the output**: Check the generated Excel file in Output/
3. **Explore categories**: Use the data categories for focused analysis
4. **Add new sources**: Use the interactive configuration tool
5. **Build custom analysis**: Leverage the unified dataset for your specific needs

The system transforms your multi-source drilling data into a powerful, unified analysis platform!
