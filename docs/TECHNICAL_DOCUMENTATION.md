# TECHNICAL DOCUMENTATION

## System Architecture

### Core Components

#### 1. Data Mapping Configuration (`core/data_mapping_config.py`)
Central configuration system that defines:
- **Standard field schema** with data types and requirements
- **Source-specific mappings** for each vendor
- **Validation rules** and data categories
- **Extensible design** for adding new sources

Key Classes:
- `FieldMapping`: Defines standard field properties
- `SourceConfig`: Vendor-specific configuration 
- `DataMappingConfig`: Main configuration manager

#### 2. Universal Data Integration (`core/universal_data_integration.py`)
Main integration engine that:
- **Loads and validates** source data files
- **Applies field mappings** and standardization
- **Creates composite well identifiers**
- **Generates comprehensive reports**

Key Features:
- Automatic file detection and processing
- Data type conversion and validation
- Conflict resolution for duplicate records
- Timestamped output with audit trails

#### 3. Safe GDC License Lookup (`core/gdc_safe_license_lookup.py`)
Conservative Oracle database lookup that:
- **Connects to GDC database** using established credentials
- **Infers province** from longitude coordinates (AB/BC boundary at -120°W)
- **Applies multiple verification criteria** for high-confidence matching
- **Prevents false positives** through strict validation

Verification Criteria:
- Coordinate proximity (≤100m tolerance)
- Spud date verification (≤7 days difference)  
- Legal location pattern matching
- Province verification

#### 4. Interactive Configuration Tool (`core/source_config_tool.py`)
User-friendly interface for:
- **Adding new data sources** without coding
- **Column mapping assistance** with validation
- **Real-time feedback** on mapping completeness
- **Configuration file updates**

## Database Integration

### GDC Oracle Connection
- **Host**: WC-CGY-ORAP01
- **Port**: 1521
- **Service**: PRD1
- **Schema**: GDC.WELL

### Key Database Fields
- `WELL_NUM`: License number (unique by province)
- `WELL_NAME`: Well name for matching
- `PROVINCE_STATE`: Province code (AB/BC)
- `SURFACE_LATITUDE/LONGITUDE`: Surface coordinates
- `SPUD_DATE`: Well spud date for verification

### Safety Measures
- **Read-only access** to prevent data modification
- **Connection pooling** for efficient resource usage
- **Error handling** for database connectivity issues
- **SQL injection prevention** through parameterized queries

## Data Flow

### 1. Data Ingestion
```
Input Files → File Detection → Format Validation → Loading
```

### 2. Standardization  
```
Raw Data → Field Mapping → Data Type Conversion → Validation
```

### 3. Integration
```
Standardized Data → Deduplication → Composite Keys → Merging
```

### 4. License Lookup
```
Missing Licenses → Province Inference → GDC Query → Verification → Updates
```

### 5. Output Generation
```
Integrated Data → Quality Reports → Excel Export → Audit Logs
```

## Error Handling

### File Processing Errors
- **Missing files**: Graceful degradation with warnings
- **Format errors**: Skip problematic rows with logging
- **Permission errors**: Clear error messages with suggestions

### Database Errors
- **Connection failures**: Retry logic with exponential backoff
- **Query timeouts**: Configurable timeout settings
- **Data type mismatches**: Type conversion with fallbacks

### Data Validation Errors
- **Required field missing**: Clear identification and reporting
- **Invalid data types**: Conversion attempts with logging
- **Constraint violations**: Detailed error descriptions

## Performance Considerations

### Memory Management
- **Chunked processing** for large files
- **Efficient data structures** (pandas DataFrames)
- **Memory monitoring** and garbage collection

### Database Optimization
- **Parameterized queries** for performance
- **Result set limiting** to prevent resource exhaustion
- **Connection reuse** to minimize overhead

### Processing Efficiency
- **Vectorized operations** using pandas
- **Parallel processing** where applicable
- **Caching** of frequently accessed data

## Security Features

### Database Security
- **Read-only permissions** on GDC database
- **Parameterized queries** to prevent SQL injection
- **Connection encryption** using Oracle client libraries

### Data Protection
- **No sensitive data storage** in configuration files
- **Audit logging** of all database access
- **Secure file handling** with proper permissions

### Error Information
- **Sanitized error messages** without exposing internal details
- **Logging separation** between debug and production info
- **Secure credential handling**

## Configuration Management

### File Structure
```python
{
    'source_name': {
        'name': 'Display Name',
        'description': 'Source description', 
        'folder_path': 'Input/SourceFolder',
        'file_pattern': '*.xlsx',
        'sheet_name': 'SheetName',
        'skip_rows': 0,
        'column_mappings': {
            'standard_field': 'Source Column Name'
        }
    }
}
```

### Adding New Sources
1. **Use interactive tool** (`source_config_tool.py`)
2. **Define column mappings** for required fields
3. **Validate configuration** with test data
4. **Update documentation** with source details

## Quality Assurance

### Data Validation
- **Required field checking** for all sources
- **Data type validation** with conversion
- **Range checking** for numeric fields
- **Date format standardization**

### Integration Testing
- **Round-trip testing** of all transformations
- **Cross-source validation** for consistency
- **Performance benchmarking** with large datasets
- **Error condition testing** for robustness

### Audit Capabilities
- **Complete transformation tracking** from source to output
- **Change detection** and versioning
- **Quality metrics** reporting
- **Exception logging** with detailed context

## Maintenance and Support

### Regular Maintenance
- **Database connection testing** 
- **File system cleanup** of old outputs
- **Configuration validation** for changes
- **Performance monitoring** and optimization

### Troubleshooting
- **Comprehensive logging** at multiple levels
- **Error categorization** for quick diagnosis
- **Recovery procedures** for common issues
- **Debug mode** for detailed analysis

### Updates and Enhancements
- **Version control** with Git
- **Change documentation** in commit messages
- **Backward compatibility** preservation
- **Testing protocols** for new features
