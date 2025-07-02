"""
Analyze Field Names in Bit Data vs GDC Well Table
Compare field names to understand matching opportunities
"""

import pandas as pd
import numpy as np
from pathlib import Path
import oracledb
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_gdc():
    """Connect to GDC Oracle database"""
    try:
        connection_params = {
            'host': 'WC-CGY-ORAP01',
            'port': 1521,
            'service': 'PRD1',
            'user': 'synergyro',
            'password': 'synergyro'
        }
        
        dsn = f"{connection_params['host']}:{connection_params['port']}/{connection_params['service']}"
        connection = oracledb.connect(
            user=connection_params['user'],
            password=connection_params['password'],
            dsn=dsn
        )
        
        logger.info("✅ Connected to Oracle GDC database")
        return connection
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Oracle database: {e}")
        return None

def analyze_bit_data_fields():
    """Analyze field names in the integrated bit data"""
    
    # Find the most recent integrated file
    output_dir = Path("Output")
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    if not integrated_files:
        logger.error("No integrated data files found")
        return None
    
    latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"📊 Loading bit data from: {latest_file.name}")
    
    # Load integrated data
    df = pd.read_excel(latest_file)
    
    # Filter to records missing license numbers
    missing_license = df[
        (df['data_source'] == 'ulterra') &
        ((df['license_number'].isna()) | (df['license_number'] == ''))
    ].copy()
    
    print(f"\n📊 BIT DATA ANALYSIS:")
    print("=" * 50)
    print(f"Total records missing license: {len(missing_license):,}")
    
    # Analyze field values
    if 'field' in missing_license.columns:
        field_counts = missing_license['field'].value_counts().head(20)
        print(f"\n🏞️  TOP FIELD NAMES IN BIT DATA:")
        print("-" * 40)
        for field, count in field_counts.items():
            if field is not None and str(field).strip() != '':
                print(f"  {field:<30} ({count:,} records)")
        
        unique_fields = missing_license['field'].dropna().unique()
        print(f"\nTotal unique fields: {len(unique_fields):,}")
        
        return unique_fields
    else:
        print("❌ No 'field' column found in bit data")
        return None

def analyze_gdc_fields(connection, bit_fields=None):
    """Analyze field names in the GDC well table"""
    
    try:
        # Get GDC table structure
        structure_query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = 'WELL' 
        AND OWNER = 'GDC'
        ORDER BY COLUMN_ID
        """
        
        structure_df = pd.read_sql(structure_query, connection)
        
        print(f"\n🏗️  GDC.WELL TABLE STRUCTURE:")
        print("=" * 50)
        
        # Look for field-related columns
        field_related_cols = []
        for _, row in structure_df.iterrows():
            col_name = row['COLUMN_NAME']
            if any(keyword in col_name.upper() for keyword in ['FIELD', 'AREA', 'POOL', 'FORMATION']):
                field_related_cols.append(col_name)
                print(f"  {col_name:<25} {row['DATA_TYPE']:<12} ({row['DATA_LENGTH']})")
        
        if not field_related_cols:
            print("❌ No obvious field-related columns found")
            return None
        
        # Sample data from field-related columns
        print(f"\n📋 SAMPLE DATA FROM FIELD-RELATED COLUMNS:")
        print("-" * 50)
        
        for col in field_related_cols[:3]:  # Limit to first 3 to avoid too much output
            sample_query = f"""
            SELECT DISTINCT {col} as FIELD_VALUE, COUNT(*) as COUNT
            FROM GDC.WELL 
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY COUNT(*) DESC
            FETCH FIRST 15 ROWS ONLY
            """
            
            try:
                sample_df = pd.read_sql(sample_query, connection)
                print(f"\n  🔹 {col}:")
                for _, row in sample_df.iterrows():
                    field_val = str(row['FIELD_VALUE'])[:40]  # Truncate long values
                    print(f"    {field_val:<35} ({row['COUNT']:,} wells)")
            
            except Exception as e:
                print(f"    ❌ Error sampling {col}: {e}")
        
        # If we have bit fields, try to find matches
        if bit_fields is not None and len(field_related_cols) > 0:
            print(f"\n🔍 CHECKING FOR FIELD NAME MATCHES:")
            print("-" * 50)
            
            for gdc_col in field_related_cols[:2]:  # Check top 2 columns
                print(f"\n  Checking {gdc_col}...")
                
                try:
                    # Get all unique values from this GDC column
                    gdc_query = f"""
                    SELECT DISTINCT {gdc_col} as FIELD_VALUE
                    FROM GDC.WELL 
                    WHERE {gdc_col} IS NOT NULL
                    """
                    
                    gdc_fields_df = pd.read_sql(gdc_query, connection)
                    gdc_field_set = set(gdc_fields_df['FIELD_VALUE'].str.upper().str.strip())
                    
                    # Check for matches with bit field names
                    bit_field_set = set([str(f).upper().strip() for f in bit_fields if f is not None and str(f).strip() != ''])
                    
                    matches = bit_field_set.intersection(gdc_field_set)
                    
                    if matches:
                        print(f"    ✅ Found {len(matches)} exact matches:")
                        for match in sorted(list(matches))[:10]:  # Show first 10
                            print(f"      - {match}")
                    else:
                        print(f"    ❌ No exact matches found")
                        
                        # Try partial matches
                        partial_matches = []
                        for bit_field in list(bit_field_set)[:10]:  # Check first 10 bit fields
                            for gdc_field in gdc_field_set:
                                if bit_field in gdc_field or gdc_field in bit_field:
                                    partial_matches.append((bit_field, gdc_field))
                        
                        if partial_matches:
                            print(f"    🔍 Found {len(partial_matches)} partial matches:")
                            for bit_f, gdc_f in partial_matches[:5]:  # Show first 5
                                print(f"      - '{bit_f}' <-> '{gdc_f}'")
                
                except Exception as e:
                    print(f"    ❌ Error checking {gdc_col}: {e}")
        
        return field_related_cols
        
    except Exception as e:
        logger.error(f"❌ Error analyzing GDC fields: {e}")
        return None

def main():
    """Main function to analyze field names"""
    
    print("🔍 FIELD NAME ANALYSIS")
    print("=" * 60)
    
    # Analyze bit data fields
    bit_fields = analyze_bit_data_fields()
    
    # Connect to GDC and analyze
    connection = connect_to_gdc()
    if connection:
        try:
            gdc_field_cols = analyze_gdc_fields(connection, bit_fields)
            
            # Recommendations
            print(f"\n💡 COLUMN MAPPING RECOMMENDATIONS:")
            print("=" * 50)
            
            if gdc_field_cols:
                print("Based on the analysis above, consider these mappings:")
                print("(Review the sample data to make the final decision)")
                print()
                
                for i, col in enumerate(gdc_field_cols[:3], 1):
                    print(f"{i}. For field matching: '{col}'")
                    print(f"   - Use this if the sample data shows field names similar to your bit data")
                
                print(f"\n⚠️  IMPORTANT NOTES:")
                print("- Review the sample data above to choose the best field column")
                print("- Some columns might contain area/pool names rather than field names")
                print("- Consider if partial matching strategies are needed")
                print("- Field names might be formatted differently (case, spaces, abbreviations)")
                
            else:
                print("❌ Could not identify suitable field columns in GDC table")
        
        finally:
            connection.close()
    else:
        print("❌ Could not connect to GDC database")

if __name__ == "__main__":
    main()
