"""
Explore GDC Well Table Columns for Better Field Matching
Look for columns that might contain actual field names rather than codes
"""

import pandas as pd
import oracledb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_gdc():
    """Connect to GDC Oracle database"""
    try:
        dsn = "WC-CGY-ORAP01:1521/PRD1"
        connection = oracledb.connect(user='synergyro', password='synergyro', dsn=dsn)
        return connection
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return None

def explore_all_columns():
    """Explore all columns in GDC.WELL table"""
    
    connection = connect_to_gdc()
    if not connection:
        return
    
    try:
        # Get all column information
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = 'WELL' 
        AND OWNER = 'GDC'
        ORDER BY COLUMN_ID
        """
        
        columns_df = pd.read_sql(query, connection)
        
        print("🔍 ALL GDC.WELL COLUMNS:")
        print("=" * 70)
        print(f"{'Column Name':<25} {'Type':<12} {'Length':<8} {'Nullable'}")
        print("-" * 70)
        
        # Look for columns that might contain field names
        potential_field_columns = []
        
        for _, row in columns_df.iterrows():
            col_name = row['COLUMN_NAME']
            print(f"{col_name:<25} {row['DATA_TYPE']:<12} {str(row['DATA_LENGTH']):<8} {row['NULLABLE']}")
            
            # Flag columns that might contain field names
            if any(keyword in col_name.upper() for keyword in [
                'FIELD', 'POOL', 'AREA', 'LOCATION', 'NAME', 'DISTRICT', 'FORMATION'
            ]):
                potential_field_columns.append(col_name)
        
        print(f"\n🎯 POTENTIAL FIELD-RELATED COLUMNS:")
        print("-" * 40)
        for col in potential_field_columns:
            print(f"  - {col}")
        
        # Sample data from the most promising columns
        print(f"\n📋 SAMPLE DATA FROM POTENTIAL COLUMNS:")
        print("=" * 50)
        
        sample_columns = potential_field_columns[:5]  # Sample first 5
        for col in sample_columns:
            print(f"\n🔹 {col}:")
            try:
                sample_query = f"""
                SELECT {col}, COUNT(*) as COUNT
                FROM GDC.WELL 
                WHERE {col} IS NOT NULL 
                AND ROWNUM <= 100000
                GROUP BY {col}
                ORDER BY COUNT(*) DESC
                FETCH FIRST 20 ROWS ONLY
                """
                
                sample_df = pd.read_sql(sample_query, connection)
                for _, row in sample_df.iterrows():
                    value = str(row[col])
                    if len(value) > 40:
                        value = value[:37] + "..."
                    print(f"  {value:<40} ({row['COUNT']:,} wells)")
                    
            except Exception as e:
                print(f"  ❌ Error sampling {col}: {e}")
        
        # Additional specific checks for well location/name patterns
        print(f"\n🔍 CHECKING FOR WELL NAME PATTERNS:")
        print("-" * 40)
        
        name_columns = [col for col in columns_df['COLUMN_NAME'] if 'NAME' in col.upper()]
        for col in name_columns[:3]:
            print(f"\n📝 {col}:")
            try:
                # Look for patterns that might indicate field names
                pattern_query = f"""
                SELECT {col}, COUNT(*) as COUNT
                FROM GDC.WELL 
                WHERE {col} IS NOT NULL 
                AND (
                    UPPER({col}) LIKE '%CREEK%' OR
                    UPPER({col}) LIKE '%KAKWA%' OR 
                    UPPER({col}) LIKE '%WAPITI%' OR
                    UPPER({col}) LIKE '%ANTE%' OR
                    UPPER({col}) LIKE '%GOLD%'
                )
                GROUP BY {col}
                ORDER BY COUNT(*) DESC
                FETCH FIRST 10 ROWS ONLY
                """
                
                pattern_df = pd.read_sql(pattern_query, connection)
                if not pattern_df.empty:
                    print("  ✅ Found potential field name matches:")
                    for _, row in pattern_df.iterrows():
                        print(f"    {row[col]:<35} ({row['COUNT']:,} wells)")
                else:
                    print("  ❌ No field name patterns found")
                    
            except Exception as e:
                print(f"  ❌ Error checking patterns in {col}: {e}")
    
    finally:
        connection.close()

if __name__ == "__main__":
    explore_all_columns()
