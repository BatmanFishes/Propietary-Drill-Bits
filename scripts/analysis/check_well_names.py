"""
Check WELL_NAME column for field name patterns
"""

import pandas as pd
import oracledb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_well_names():
    """Check WELL_NAME column for patterns matching our bit field names"""
    
    try:
        dsn = "WC-CGY-ORAP01:1521/PRD1"
        connection = oracledb.connect(user='synergyro', password='synergyro', dsn=dsn)
        
        # Our bit field names
        bit_fields = ['ANTE CREEK', 'KAKWA', 'WAPITI', 'GOLD CREEK', 'KARR', 'JAYAR', 
                     'BERLAND RIVER', 'ELMWORTH', 'KAYBOB', 'KLESKUN', 'MANIR', 'BILBO']
        
        print("🔍 CHECKING WELL_NAME COLUMN FOR FIELD PATTERNS:")
        print("=" * 60)
        
        for field in bit_fields:
            print(f"\n🔹 Searching for '{field}':")
            
            # Search for this field name in well names
            query = f"""
            SELECT WELL_NAME, COUNT(*) as COUNT
            FROM GDC.WELL 
            WHERE UPPER(WELL_NAME) LIKE '%{field.upper()}%'
            GROUP BY WELL_NAME
            ORDER BY COUNT(*) DESC
            FETCH FIRST 10 ROWS ONLY
            """
            
            try:
                results = pd.read_sql(query, connection)
                if not results.empty:
                    print(f"  ✅ Found {len(results)} well name patterns:")
                    for _, row in results.iterrows():
                        well_name = row['WELL_NAME']
                        if len(well_name) > 50:
                            well_name = well_name[:47] + "..."
                        print(f"    {well_name:<50} ({row['COUNT']:,} wells)")
                else:
                    print(f"  ❌ No matches found for '{field}'")
                    
            except Exception as e:
                print(f"  ❌ Error searching for '{field}': {e}")
        
        # Also check if field names might be encoded in a different way
        print(f"\n🔍 CHECKING FOR ALTERNATIVE FIELD ENCODING:")
        print("-" * 50)
        
        # Check if there's a pattern in well names vs. assigned fields
        query = """
        SELECT DISTINCT ASSIGNED_FIELD, WELL_NAME
        FROM GDC.WELL 
        WHERE WELL_NAME IS NOT NULL 
        AND ASSIGNED_FIELD IS NOT NULL
        AND (
            UPPER(WELL_NAME) LIKE '%ANTE%' OR
            UPPER(WELL_NAME) LIKE '%KAKWA%' OR
            UPPER(WELL_NAME) LIKE '%WAPITI%' OR
            UPPER(WELL_NAME) LIKE '%GOLD%'
        )
        ORDER BY ASSIGNED_FIELD
        FETCH FIRST 20 ROWS ONLY
        """
        
        try:
            patterns = pd.read_sql(query, connection)
            if not patterns.empty:
                print("✅ Found some field/well name combinations:")
                for _, row in patterns.iterrows():
                    print(f"  {row['ASSIGNED_FIELD']:<15} -> {row['WELL_NAME'][:60]}")
            else:
                print("❌ No clear field/well name patterns found")
        
        except Exception as e:
            print(f"❌ Error checking patterns: {e}")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Connection or query error: {e}")

if __name__ == "__main__":
    check_well_names()
