#!/usr/bin/env python3
"""
Export Unmatched Wells for Audit
Creates a detailed export of wells that didn't match in the GDC lookup for manual review.
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_missing_license_records():
    """Load records that are missing license numbers"""
    # Find the most recent integrated file
    output_dir = Path("Output")
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    
    if not integrated_files:
        logger.error("No integrated data files found")
        return None
    
    # Use the most recent file
    latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Loading data from: {latest_file.name}")
    
    # Load the data
    df = pd.read_excel(latest_file)
    
    # Filter to records missing license numbers
    missing_licenses = df[
        (df['data_source'] == 'ulterra') &
        ((df['license_number'].isna()) | (df['license_number'] == ''))
    ].copy()
    
    logger.info(f"Found {len(missing_licenses)} records missing license numbers")
    return missing_licenses

def load_lookup_results():
    """Load the most recent lookup results"""
    output_dir = Path("Output")
    lookup_files = list(output_dir.glob("License_Lookup_Results_*.xlsx"))
    
    if not lookup_files:
        logger.warning("No lookup results files found")
        return None, None
    
    # Use the most recent file
    latest_file = max(lookup_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Loading lookup results from: {latest_file.name}")
    
    # Load both sheets
    try:
        recommendations = pd.read_excel(latest_file, sheet_name='Recommendations')
        well_matches = pd.read_excel(latest_file, sheet_name='Well_Name_Matches')
        return recommendations, well_matches
    except Exception as e:
        logger.error(f"Error loading lookup results: {e}")
        return None, None

def create_audit_export(missing_licenses, recommendations, well_matches):
    """Create detailed audit export of unmatched wells"""
    logger.info("Creating audit export...")
    
    # Get wells that were matched
    matched_wells = set()
    if recommendations is not None and len(recommendations) > 0:
        matched_wells.update(recommendations['well_name'].unique())
    
    # Filter to unmatched wells only
    unmatched = missing_licenses[~missing_licenses['well_name'].isin(matched_wells)].copy()
    logger.info(f"Found {len(unmatched)} unmatched wells for audit")
    
    # Create audit columns with detailed information
    audit_data = []
    
    for idx, row in unmatched.iterrows():
        audit_record = {
            # === WELL IDENTIFICATION ===
            'well_name': row['well_name'],
            'well_number': row.get('well_number', ''),
            'operator': row['operator'],
            'field': row['field'],
            'data_source': row['data_source'],
            'source_file': row.get('source_file', ''),
            
            # === LOCATION DETAILS ===
            'latitude': row.get('latitude', ''),
            'longitude': row.get('longitude', ''),
            'lsd': row.get('lsd', ''),
            'section': row.get('section', ''),
            'township': row.get('township', ''),
            'range': row.get('range', ''),
            
            # === TIMING ===
            'spud_date': row.get('spud_date', ''),
            'run_date': row.get('run_date', ''),
            
            # === BIT DETAILS ===
            'bit_manufacturer': row.get('bit_manufacturer', ''),
            'bit_size_mm': row.get('bit_size_mm', ''),
            'bit_type': row.get('bit_type', ''),
            
            # === AUDIT ANALYSIS ===
            'well_name_length': len(str(row['well_name'])) if pd.notna(row['well_name']) else 0,
            'has_coordinates': 'Yes' if (pd.notna(row.get('latitude')) and pd.notna(row.get('longitude'))) else 'No',
            'has_spud_date': 'Yes' if pd.notna(row.get('spud_date')) else 'No',
            'has_legal_location': 'Yes' if any(pd.notna(row.get(col)) for col in ['lsd', 'section', 'township', 'range']) else 'No',
            
            # === POTENTIAL ISSUES ===
            'potential_issues': [],
        }
        
        # Analyze potential matching issues
        issues = []
        
        # Check well name quality
        well_name = str(row['well_name']) if pd.notna(row['well_name']) else ''
        if len(well_name) < 5:
            issues.append("Well name too short")
        if not any(char.isdigit() for char in well_name):
            issues.append("Well name lacks numbers")
        if well_name.count(' ') > 8:
            issues.append("Well name very long/complex")
        
        # Check coordinate quality
        if not (pd.notna(row.get('latitude')) and pd.notna(row.get('longitude'))):
            issues.append("Missing coordinates")
        elif (abs(float(row.get('latitude', 0))) < 50 or abs(float(row.get('latitude', 0))) > 70 or
              abs(float(row.get('longitude', 0))) < 100 or abs(float(row.get('longitude', 0))) > 140):
            issues.append("Coordinates outside typical Canadian range")
        
        # Check field name
        if pd.isna(row.get('field')) or str(row.get('field')).strip() == '':
            issues.append("Missing field name")
        
        # Check spud date
        if pd.isna(row.get('spud_date')):
            issues.append("Missing spud date")
        
        # Check for special characters or formatting issues
        if any(char in well_name for char in ['/', '\\', '*', '?', '<', '>', '|']):
            issues.append("Well name contains special characters")
        
        audit_record['potential_issues'] = '; '.join(issues) if issues else 'None identified'
        audit_record['issue_count'] = len(issues)
        
        audit_data.append(audit_record)
    
    # Convert to DataFrame
    audit_df = pd.DataFrame(audit_data)
    
    # Sort by issue count (most issues first) then by field
    audit_df = audit_df.sort_values(['issue_count', 'field', 'well_name'], ascending=[False, True, True])
    
    return audit_df

def create_field_summary(unmatched_df):
    """Create a summary by field for quick analysis"""
    field_summary = unmatched_df.groupby('field').agg({
        'well_name': 'count',
        'has_coordinates': lambda x: sum(x == 'Yes'),
        'has_spud_date': lambda x: sum(x == 'Yes'),
        'has_legal_location': lambda x: sum(x == 'Yes'),
        'issue_count': ['mean', 'sum']
    }).round(2)
    
    # Flatten column names
    field_summary.columns = [
        'total_wells', 'wells_with_coords', 'wells_with_spud', 
        'wells_with_legal', 'avg_issues_per_well', 'total_issues'
    ]
    
    # Add percentages
    field_summary['coord_percentage'] = (field_summary['wells_with_coords'] / field_summary['total_wells'] * 100).round(1)
    field_summary['spud_percentage'] = (field_summary['wells_with_spud'] / field_summary['total_wells'] * 100).round(1)
    field_summary['legal_percentage'] = (field_summary['wells_with_legal'] / field_summary['total_wells'] * 100).round(1)
    
    return field_summary.sort_values('total_wells', ascending=False)

def create_issue_analysis(unmatched_df):
    """Analyze the most common issues preventing matches"""
    all_issues = []
    for issues_str in unmatched_df['potential_issues']:
        if issues_str and issues_str != 'None identified':
            all_issues.extend([issue.strip() for issue in issues_str.split(';')])
    
    if all_issues:
        issue_counts = pd.Series(all_issues).value_counts()
        issue_df = pd.DataFrame({
            'issue': issue_counts.index,
            'count': issue_counts.values,
            'percentage': (issue_counts.values.astype(float) / len(unmatched_df) * 100).round(1)
        })
        return issue_df
    else:
        return pd.DataFrame(columns=['issue', 'count', 'percentage'])

def export_audit_data():
    """Main function to export audit data"""
    logger.info("🔍 UNMATCHED WELLS AUDIT EXPORT")
    logger.info("=" * 50)
    
    # Load data
    missing_licenses = load_missing_license_records()
    if missing_licenses is None:
        logger.error("Could not load missing license data")
        return
    
    recommendations, well_matches = load_lookup_results()
    
    # Create audit export
    audit_df = create_audit_export(missing_licenses, recommendations, well_matches)
    field_summary = create_field_summary(audit_df)
    issue_analysis = create_issue_analysis(audit_df)
    
    # Create output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"Output/Unmatched_Wells_Audit_{timestamp}.xlsx"
    
    # Export to Excel with multiple sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Main audit data
        audit_df.to_excel(writer, sheet_name='Unmatched_Wells_Detail', index=False)
        
        # Field summary
        field_summary.to_excel(writer, sheet_name='Field_Summary')
        
        # Issue analysis
        issue_analysis.to_excel(writer, sheet_name='Issue_Analysis', index=False)
        
        # Add a summary sheet with key stats
        summary_stats = pd.DataFrame({
            'Metric': [
                'Total Unmatched Wells',
                'Wells Missing Coordinates',
                'Wells Missing Spud Date',
                'Wells Missing Legal Location',
                'Most Common Field',
                'Avg Issues Per Well',
                'Wells With No Issues Identified'
            ],
            'Value': [
                len(audit_df),
                len(audit_df[audit_df['has_coordinates'] == 'No']),
                len(audit_df[audit_df['has_spud_date'] == 'No']),
                len(audit_df[audit_df['has_legal_location'] == 'No']),
                audit_df['field'].mode().iloc[0] if len(audit_df) > 0 else 'N/A',
                audit_df['issue_count'].mean(),
                len(audit_df[audit_df['potential_issues'] == 'None identified'])
            ]
        })
        summary_stats.to_excel(writer, sheet_name='Summary_Stats', index=False)
    
    logger.info(f"✅ Audit export saved: {output_file}")
    logger.info(f"📊 Export contains {len(audit_df)} unmatched wells")
    logger.info(f"📋 Top issues:")
    
    if len(issue_analysis) > 0:
        for idx, row in issue_analysis.head(5).iterrows():
            logger.info(f"   • {row['issue']}: {row['count']} wells ({row['percentage']}%)")
    
    logger.info(f"🏭 Fields with most unmatched wells:")
    for field, count in audit_df['field'].value_counts().head(5).items():
        logger.info(f"   • {field}: {count} wells")
    
    return output_file

if __name__ == "__main__":
    export_audit_data()
