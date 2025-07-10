import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def analyze_td_bits():
    """
    Analyze trends for bits with DullGrade_Reason = 'TD' (Total Depth)
    """
    
    # Find the most recent merged file
    output_folder = Path("Output")
    merged_files = list(output_folder.glob("Ulterra_Merged_*.xlsx"))
    
    if not merged_files:
        print("No merged files found! Please run the merge script first.")
        return
    
    # Get the most recent file
    latest_file = max(merged_files, key=lambda x: x.stat().st_mtime)
    print(f"Analyzing data from: {latest_file.name}")
    
    # Load the data
    df = pd.read_excel(latest_file)
    print(f"Total records: {len(df)}")
    
    # Filter for TD bits
    td_bits = df[df['DullGrade_Reason'] == 'TD'].copy()
    print(f"TD (Total Depth) bits: {len(td_bits)}")
    
    if len(td_bits) == 0:
        print("No bits with DullGrade_Reason = 'TD' found!")
        return
    
    print(f"TD bits represent {len(td_bits)/len(df)*100:.1f}% of all bits")
    
    # Basic statistics
    print("\n" + "="*50)
    print("TD BITS ANALYSIS")
    print("="*50)
    
    # Key numeric columns to analyze
    numeric_columns = []
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64'] and col not in ['Source_File', 'File_Modified']:
            if td_bits[col].notna().sum() > 10:  # Only include columns with sufficient data
                numeric_columns.append(col)
    
    print(f"\nAnalyzing {len(numeric_columns)} numeric columns with sufficient data")
    
    # Display basic info about TD bits
    print("\n1. BASIC STATISTICS")
    print("-" * 30)
    
    # Bit size distribution
    if 'BitSize' in df.columns:
        bit_sizes = td_bits['BitSize'].value_counts()
        print(f"\nBit Sizes for TD bits:")
        for size, count in bit_sizes.head(10).items():
            print(f"  {size}: {count} bits ({count/len(td_bits)*100:.1f}%)")
    
    # Operator distribution
    if 'OperatorName' in df.columns:
        operators = td_bits['OperatorName'].value_counts()
        print(f"\nTop Operators using TD bits:")
        for op, count in operators.head(5).items():
            print(f"  {op}: {count} bits")
    
    # Performance metrics
    print("\n2. PERFORMANCE METRICS")
    print("-" * 30)
    
    # Key performance indicators
    performance_cols = [
        'TotalFootage', 'TotalHours', 'ROP_Overall', 'ROP_OnBottom',
        'WOB_Avg', 'RPM_Avg', 'Torque_Avg', 'GPM_Avg', 'SPP_Avg'
    ]
    
    for col in performance_cols:
        if col in td_bits.columns and td_bits[col].notna().sum() > 10:
            values = td_bits[col].dropna()
            print(f"\n{col}:")
            print(f"  Count: {len(values)}")
            print(f"  Mean: {values.mean():.2f}")
            print(f"  Median: {values.median():.2f}")
            print(f"  Std: {values.std():.2f}")
            print(f"  Min: {values.min():.2f}")
            print(f"  Max: {values.max():.2f}")
    
    # Comparison with non-TD bits
    print("\n3. COMPARISON WITH OTHER DULL REASONS")
    print("-" * 30)
    
    other_bits = df[df['DullGrade_Reason'] != 'TD'].copy()
    
    comparison_cols = ['TotalFootage', 'TotalHours', 'ROP_Overall']
    
    for col in comparison_cols:
        if col in df.columns and df[col].notna().sum() > 20:
            td_values = td_bits[col].dropna()
            other_values = other_bits[col].dropna()
            
            if len(td_values) > 0 and len(other_values) > 0:
                print(f"\n{col} Comparison:")
                print(f"  TD bits - Mean: {td_values.mean():.2f}, Median: {td_values.median():.2f}")
                print(f"  Other bits - Mean: {other_values.mean():.2f}, Median: {other_values.median():.2f}")
                
                # Simple statistical test
                if td_values.mean() > other_values.mean():
                    diff_pct = (td_values.mean() - other_values.mean()) / other_values.mean() * 100
                    print(f"  TD bits are {diff_pct:.1f}% higher on average")
                else:
                    diff_pct = (other_values.mean() - td_values.mean()) / td_values.mean() * 100
                    print(f"  TD bits are {diff_pct:.1f}% lower on average")
    
    # Time trends (if we have date information)
    print("\n4. TIME TRENDS")
    print("-" * 30)
    
    # Try to find date columns
    date_cols = []
    for col in td_bits.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            try:
                # Try to convert to datetime
                td_bits[col] = pd.to_datetime(td_bits[col], errors='coerce')
                if td_bits[col].notna().sum() > 10:
                    date_cols.append(col)
            except:
                continue
    
    if date_cols:
        for date_col in date_cols[:2]:  # Analyze up to 2 date columns
            print(f"\nTrends by {date_col}:")
            td_bits[f'{date_col}_year'] = td_bits[date_col].dt.year
            yearly_counts = td_bits[f'{date_col}_year'].value_counts().sort_index()
            
            for year, count in yearly_counts.items():
                if pd.notna(year) and year is not None:
                    print(f"  {year}: {count} TD bits")
    else:
        print("No clear date columns found for time trend analysis")
    
    # Source file analysis
    print("\n5. DATA SOURCE BREAKDOWN")
    print("-" * 30)
    
    if 'Source_File' in td_bits.columns:
        source_counts = td_bits['Source_File'].value_counts()
        print("TD bits by source file:")
        for source, count in source_counts.items():
            print(f"  {source}: {count} bits ({count/len(td_bits)*100:.1f}%)")
    
    # Create a summary report
    create_td_summary_report(td_bits, df, latest_file.stem)
    
    print(f"\n{'='*50}")
    print("Analysis complete! Check the Output folder for detailed reports.")
    print(f"{'='*50}")

def create_td_summary_report(td_bits, all_bits, filename_base):
    """
    Create a detailed Excel report of TD bit analysis
    """
    
    output_path = Path("Output") / f"TD_Analysis_{filename_base}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: TD Bits Raw Data
        td_bits.to_excel(writer, sheet_name='TD_Bits_Data', index=False)
        
        # Sheet 2: Summary Statistics
        summary_data = []
        
        # Performance metrics
        performance_cols = [
            'TotalFootage', 'TotalHours', 'ROP_Overall', 'ROP_OnBottom',
            'WOB_Avg', 'RPM_Avg', 'Torque_Avg', 'GPM_Avg', 'SPP_Avg'
        ]
        
        for col in performance_cols:
            if col in td_bits.columns and td_bits[col].notna().sum() > 5:
                values = td_bits[col].dropna()
                summary_data.append({
                    'Metric': col,
                    'Count': len(values),
                    'Mean': values.mean(),
                    'Median': values.median(),
                    'Std_Dev': values.std(),
                    'Min': values.min(),
                    'Max': values.max(),
                    'Q25': values.quantile(0.25),
                    'Q75': values.quantile(0.75)
                })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary_Stats', index=False)
        
        # Sheet 3: Comparisons
        comparison_data = []
        
        for col in performance_cols:
            if col in all_bits.columns and all_bits[col].notna().sum() > 20:
                td_values = td_bits[col].dropna()
                other_values = all_bits[all_bits['DullGrade_Reason'] != 'TD'][col].dropna()
                
                if len(td_values) > 5 and len(other_values) > 5:
                    comparison_data.append({
                        'Metric': col,
                        'TD_Mean': td_values.mean(),
                        'TD_Median': td_values.median(),
                        'TD_Count': len(td_values),
                        'Other_Mean': other_values.mean(),
                        'Other_Median': other_values.median(),
                        'Other_Count': len(other_values),
                        'Difference_Pct': (td_values.mean() - other_values.mean()) / other_values.mean() * 100
                    })
        
        if comparison_data:
            comparison_df = pd.DataFrame(comparison_data)
            comparison_df.to_excel(writer, sheet_name='TD_vs_Others', index=False)
        
        # Sheet 4: Breakdowns
        breakdown_data = []
        
        # Bit size breakdown
        if 'BitSize' in td_bits.columns:
            bit_size_counts = td_bits['BitSize'].value_counts()
            for size, count in bit_size_counts.items():
                breakdown_data.append({
                    'Category': 'Bit Size',
                    'Value': size,
                    'Count': count,
                    'Percentage': count / len(td_bits) * 100
                })
        
        # Operator breakdown
        if 'OperatorName' in td_bits.columns:
            operator_counts = td_bits['OperatorName'].value_counts()
            for operator, count in operator_counts.head(10).items():
                breakdown_data.append({
                    'Category': 'Operator',
                    'Value': operator,
                    'Count': count,
                    'Percentage': count / len(td_bits) * 100
                })
        
        if breakdown_data:
            breakdown_df = pd.DataFrame(breakdown_data)
            breakdown_df.to_excel(writer, sheet_name='Breakdowns', index=False)
    
    print(f"\nDetailed analysis saved to: {output_path}")

if __name__ == "__main__":
    analyze_td_bits()
