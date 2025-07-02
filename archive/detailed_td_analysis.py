import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def detailed_td_analysis():
    """
    Detailed analysis of TD (Total Depth) bits with actual performance metrics
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
    
    print(f"\n{'='*60}")
    print("DETAILED TD BITS PERFORMANCE ANALYSIS")
    print(f"{'='*60}")
    
    # 1. DRILLING PERFORMANCE METRICS
    print("\n1. DRILLING PERFORMANCE METRICS")
    print("-" * 40)
    
    performance_metrics = {
        'DepthDrilled (m)': 'Depth drilled per bit run',
        'DrillingHours': 'Total drilling hours',
        'ROP (m/hr)': 'Rate of penetration',
        'OnBottomHours': 'Hours on bottom',
        'OnBottomROP (m/hr)': 'ROP while on bottom'
    }
    
    td_performance = {}
    
    for col, description in performance_metrics.items():
        if col in td_bits.columns:
            values = td_bits[col].dropna()
            if len(values) > 0:
                td_performance[col] = {
                    'count': len(values),
                    'mean': values.mean(),
                    'median': values.median(),
                    'std': values.std(),
                    'min': values.min(),
                    'max': values.max(),
                    'q25': values.quantile(0.25),
                    'q75': values.quantile(0.75)
                }
                
                print(f"\n{col} ({description}):")
                print(f"  Count: {len(values)}")
                print(f"  Mean: {values.mean():.2f}")
                print(f"  Median: {values.median():.2f}")
                print(f"  Range: {values.min():.2f} - {values.max():.2f}")
                print(f"  Std Dev: {values.std():.2f}")
    
    # 2. OPERATING PARAMETERS
    print("\n2. OPERATING PARAMETERS")
    print("-" * 40)
    
    operating_params = {
        'WOB_Low (daN)': 'Weight on Bit - Low',
        'WOB_High (daN)': 'Weight on Bit - High',
        'SurfaceRPM_Low': 'Surface RPM - Low',
        'SurfaceRPM_High': 'Surface RPM - High'
    }
    
    for col, description in operating_params.items():
        if col in td_bits.columns:
            values = td_bits[col].dropna()
            if len(values) > 10:
                print(f"\n{col} ({description}):")
                print(f"  Count: {len(values)}")
                print(f"  Mean: {values.mean():.1f}")
                print(f"  Median: {values.median():.1f}")
                print(f"  Range: {values.min():.1f} - {values.max():.1f}")
    
    # 3. BIT SIZE AND DEPTH ANALYSIS
    print("\n3. BIT SIZE AND DEPTH ANALYSIS")
    print("-" * 40)
    
    # Bit size analysis
    if 'BitSize' in td_bits.columns:
        bit_sizes = td_bits['BitSize'].value_counts()
        print(f"\nBit Sizes for TD bits (Top 10):")
        for size, count in bit_sizes.head(10).items():
            avg_depth = td_bits[td_bits['BitSize'] == size]['DepthDrilled (m)'].mean()
            avg_rop = td_bits[td_bits['BitSize'] == size]['ROP (m/hr)'].mean()
            print(f"  {size}: {count} bits ({count/len(td_bits)*100:.1f}%) - Avg Depth: {avg_depth:.1f}m, Avg ROP: {avg_rop:.1f}m/hr")
    
    # Depth analysis
    if 'TotalDepth (m)' in td_bits.columns and 'DepthIn (m)' in td_bits.columns:
        td_bits_clean = td_bits.dropna(subset=['TotalDepth (m)', 'DepthIn (m)'])
        td_bits_clean['DepthFromSurface'] = td_bits_clean['DepthIn (m)']
        td_bits_clean['DepthCategory'] = pd.cut(td_bits_clean['DepthFromSurface'], 
                                               bins=[0, 1000, 2000, 3000, 4000, float('inf')],
                                               labels=['0-1000m', '1000-2000m', '2000-3000m', '3000-4000m', '4000m+'])
        
        print(f"\nTD Bits by Depth Category:")
        depth_analysis = td_bits_clean.groupby('DepthCategory').agg({
            'DepthDrilled (m)': ['count', 'mean'],
            'ROP (m/hr)': 'mean',
            'DrillingHours': 'mean'
        }).round(2)
        
        for depth_cat in depth_analysis.index:
            count = depth_analysis.loc[depth_cat, ('DepthDrilled (m)', 'count')]
            avg_drilled = depth_analysis.loc[depth_cat, ('DepthDrilled (m)', 'mean')]
            avg_rop = depth_analysis.loc[depth_cat, ('ROP (m/hr)', 'mean')]
            avg_hours = depth_analysis.loc[depth_cat, ('DrillingHours', 'mean')]
            print(f"  {depth_cat}: {count} bits - Avg Drilled: {avg_drilled:.1f}m, Avg ROP: {avg_rop:.1f}m/hr, Avg Hours: {avg_hours:.1f}")
    
    # 4. COMPARISON WITH OTHER DULL REASONS
    print("\n4. COMPARISON WITH OTHER DULL REASONS")
    print("-" * 40)
    
    # Compare TD vs top other dull reasons
    top_dull_reasons = df['DullGrade_Reason'].value_counts().head(5)
    print(f"\nTop 5 Dull Reasons:")
    for reason, count in top_dull_reasons.items():
        print(f"  {reason}: {count} bits ({count/len(df)*100:.1f}%)")
    
    # Performance comparison
    comparison_metrics = ['DepthDrilled (m)', 'ROP (m/hr)', 'DrillingHours']
    
    print(f"\nPerformance Comparison (TD vs Others):")
    for metric in comparison_metrics:
        if metric in df.columns:
            td_values = td_bits[metric].dropna()
            
            # Compare with other top reasons
            for reason in ['BHA', 'PR']:  # Top 2 other reasons
                if reason in df['DullGrade_Reason'].values:
                    other_values = df[df['DullGrade_Reason'] == reason][metric].dropna()
                    
                    if len(td_values) > 5 and len(other_values) > 5:
                        td_mean = td_values.mean()
                        other_mean = other_values.mean()
                        diff_pct = (td_mean - other_mean) / other_mean * 100
                        
                        print(f"\n  {metric}:")
                        print(f"    TD: {td_mean:.2f} (n={len(td_values)})")
                        print(f"    {reason}: {other_mean:.2f} (n={len(other_values)})")
                        print(f"    Difference: {diff_pct:+.1f}%")
    
    # 5. TIME TRENDS
    print("\n5. TIME TRENDS")
    print("-" * 40)
    
    if 'SpudDate' in td_bits.columns:
        td_bits['SpudDate'] = pd.to_datetime(td_bits['SpudDate'], errors='coerce')
        td_bits['SpudYear'] = td_bits['SpudDate'].dt.year
        
        yearly_trends = td_bits.groupby('SpudYear').agg({
            'DepthDrilled (m)': ['count', 'mean', 'max'],
            'ROP (m/hr)': ['mean', 'max'],
            'DrillingHours': ['mean', 'max']
        }).round(2)
        
        print(f"\nYearly Trends for TD Bits (2020+):")
        for year in sorted(yearly_trends.index):
            if pd.notna(year) and year >= 2020:
                count = yearly_trends.loc[year, ('DepthDrilled (m)', 'count')]
                avg_depth = yearly_trends.loc[year, ('DepthDrilled (m)', 'mean')]
                max_depth = yearly_trends.loc[year, ('DepthDrilled (m)', 'max')]
                avg_rop = yearly_trends.loc[year, ('ROP (m/hr)', 'mean')]
                max_rop = yearly_trends.loc[year, ('ROP (m/hr)', 'max')]
                avg_hours = yearly_trends.loc[year, ('DrillingHours', 'mean')]
                max_hours = yearly_trends.loc[year, ('DrillingHours', 'max')]
                
                print(f"  {year}: {count} bits")
                print(f"    Depth - Avg: {avg_depth:.1f}m, Max: {max_depth:.1f}m")
                print(f"    ROP - Avg: {avg_rop:.1f}m/hr, Max: {max_rop:.1f}m/hr")
                print(f"    Hours - Avg: {avg_hours:.1f}h, Max: {max_hours:.1f}h")
    
    # 6. KEY INSIGHTS
    print("\n6. KEY INSIGHTS")
    print("-" * 40)
    
    insights = []
    
    # ROP insights
    if 'ROP (m/hr)' in td_performance:
        avg_rop = td_performance['ROP (m/hr)']['mean']
        median_rop = td_performance['ROP (m/hr)']['median']
        insights.append(f"Average ROP for TD bits: {avg_rop:.2f} m/hr (median: {median_rop:.2f} m/hr)")
    
    # Depth insights
    if 'DepthDrilled (m)' in td_performance:
        avg_depth = td_performance['DepthDrilled (m)']['mean']
        median_depth = td_performance['DepthDrilled (m)']['median']
        insights.append(f"Average depth drilled per TD bit: {avg_depth:.1f}m (median: {median_depth:.1f}m)")
    
    # Hours insights
    if 'DrillingHours' in td_performance:
        avg_hours = td_performance['DrillingHours']['mean']
        insights.append(f"Average drilling hours for TD bits: {avg_hours:.1f} hours")
    
    # Source file insights
    if 'Source_File' in td_bits.columns:
        dominant_source = td_bits['Source_File'].value_counts().iloc[0]
        dominant_source_name = td_bits['Source_File'].value_counts().index[0]
        insights.append(f"Most TD bits come from {dominant_source_name} ({dominant_source} bits, {dominant_source/len(td_bits)*100:.1f}%)")
    
    for i, insight in enumerate(insights, 1):
        print(f"  {i}. {insight}")
    
    # Create detailed Excel report
    create_comprehensive_report(td_bits, df, latest_file.stem, td_performance)
    
    print(f"\n{'='*60}")
    print("DETAILED ANALYSIS COMPLETE!")
    print("Check the Output folder for comprehensive Excel reports.")
    print(f"{'='*60}")

def create_comprehensive_report(td_bits, all_bits, filename_base, performance_data):
    """
    Create a comprehensive Excel report with multiple analysis sheets
    """
    
    output_path = Path("Output") / f"TD_Comprehensive_Analysis_{filename_base}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: TD Bits Raw Data
        td_bits.to_excel(writer, sheet_name='TD_Bits_Raw_Data', index=False)
        
        # Sheet 2: Performance Summary
        perf_summary = []
        for metric, stats in performance_data.items():
            perf_summary.append({
                'Metric': metric,
                'Count': stats['count'],
                'Mean': round(stats['mean'], 2),
                'Median': round(stats['median'], 2),
                'Std_Dev': round(stats['std'], 2),
                'Min': round(stats['min'], 2),
                'Max': round(stats['max'], 2),
                'Q25': round(stats['q25'], 2),
                'Q75': round(stats['q75'], 2)
            })
        
        if perf_summary:
            pd.DataFrame(perf_summary).to_excel(writer, sheet_name='Performance_Summary', index=False)
        
        # Sheet 3: Bit Size Analysis
        if 'BitSize' in td_bits.columns:
            bit_size_analysis = td_bits.groupby('BitSize').agg({
                'DepthDrilled (m)': ['count', 'mean', 'median'],
                'ROP (m/hr)': ['mean', 'median'],
                'DrillingHours': ['mean', 'median']
            }).round(2)
            
            bit_size_analysis.to_excel(writer, sheet_name='Bit_Size_Analysis')
        
        # Sheet 4: Yearly Trends
        if 'SpudDate' in td_bits.columns:
            td_bits_copy = td_bits.copy()
            td_bits_copy['SpudDate'] = pd.to_datetime(td_bits_copy['SpudDate'], errors='coerce')
            td_bits_copy['SpudYear'] = td_bits_copy['SpudDate'].dt.year
            
            yearly_analysis = td_bits_copy.groupby('SpudYear').agg({
                'DepthDrilled (m)': ['count', 'mean', 'median', 'max'],
                'ROP (m/hr)': ['mean', 'median', 'max'],
                'DrillingHours': ['mean', 'median', 'max']
            }).round(2)
            
            yearly_analysis.to_excel(writer, sheet_name='Yearly_Trends')
        
        # Sheet 5: Operator Analysis
        if 'OperatorName' in td_bits.columns:
            operator_analysis = td_bits.groupby('OperatorName').agg({
                'DepthDrilled (m)': ['count', 'mean'],
                'ROP (m/hr)': 'mean',
                'DrillingHours': 'mean'
            }).round(2).sort_values(('DepthDrilled (m)', 'count'), ascending=False)
            
            operator_analysis.to_excel(writer, sheet_name='Operator_Analysis')
    
    print(f"\nComprehensive analysis saved to: {output_path}")

if __name__ == "__main__":
    detailed_td_analysis()
