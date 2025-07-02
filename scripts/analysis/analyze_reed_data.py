import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def analyze_reed_montney_data():
    """
    Analyze Reed Montney drilling data since 2020
    """
    
    # Load the Reed data
    reed_file = Path("Input/Reed/All+Montney+since+2020.xlsx")
    output_folder = Path("Output")
    output_folder.mkdir(exist_ok=True)
    
    if not reed_file.exists():
        print(f"Reed file not found: {reed_file}")
        return
    
    print(f"Loading Reed Montney data from: {reed_file.name}")
    df = pd.read_excel(reed_file)
    
    print(f"\n{'='*80}")
    print("REED MONTNEY DRILLING DATA ANALYSIS")
    print(f"{'='*80}")
    print(f"Total records: {len(df):,}")
    print(f"Date range: {df['Spud'].min()} to {df['Spud'].max()}")
    print(f"Columns: {len(df.columns)}")
    
    # Basic data quality check
    print(f"\n{'='*60}")
    print("DATA QUALITY OVERVIEW")
    print(f"{'='*60}")
    
    key_columns = ['Bit Size', 'Bit Type', 'Bit Mfg', 'Distance', 'Hrs', 'ROP', 'Operator']
    for col in key_columns:
        if col in df.columns:
            non_null = df[col].notna().sum()
            print(f"{col}: {non_null:,} non-null values ({non_null/len(df)*100:.1f}%)")
    
    # Bit Manufacturer Analysis
    print(f"\n{'='*60}")
    print("BIT MANUFACTURER ANALYSIS")
    print(f"{'='*60}")
    
    bit_mfg_analysis = df.groupby('Bit Mfg').agg({
        'Distance': ['count', 'mean', 'sum'],
        'ROP': 'mean',
        'Hrs': 'mean'
    }).round(2)
    
    print("Performance by Bit Manufacturer:")
    print(f"{'Manufacturer':<25} {'Count':<8} {'Avg Dist':<10} {'Total Dist':<12} {'Avg ROP':<10} {'Avg Hrs':<10}")
    print("-" * 85)
    
    for mfg in bit_mfg_analysis.index:
        count = bit_mfg_analysis.loc[mfg, ('Distance', 'count')]
        avg_dist = bit_mfg_analysis.loc[mfg, ('Distance', 'mean')]
        total_dist = bit_mfg_analysis.loc[mfg, ('Distance', 'sum')]
        avg_rop = bit_mfg_analysis.loc[mfg, ('ROP', 'mean')]
        avg_hrs = bit_mfg_analysis.loc[mfg, ('Hrs', 'mean')]
        
        if pd.notna(count) and count > 50:  # Only show manufacturers with significant volume
            print(f"{mfg:<25} {count:<8.0f} {avg_dist:<10.0f} {total_dist:<12.0f} {avg_rop:<10.1f} {avg_hrs:<10.1f}")
    
    # Reed Hycalog Detailed Analysis
    print(f"\n{'='*60}")
    print("REED HYCALOG DETAILED ANALYSIS")
    print(f"{'='*60}")
    
    reed_data = df[df['Bit Mfg'] == 'REEDHYCALOG'].copy()
    print(f"Reed Hycalog bits: {len(reed_data):,} ({len(reed_data)/len(df)*100:.1f}% of total)")
    
    # Reed performance metrics
    reed_metrics = {
        'Distance': reed_data['Distance'].dropna(),
        'ROP': reed_data['ROP'].dropna(),
        'Hours': reed_data['Hrs'].dropna()
    }
    
    print("\nReed Hycalog Performance Metrics:")
    for metric, values in reed_metrics.items():
        if len(values) > 0:
            print(f"\n{metric}:")
            print(f"  Count: {len(values):,}")
            print(f"  Mean: {values.mean():.2f}")
            print(f"  Median: {values.median():.2f}")
            print(f"  75th Percentile: {values.quantile(0.75):.2f}")
            print(f"  90th Percentile: {values.quantile(0.90):.2f}")
            print(f"  Maximum: {values.max():.2f}")
    
    # Bit Type Analysis for Reed
    print(f"\n{'='*60}")
    print("REED BIT TYPE ANALYSIS")
    print(f"{'='*60}")
    
    if 'Bit Type' in reed_data.columns:
        reed_bit_types = reed_data.groupby('Bit Type').agg({
            'Distance': ['count', 'mean'],
            'ROP': 'mean',
            'Hrs': 'mean'
        }).round(2)
        
        print("Reed Bit Types Performance:")
        print(f"{'Bit Type':<20} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10} {'Avg Hrs':<10}")
        print("-" * 65)
        
        for bit_type in reed_bit_types.index:
            if pd.notna(bit_type):
                count = reed_bit_types.loc[bit_type, ('Distance', 'count')]
                avg_dist = reed_bit_types.loc[bit_type, ('Distance', 'mean')]
                avg_rop = reed_bit_types.loc[bit_type, ('ROP', 'mean')]
                avg_hrs = reed_bit_types.loc[bit_type, ('Hrs', 'mean')]
                
                if count > 10:  # Only show types with reasonable volume
                    print(f"{str(bit_type)[:19]:<20} {count:<8.0f} {avg_dist:<10.0f} {avg_rop:<10.1f} {avg_hrs:<10.1f}")
    
    # Operator Analysis
    print(f"\n{'='*60}")
    print("OPERATOR ANALYSIS")
    print(f"{'='*60}")
    
    if 'Operator' in df.columns:
        operator_analysis = df.groupby('Operator').agg({
            'Distance': ['count', 'mean'],
            'ROP': 'mean'
        }).round(2).sort_values(('Distance', 'count'), ascending=False)
        
        print("Top Operators by Volume:")
        print(f"{'Operator':<30} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10}")
        print("-" * 65)
        
        for operator in operator_analysis.head(10).index:
            count = operator_analysis.loc[operator, ('Distance', 'count')]
            avg_dist = operator_analysis.loc[operator, ('Distance', 'mean')]
            avg_rop = operator_analysis.loc[operator, ('ROP', 'mean')]
            
            if pd.notna(count):
                print(f"{str(operator)[:29]:<30} {count:<8.0f} {avg_dist:<10.0f} {avg_rop:<10.1f}")
    
    # Time Trends Analysis
    print(f"\n{'='*60}")
    print("TIME TRENDS ANALYSIS")
    print(f"{'='*60}")
    
    if 'Spud' in df.columns:
        df['SpudYear'] = df['Spud'].dt.year
        
        yearly_trends = df.groupby(['SpudYear', 'Bit Mfg']).agg({
            'Distance': ['count', 'mean'],
            'ROP': 'mean'
        }).round(2)
        
        print("Reed vs Ulterra Yearly Trends:")
        print(f"{'Year':<6} {'Manufacturer':<15} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10}")
        print("-" * 55)
        
        for year in sorted(df['SpudYear'].dropna().unique()):
            if year >= 2020:
                for mfg in ['REEDHYCALOG', 'ULTERRA']:
                    if (year, mfg) in yearly_trends.index:
                        count = yearly_trends.loc[(year, mfg), ('Distance', 'count')]
                        avg_dist = yearly_trends.loc[(year, mfg), ('Distance', 'mean')]
                        avg_rop = yearly_trends.loc[(year, mfg), ('ROP', 'mean')]
                        
                        if pd.notna(count) and count > 5:
                            print(f"{year:<6.0f} {mfg:<15} {count:<8.0f} {avg_dist:<10.0f} {avg_rop:<10.1f}")
    
    # Bit Size Analysis
    print(f"\n{'='*60}")
    print("BIT SIZE ANALYSIS")
    print(f"{'='*60}")
    
    if 'Bit Size' in df.columns:
        bit_size_analysis = df.groupby('Bit Size').agg({
            'Distance': ['count', 'mean'],
            'ROP': 'mean',
            'Bit Mfg': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'N/A'
        }).round(2).sort_values(('Distance', 'count'), ascending=False)
        
        print("Performance by Bit Size:")
        print(f"{'Size':<8} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10} {'Dom. Mfg':<15}")
        print("-" * 60)
        
        for size in bit_size_analysis.head(10).index:
            if pd.notna(size):
                count = bit_size_analysis.loc[size, ('Distance', 'count')]
                avg_dist = bit_size_analysis.loc[size, ('Distance', 'mean')]
                avg_rop = bit_size_analysis.loc[size, ('ROP', 'mean')]
                dom_mfg = bit_size_analysis.loc[size, ('Bit Mfg', '<lambda>')]
                
                print(f"{size:<8.3f} {count:<8.0f} {avg_dist:<10.0f} {avg_rop:<10.1f} {str(dom_mfg)[:14]:<15}")
    
    # Comparative Analysis: Reed vs Competition
    print(f"\n{'='*60}")
    print("REED VS COMPETITION ANALYSIS")
    print(f"{'='*60}")
    
    # Compare Reed against other major manufacturers
    major_mfgs = ['REEDHYCALOG', 'HUGHES CHRISTENSEN', 'SMITH BITS', 'ULTERRA']
    
    comparison_data = []
    for mfg in major_mfgs:
        mfg_data = df[df['Bit Mfg'] == mfg]
        if len(mfg_data) > 100:  # Only include manufacturers with significant data
            comparison_data.append({
                'Manufacturer': mfg,
                'Count': len(mfg_data),
                'Market_Share': len(mfg_data) / len(df) * 100,
                'Avg_Distance': mfg_data['Distance'].mean(),
                'Avg_ROP': mfg_data['ROP'].mean(),
                'Avg_Hours': mfg_data['Hrs'].mean(),
                'Total_Distance': mfg_data['Distance'].sum()
            })
    
    comparison_df = pd.DataFrame(comparison_data).round(2)
    
    print("Major Manufacturer Comparison:")
    print(f"{'Manufacturer':<20} {'Count':<8} {'Market%':<9} {'Avg Dist':<10} {'Avg ROP':<10} {'Total Dist':<12}")
    print("-" * 80)
    
    for _, row in comparison_df.iterrows():
        print(f"{row['Manufacturer']:<20} {row['Count']:<8.0f} {row['Market_Share']:<9.1f} "
              f"{row['Avg_Distance']:<10.0f} {row['Avg_ROP']:<10.1f} {row['Total_Distance']:<12.0f}")
    
    # Key Insights
    print(f"\n{'='*60}")
    print("KEY INSIGHTS")
    print(f"{'='*60}")
    
    insights = []
    
    # Market share
    reed_share = len(reed_data) / len(df) * 100
    insights.append(f"1. Reed Hycalog dominates with {reed_share:.1f}% market share in Montney")
    
    # Performance comparison
    reed_avg_rop = reed_data['ROP'].mean()
    overall_avg_rop = df['ROP'].mean()
    if reed_avg_rop > overall_avg_rop:
        rop_diff = (reed_avg_rop - overall_avg_rop) / overall_avg_rop * 100
        insights.append(f"2. Reed bits show {rop_diff:.1f}% higher ROP than market average")
    else:
        rop_diff = (overall_avg_rop - reed_avg_rop) / reed_avg_rop * 100
        insights.append(f"2. Reed bits show {rop_diff:.1f}% lower ROP than market average")
    
    # Distance performance
    reed_avg_dist = reed_data['Distance'].mean()
    insights.append(f"3. Reed bits average {reed_avg_dist:.0f}m per run")
    
    # Market trends
    if 'SpudYear' in df.columns:
        recent_reed = reed_data[reed_data['SpudYear'] >= 2023]
        if len(recent_reed) > 50:
            recent_rop = recent_reed['ROP'].mean()
            insights.append(f"4. Recent Reed performance (2023+): {recent_rop:.1f} m/hr average ROP")
    
    for insight in insights:
        print(f"  {insight}")
    
    # Create Excel report
    create_reed_analysis_report(df, reed_data, comparison_df)
    
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*60}")

def create_reed_analysis_report(df, reed_data, comparison_df):
    """
    Create comprehensive Excel report for Reed analysis
    """
    output_path = Path("Output") / f"Reed_Montney_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: Raw Data Summary
        summary_data = {
            'Total_Records': len(df),
            'Reed_Records': len(reed_data),
            'Reed_Market_Share': len(reed_data) / len(df) * 100,
            'Date_Range_Start': df['Spud'].min(),
            'Date_Range_End': df['Spud'].max(),
            'Reed_Avg_ROP': reed_data['ROP'].mean(),
            'Reed_Avg_Distance': reed_data['Distance'].mean(),
            'Reed_Avg_Hours': reed_data['Hrs'].mean()
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Sheet 2: Manufacturer Comparison
        comparison_df.to_excel(writer, sheet_name='Manufacturer_Comparison', index=False)
        
        # Sheet 3: Reed Bit Types
        if 'Bit Type' in reed_data.columns:
            reed_bit_analysis = reed_data.groupby('Bit Type').agg({
                'Distance': ['count', 'mean', 'max'],
                'ROP': ['mean', 'max'],
                'Hrs': ['mean', 'max']
            }).round(2)
            
            reed_bit_analysis.to_excel(writer, sheet_name='Reed_Bit_Types')
        
        # Sheet 4: Operator Analysis
        if 'Operator' in df.columns:
            operator_reed = df.groupby('Operator').agg({
                'Distance': 'count',
                'Bit Mfg': lambda x: (x == 'REEDHYCALOG').sum()
            })
            operator_reed['Reed_Percentage'] = (operator_reed['Bit Mfg'] / operator_reed['Distance'] * 100).round(1)
            operator_reed = operator_reed.sort_values('Distance', ascending=False)
            
            operator_reed.to_excel(writer, sheet_name='Operator_Reed_Usage')
        
        # Sheet 5: Yearly Trends
        if 'SpudYear' in df.columns:
            yearly_analysis = df.groupby(['SpudYear', 'Bit Mfg']).agg({
                'Distance': ['count', 'mean'],
                'ROP': 'mean'
            }).round(2)
            
            yearly_analysis.to_excel(writer, sheet_name='Yearly_Trends')
    
    print(f"\nDetailed analysis saved to: {output_path}")

if __name__ == "__main__":
    analyze_reed_montney_data()
