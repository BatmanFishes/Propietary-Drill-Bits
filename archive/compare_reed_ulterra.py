import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def compare_reed_vs_ulterra():
    """
    Compare Reed Hycalog vs Ulterra drilling performance
    """
    
    print("REED vs ULTERRA DRILLING COMPARISON")
    print("=" * 60)
    
    # Load Reed data
    reed_path = Path("Input/Reed/All+Montney+since+2020.xlsx")
    if not reed_path.exists():
        print("Reed data file not found!")
        return
    
    reed_df = pd.read_excel(reed_path)
    print(f"Reed dataset: {len(reed_df):,} total records")
    
    # Load Ulterra data
    output_folder = Path("Output")
    ulterra_files = list(output_folder.glob("Ulterra_Merged_*.xlsx"))
    
    if not ulterra_files:
        print("Ulterra merged file not found! Please run merge script first.")
        return
    
    ulterra_file = max(ulterra_files, key=lambda x: x.stat().st_mtime)
    ulterra_df = pd.read_excel(ulterra_file)
    print(f"Ulterra dataset: {len(ulterra_df):,} total records")
    
    # Filter Reed data for Reed Hycalog and Ulterra
    reed_bits = reed_df[reed_df['Bit Mfg'] == 'REEDHYCALOG'].copy()
    ulterra_bits_from_reed = reed_df[reed_df['Bit Mfg'] == 'ULTERRA'].copy()
    
    print(f"\nFrom Reed dataset:")
    print(f"  Reed Hycalog bits: {len(reed_bits):,}")
    print(f"  Ulterra bits: {len(ulterra_bits_from_reed):,}")
    
    # TD bits from Ulterra dataset
    ulterra_td_bits = ulterra_df[ulterra_df['DullGrade_Reason'] == 'TD'].copy()
    print(f"\nFrom Ulterra dataset:")
    print(f"  TD bits: {len(ulterra_td_bits):,}")
    
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)
    
    # Performance comparison
    comparison_data = []
    
    # Reed Hycalog performance
    reed_distance = reed_bits['Distance'].mean()
    reed_rop = reed_bits['ROP'].mean()
    reed_hours = reed_bits['Hrs'].mean()
    
    comparison_data.append({
        'Manufacturer': 'Reed Hycalog',
        'Source': 'Reed Dataset',
        'Count': len(reed_bits),
        'Avg_Distance_m': reed_distance,
        'Avg_ROP_m_hr': reed_rop,
        'Avg_Hours': reed_hours,
        'Max_Distance_m': reed_bits['Distance'].max(),
        'Max_ROP_m_hr': reed_bits['ROP'].max()
    })
    
    # Ulterra from Reed dataset
    ulterra_reed_distance = ulterra_bits_from_reed['Distance'].mean()
    ulterra_reed_rop = ulterra_bits_from_reed['ROP'].mean()
    ulterra_reed_hours = ulterra_bits_from_reed['Hrs'].mean()
    
    comparison_data.append({
        'Manufacturer': 'Ulterra',
        'Source': 'Reed Dataset',
        'Count': len(ulterra_bits_from_reed),
        'Avg_Distance_m': ulterra_reed_distance,
        'Avg_ROP_m_hr': ulterra_reed_rop,
        'Avg_Hours': ulterra_reed_hours,
        'Max_Distance_m': ulterra_bits_from_reed['Distance'].max(),
        'Max_ROP_m_hr': ulterra_bits_from_reed['ROP'].max()
    })
    
    # Ulterra TD bits from Ulterra dataset
    if 'DepthDrilled (m)' in ulterra_td_bits.columns:
        ulterra_td_distance = ulterra_td_bits['DepthDrilled (m)'].mean()
        ulterra_td_rop = ulterra_td_bits['ROP (m/hr)'].mean()
        ulterra_td_hours = ulterra_td_bits['DrillingHours'].mean()
        
        comparison_data.append({
            'Manufacturer': 'Ulterra TD',
            'Source': 'Ulterra Dataset',
            'Count': len(ulterra_td_bits),
            'Avg_Distance_m': ulterra_td_distance,
            'Avg_ROP_m_hr': ulterra_td_rop,
            'Avg_Hours': ulterra_td_hours,
            'Max_Distance_m': ulterra_td_bits['DepthDrilled (m)'].max(),
            'Max_ROP_m_hr': ulterra_td_bits['ROP (m/hr)'].max()
        })
    
    # Create comparison DataFrame
    comparison_df = pd.DataFrame(comparison_data)
    
    print("\nDetailed Performance Comparison:")
    print("-" * 80)
    print(f"{'Manufacturer':<15} {'Source':<15} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10} {'Avg Hrs':<10}")
    print("-" * 80)
    
    for _, row in comparison_df.iterrows():
        print(f"{row['Manufacturer']:<15} {row['Source']:<15} {row['Count']:<8,} "
              f"{row['Avg_Distance_m']:<10.0f} {row['Avg_ROP_m_hr']:<10.1f} {row['Avg_Hours']:<10.1f}")
    
    # Performance insights
    print("\n" + "=" * 60)
    print("KEY PERFORMANCE INSIGHTS")
    print("=" * 60)
    
    insights = []
    
    # Compare Reed vs Ulterra from same dataset
    if ulterra_reed_rop > reed_rop:
        rop_diff = ((ulterra_reed_rop - reed_rop) / reed_rop) * 100
        insights.append(f"1. Ulterra shows {rop_diff:.1f}% higher ROP than Reed Hycalog in Montney")
    else:
        rop_diff = ((reed_rop - ulterra_reed_rop) / ulterra_reed_rop) * 100
        insights.append(f"1. Reed Hycalog shows {rop_diff:.1f}% higher ROP than Ulterra in Montney")
    
    # Distance comparison
    if ulterra_reed_distance > reed_distance:
        dist_diff = ((ulterra_reed_distance - reed_distance) / reed_distance) * 100
        insights.append(f"2. Ulterra drills {dist_diff:.1f}% more distance per run than Reed Hycalog")
    else:
        dist_diff = ((reed_distance - ulterra_reed_distance) / ulterra_reed_distance) * 100
        insights.append(f"2. Reed Hycalog drills {dist_diff:.1f}% more distance per run than Ulterra")
    
    # Market share insight
    total_reed_ulterra = len(reed_bits) + len(ulterra_bits_from_reed)
    reed_share = len(reed_bits) / total_reed_ulterra * 100
    ulterra_share = len(ulterra_bits_from_reed) / total_reed_ulterra * 100
    
    insights.append(f"3. Market share in Montney: Reed {reed_share:.1f}% vs Ulterra {ulterra_share:.1f}%")
    
    # Technology advancement
    insights.append(f"4. Reed Hycalog max ROP: {reed_bits['ROP'].max():.1f} m/hr")
    insights.append(f"5. Ulterra max ROP: {ulterra_bits_from_reed['ROP'].max():.1f} m/hr")
    
    for insight in insights:
        print(f"  {insight}")
    
    # Yearly trends comparison
    print("\n" + "=" * 60)
    print("YEARLY PERFORMANCE TRENDS (2020-2025)")
    print("=" * 60)
    
    # Analyze yearly trends for both manufacturers
    print("\nReed Hycalog Yearly Performance:")
    reed_yearly = reed_bits.groupby('Year').agg({
        'Distance': ['count', 'mean'],
        'ROP': 'mean',
        'Hrs': 'mean'
    }).round(1)
    
    for year in sorted(reed_bits['Year'].unique()):
        if year >= 2020:
            year_data = reed_bits[reed_bits['Year'] == year]
            count = len(year_data)
            avg_dist = year_data['Distance'].mean()
            avg_rop = year_data['ROP'].mean()
            max_rop = year_data['ROP'].max()
            
            print(f"  {year}: {count:,} bits - Avg Distance: {avg_dist:.0f}m, "
                  f"Avg ROP: {avg_rop:.1f} m/hr, Max ROP: {max_rop:.1f} m/hr")
    
    print("\nUlterra Yearly Performance (from Reed dataset):")
    for year in sorted(ulterra_bits_from_reed['Year'].unique()):
        if year >= 2020:
            year_data = ulterra_bits_from_reed[ulterra_bits_from_reed['Year'] == year]
            count = len(year_data)
            avg_dist = year_data['Distance'].mean()
            avg_rop = year_data['ROP'].mean()
            max_rop = year_data['ROP'].max()
            
            print(f"  {year}: {count:,} bits - Avg Distance: {avg_dist:.0f}m, "
                  f"Avg ROP: {avg_rop:.1f} m/hr, Max ROP: {max_rop:.1f} m/hr")
    
    # Bit size comparison
    print("\n" + "=" * 60)
    print("BIT SIZE PERFORMANCE COMPARISON")
    print("=" * 60)
    
    # Common bit sizes
    common_sizes = ['158.8', '171.5', '222.3']
    
    for size in common_sizes:
        print(f"\n{size}mm Bit Size Comparison:")
        
        # Reed performance for this size
        reed_size = reed_bits[reed_bits['Bit Size'] == float(size)]
        if len(reed_size) > 0:
            print(f"  Reed Hycalog: {len(reed_size)} bits - "
                  f"Avg ROP: {reed_size['ROP'].mean():.1f} m/hr, "
                  f"Avg Distance: {reed_size['Distance'].mean():.0f}m")
        
        # Ulterra performance for this size
        ulterra_size = ulterra_bits_from_reed[ulterra_bits_from_reed['Bit Size'] == float(size)]
        if len(ulterra_size) > 0:
            print(f"  Ulterra: {len(ulterra_size)} bits - "
                  f"Avg ROP: {ulterra_size['ROP'].mean():.1f} m/hr, "
                  f"Avg Distance: {ulterra_size['Distance'].mean():.0f}m")
    
    # Save comprehensive comparison report
    create_comparison_report(comparison_df, reed_bits, ulterra_bits_from_reed, ulterra_td_bits)
    
    print("\n" + "=" * 60)
    print("COMPARISON ANALYSIS COMPLETE")
    print("=" * 60)

def create_comparison_report(comparison_df, reed_bits, ulterra_bits, ulterra_td_bits):
    """
    Create detailed Excel comparison report
    """
    
    output_path = Path("Output") / f"Reed_vs_Ulterra_Comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: Performance Summary
        comparison_df.to_excel(writer, sheet_name='Performance_Summary', index=False)
        
        # Sheet 2: Reed Hycalog Data Sample
        reed_sample = reed_bits.head(1000)  # First 1000 records
        reed_sample.to_excel(writer, sheet_name='Reed_Hycalog_Sample', index=False)
        
        # Sheet 3: Ulterra Data Sample
        ulterra_sample = ulterra_bits.head(1000)
        ulterra_sample.to_excel(writer, sheet_name='Ulterra_Sample', index=False)
        
        # Sheet 4: Yearly Comparison
        yearly_comparison = []
        
        for year in range(2020, 2026):
            # Reed data for this year
            reed_year = reed_bits[reed_bits['Year'] == year]
            if len(reed_year) > 0:
                yearly_comparison.append({
                    'Year': year,
                    'Manufacturer': 'Reed Hycalog',
                    'Count': len(reed_year),
                    'Avg_Distance': reed_year['Distance'].mean(),
                    'Avg_ROP': reed_year['ROP'].mean(),
                    'Max_ROP': reed_year['ROP'].max(),
                    'Avg_Hours': reed_year['Hrs'].mean()
                })
            
            # Ulterra data for this year
            ulterra_year = ulterra_bits[ulterra_bits['Year'] == year]
            if len(ulterra_year) > 0:
                yearly_comparison.append({
                    'Year': year,
                    'Manufacturer': 'Ulterra',
                    'Count': len(ulterra_year),
                    'Avg_Distance': ulterra_year['Distance'].mean(),
                    'Avg_ROP': ulterra_year['ROP'].mean(),
                    'Max_ROP': ulterra_year['ROP'].max(),
                    'Avg_Hours': ulterra_year['Hrs'].mean()
                })
        
        yearly_df = pd.DataFrame(yearly_comparison)
        yearly_df.to_excel(writer, sheet_name='Yearly_Trends', index=False)
        
        # Sheet 5: Bit Size Analysis
        bit_size_analysis = []
        
        common_sizes = [158.8, 171.5, 222.3]
        
        for size in common_sizes:
            # Reed analysis
            reed_size = reed_bits[reed_bits['Bit Size'] == size]
            if len(reed_size) > 0:
                bit_size_analysis.append({
                    'Bit_Size': size,
                    'Manufacturer': 'Reed Hycalog',
                    'Count': len(reed_size),
                    'Avg_Distance': reed_size['Distance'].mean(),
                    'Avg_ROP': reed_size['ROP'].mean(),
                    'Max_ROP': reed_size['ROP'].max()
                })
            
            # Ulterra analysis
            ulterra_size = ulterra_bits[ulterra_bits['Bit Size'] == size]
            if len(ulterra_size) > 0:
                bit_size_analysis.append({
                    'Bit_Size': size,
                    'Manufacturer': 'Ulterra',
                    'Count': len(ulterra_size),
                    'Avg_Distance': ulterra_size['Distance'].mean(),
                    'Avg_ROP': ulterra_size['ROP'].mean(),
                    'Max_ROP': ulterra_size['ROP'].max()
                })
        
        bit_size_df = pd.DataFrame(bit_size_analysis)
        bit_size_df.to_excel(writer, sheet_name='Bit_Size_Analysis', index=False)
    
    print(f"\nComparison report saved to: {output_path}")

if __name__ == "__main__":
    compare_reed_vs_ulterra()
