import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def enhanced_td_analysis_with_iadc():
    """
    Enhanced TD bits analysis incorporating IADC standards and bit grading knowledge
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
    td_bits = df[df['DullGrade_Reason'] == 'TD'].copy()
    
    print(f"\n{'='*80}")
    print("ENHANCED TD BITS ANALYSIS WITH IADC STANDARDS")
    print(f"{'='*80}")
    print(f"Total records: {len(df):,}")
    print(f"TD (Total Depth) bits: {len(td_bits):,} ({len(td_bits)/len(df)*100:.1f}%)")
    
    # IADC Dull Grading Context
    print(f"\n{'='*80}")
    print("IADC DULL GRADING CONTEXT")
    print(f"{'='*80}")
    
    print("""
TD (Total Depth) - According to IADC standards, TD means the bit was pulled because:
• The planned total depth was reached
• The casing depth was reached
• The drilling objective was completed
• This is typically a PLANNED pull, not due to bit failure
    
This explains why TD bits often show good performance - they were pulled
at completion, not due to wear or damage.
    """)
    
    # Analyze other dull grade reasons for context
    print("\nDull Grade Reason Analysis:")
    dull_reasons = df['DullGrade_Reason'].value_counts()
    
    # IADC Dull Grade Reason explanations
    dull_explanations = {
        'TD': 'Total Depth/Casing Depth - Planned completion',
        'BHA': 'Change Bottom Hole Assembly - Operational change',
        'PR': 'Penetration Rate - Poor drilling performance',
        'DTF': 'Downhole Tool Failure - Equipment failure',
        'DMF': 'Downhole Motor Failure - Motor problems',
        'HP': 'Hole Problems - Wellbore issues',
        'TQ': 'Torque - High torque conditions',
        'PP': 'Pump Pressure - Pressure limitations',
        'LIH': 'Left In Hole - Bit stuck/lost',
        'RIG': 'Rig Repair - Rig maintenance'
    }
    
    for reason, count in dull_reasons.head(10).items():
        explanation = dull_explanations.get(reason, 'See IADC standards for details')
        percentage = count / len(df) * 100
        print(f"  {reason}: {count:,} bits ({percentage:.1f}%) - {explanation}")
    
    # TD Bit Performance Analysis
    print(f"\n{'='*80}")
    print("TD BIT PERFORMANCE CHARACTERISTICS")
    print(f"{'='*80}")
    
    # Key insight: TD bits should show GOOD performance since they completed their job
    performance_metrics = {
        'DepthDrilled (m)': 'Meters drilled per bit run',
        'ROP (m/hr)': 'Rate of Penetration',
        'DrillingHours': 'Total drilling time',
        'OnBottomROP (m/hr)': 'ROP while actively drilling'
    }
    
    print("\nPerformance Summary for TD Bits:")
    for metric, description in performance_metrics.items():
        if metric in td_bits.columns:
            values = td_bits[metric].dropna()
            if len(values) > 10:
                print(f"\n{metric} ({description}):")
                print(f"  Count: {len(values):,}")
                print(f"  Mean: {values.mean():.2f}")
                print(f"  Median: {values.median():.2f}")
                print(f"  75th Percentile: {values.quantile(0.75):.2f}")
                print(f"  90th Percentile: {values.quantile(0.90):.2f}")
                print(f"  Maximum: {values.max():.2f}")
    
    # Bit Type Analysis (if available)
    print(f"\n{'='*80}")
    print("BIT TYPE AND CLASSIFICATION ANALYSIS")
    print(f"{'='*80}")
    
    # Look for bit type information
    bit_type_columns = [col for col in td_bits.columns if any(word in col.lower() 
                       for word in ['type', 'class', 'iadc', 'body', 'matrix', 'steel', 'pdc'])]
    
    if bit_type_columns:
        print(f"Available bit classification columns: {bit_type_columns}")
        
        for col in bit_type_columns[:3]:  # Analyze top 3 relevant columns
            if td_bits[col].notna().sum() > 10:
                print(f"\n{col} Distribution for TD Bits:")
                type_counts = td_bits[col].value_counts().head(10)
                for bit_type, count in type_counts.items():
                    print(f"  {bit_type}: {count} bits ({count/len(td_bits)*100:.1f}%)")
    else:
        print("No specific bit type classification columns found.")
    
    # Bit Size Analysis with Industry Context
    print(f"\n{'='*80}")
    print("BIT SIZE ANALYSIS WITH INDUSTRY STANDARDS")
    print(f"{'='*80}")
    
    if 'BitSize' in td_bits.columns:
        bit_sizes = td_bits['BitSize'].value_counts().head(10)
        
        print("Bit Size Distribution for TD Bits:")
        print("(Common sizes: 156mm=6.125\", 159mm=6.25\", 171mm=6.75\", 222mm=8.75\")")
        
        for size, count in bit_sizes.items():
            # Calculate performance by bit size
            size_data = td_bits[td_bits['BitSize'] == size]
            avg_depth = size_data['DepthDrilled (m)'].mean() if 'DepthDrilled (m)' in size_data.columns else 0
            avg_rop = size_data['ROP (m/hr)'].mean() if 'ROP (m/hr)' in size_data.columns else 0
            
            print(f"\n  {size}: {count} bits ({count/len(td_bits)*100:.1f}%)")
            if avg_depth > 0:
                print(f"    Average Depth Drilled: {avg_depth:.1f}m")
            if avg_rop > 0:
                print(f"    Average ROP: {avg_rop:.1f} m/hr")
    
    # Formation Analysis
    print(f"\n{'='*80}")
    print("FORMATION AND GEOLOGICAL ANALYSIS")
    print(f"{'='*80}")
    
    # Look for formation-related columns
    formation_columns = [col for col in td_bits.columns if any(word in col.lower() 
                        for word in ['formation', 'geology', 'rock', 'strength', 'hardness'])]
    
    if formation_columns:
        print(f"Formation-related columns found: {formation_columns}")
        
        for col in formation_columns[:2]:  # Analyze top 2 formation columns
            if td_bits[col].notna().sum() > 5:
                print(f"\n{col} for TD Bits:")
                form_counts = td_bits[col].value_counts().head(8)
                for formation, count in form_counts.items():
                    print(f"  {formation}: {count} bits")
    else:
        print("No specific formation classification columns found.")
    
    # Performance Benchmarking
    print(f"\n{'='*80}")
    print("TD vs OTHER DULL REASONS - PERFORMANCE COMPARISON")
    print(f"{'='*80}")
    
    print("Expected: TD bits should show GOOD performance since they completed their job")
    print("vs. other reasons which may indicate problems or limitations\n")
    
    # Compare TD vs other common reasons
    comparison_reasons = ['BHA', 'PR', 'DTF', 'DMF']
    metrics_to_compare = ['DepthDrilled (m)', 'ROP (m/hr)', 'DrillingHours']
    
    print(f"{'Metric':<20} {'TD':<12} {'BHA':<12} {'PR':<12} {'Performance':<15}")
    print("-" * 75)
    
    for metric in metrics_to_compare:
        if metric in df.columns:
            row = f"{metric:<20}"
            
            td_mean = td_bits[metric].mean()
            row += f"{td_mean:<12.1f}"
            
            # Compare with BHA and PR
            for reason in ['BHA', 'PR']:
                if reason in df['DullGrade_Reason'].values:
                    other_data = df[df['DullGrade_Reason'] == reason][metric].dropna()
                    if len(other_data) > 0:
                        other_mean = other_data.mean()
                        row += f"{other_mean:<12.1f}"
                    else:
                        row += f"{'N/A':<12}"
                else:
                    row += f"{'N/A':<12}"
            
            # Performance assessment
            if metric == 'ROP (m/hr)':
                if td_mean > 25:
                    performance = "Good"
                elif td_mean > 20:
                    performance = "Average"
                else:
                    performance = "Below Avg"
            elif metric == 'DepthDrilled (m)':
                if td_mean > 1200:
                    performance = "Good"
                elif td_mean > 800:
                    performance = "Average" 
                else:
                    performance = "Below Avg"
            else:
                performance = "Variable"
            
            row += f"{performance:<15}"
            print(row)
    
    # Year-over-Year Trends with Industry Context
    print(f"\n{'='*80}")
    print("YEARLY TRENDS WITH INDUSTRY EVOLUTION")
    print(f"{'='*80}")
    
    if 'SpudDate' in td_bits.columns:
        td_bits['SpudDate'] = pd.to_datetime(td_bits['SpudDate'], errors='coerce')
        td_bits['SpudYear'] = td_bits['SpudDate'].dt.year
        
        print("TD Bit Performance Evolution (2020+):")
        print("(Reflects technology improvements and operational optimization)")
        
        for year in sorted(td_bits['SpudYear'].dropna().unique()):
            if year >= 2020:
                year_data = td_bits[td_bits['SpudYear'] == year]
                
                count = len(year_data)
                avg_depth = year_data['DepthDrilled (m)'].mean()
                max_depth = year_data['DepthDrilled (m)'].max()
                avg_rop = year_data['ROP (m/hr)'].mean()
                max_rop = year_data['ROP (m/hr)'].max()
                
                print(f"\n  {int(year)}: {count} TD bits")
                print(f"    Depth - Average: {avg_depth:.1f}m, Maximum: {max_depth:.1f}m")
                print(f"    ROP - Average: {avg_rop:.1f} m/hr, Maximum: {max_rop:.1f} m/hr")
                
                # Technology trend insights
                if year == 2025 and count > 10:
                    print(f"    → 2025 showing improved performance trends")
                elif year >= 2023 and avg_rop > 30:
                    print(f"    → Strong performance indicating technology advancement")
    
    # Key Insights and Recommendations
    print(f"\n{'='*80}")
    print("KEY INSIGHTS AND RECOMMENDATIONS")
    print(f"{'='*80}")
    
    insights = []
    
    # Overall TD performance
    avg_rop = td_bits['ROP (m/hr)'].mean()
    avg_depth = td_bits['DepthDrilled (m)'].mean()
    
    insights.append(f"1. TD bits show average ROP of {avg_rop:.1f} m/hr - {'Good' if avg_rop > 25 else 'Moderate'} performance")
    insights.append(f"2. Average depth per TD bit: {avg_depth:.0f}m - indicating successful completion runs")
    insights.append(f"3. TD represents {len(td_bits)/len(df)*100:.1f}% of all bits - significant portion of planned completions")
    
    # Performance comparison
    if 'BHA' in df['DullGrade_Reason'].values:
        bha_rop = df[df['DullGrade_Reason'] == 'BHA']['ROP (m/hr)'].mean()
        if avg_rop < bha_rop:
            diff = ((bha_rop - avg_rop) / avg_rop) * 100
            insights.append(f"4. TD bits show {diff:.1f}% lower ROP than BHA changes - may indicate end-of-run conditions")
    
    # Technology trends
    if 'SpudDate' in td_bits.columns:
        recent_data = td_bits[td_bits['SpudYear'] >= 2023]
        if len(recent_data) > 20:
            recent_rop = recent_data['ROP (m/hr)'].mean()
            insights.append(f"5. Recent TD bits (2023+) show {recent_rop:.1f} m/hr average - technology improvements evident")
    
    # Bit size insights
    if 'BitSize' in td_bits.columns:
        dominant_size = td_bits['BitSize'].mode().iloc[0]
        dominant_count = (td_bits['BitSize'] == dominant_size).sum()
        insights.append(f"6. {dominant_size} is most common TD bit size ({dominant_count} bits) - field optimization")
    
    for insight in insights:
        print(f"  {insight}")
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE - Based on IADC Standards")
    print(f"{'='*80}")
    
    # Create enhanced report
    create_enhanced_report(td_bits, df, latest_file.stem)

def create_enhanced_report(td_bits, all_bits, filename_base):
    """
    Create enhanced Excel report with IADC context
    """
    output_path = Path("Output") / f"Enhanced_TD_Analysis_{filename_base}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Sheet 1: TD Bits Raw Data
        td_bits.to_excel(writer, sheet_name='TD_Bits_Data', index=False)
        
        # Sheet 2: IADC Dull Reason Analysis
        dull_summary = []
        dull_reasons = all_bits['DullGrade_Reason'].value_counts()
        
        dull_explanations = {
            'TD': 'Total Depth/Casing Depth - Planned completion',
            'BHA': 'Change Bottom Hole Assembly - Operational change',
            'PR': 'Penetration Rate - Poor drilling performance',
            'DTF': 'Downhole Tool Failure - Equipment failure',
            'DMF': 'Downhole Motor Failure - Motor problems',
            'HP': 'Hole Problems - Wellbore issues',
            'TQ': 'Torque - High torque conditions',
            'PP': 'Pump Pressure - Pressure limitations',
            'LIH': 'Left In Hole - Bit stuck/lost',
            'RIG': 'Rig Repair - Rig maintenance'
        }
        
        for reason, count in dull_reasons.items():
            dull_summary.append({
                'Dull_Reason': reason,
                'Count': count,
                'Percentage': count / len(all_bits) * 100,
                'IADC_Explanation': dull_explanations.get(reason, 'See IADC standards'),
                'Avg_ROP': all_bits[all_bits['DullGrade_Reason'] == reason]['ROP (m/hr)'].mean(),
                'Avg_Depth': all_bits[all_bits['DullGrade_Reason'] == reason]['DepthDrilled (m)'].mean()
            })
        
        pd.DataFrame(dull_summary).to_excel(writer, sheet_name='IADC_Dull_Analysis', index=False)
        
        # Sheet 3: TD Performance by Bit Size
        if 'BitSize' in td_bits.columns:
            size_analysis = td_bits.groupby('BitSize').agg({
                'DepthDrilled (m)': ['count', 'mean', 'median', 'max'],
                'ROP (m/hr)': ['mean', 'median', 'max'],
                'DrillingHours': ['mean', 'median', 'max']
            }).round(2)
            
            size_analysis.to_excel(writer, sheet_name='TD_by_Bit_Size')
        
        # Sheet 4: Yearly Evolution
        if 'SpudDate' in td_bits.columns:
            td_copy = td_bits.copy()
            td_copy['SpudDate'] = pd.to_datetime(td_copy['SpudDate'], errors='coerce')
            td_copy['SpudYear'] = td_copy['SpudDate'].dt.year
            
            yearly_evolution = td_copy.groupby('SpudYear').agg({
                'DepthDrilled (m)': ['count', 'mean', 'max'],
                'ROP (m/hr)': ['mean', 'max'],
                'DrillingHours': ['mean', 'max']
            }).round(2)
            
            yearly_evolution.to_excel(writer, sheet_name='Yearly_Evolution')
    
    print(f"\nEnhanced analysis saved to: {output_path}")

if __name__ == "__main__":
    enhanced_td_analysis_with_iadc()
