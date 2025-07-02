import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def compare_reed_vs_ulterra_corrected():
    """
    Compare Reed Hycalog vs Ulterra drilling performance - corrected version
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
    
    # Parse spud dates and add year column
    reed_df['Spud_Date'] = pd.to_datetime(reed_df['Spud'], errors='coerce')
    reed_df['Spud_Year'] = reed_df['Spud_Date'].dt.year
    
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
    
    print("\n" + "=" * 80)
    print("DETAILED PERFORMANCE COMPARISON")
    print("=" * 80)
    
    # Performance comparison table
    print(f"{'Manufacturer':<20} {'Dataset':<15} {'Count':<8} {'Avg Dist (m)':<12} {'Avg ROP':<10} {'Max ROP':<10} {'Avg Hrs':<10}")
    print("-" * 85)
    
    # Reed Hycalog performance
    reed_distance = reed_bits['Distance'].mean()
    reed_rop = reed_bits['ROP'].mean()
    reed_hours = reed_bits['Hrs'].mean()
    reed_max_rop = reed_bits['ROP'].max()
    
    print(f"{'Reed Hycalog':<20} {'Reed Data':<15} {len(reed_bits):<8,} "
          f"{reed_distance:<12.0f} {reed_rop:<10.1f} {reed_max_rop:<10.1f} {reed_hours:<10.1f}")
    
    # Ulterra from Reed dataset
    ulterra_reed_distance = ulterra_bits_from_reed['Distance'].mean()
    ulterra_reed_rop = ulterra_bits_from_reed['ROP'].mean()
    ulterra_reed_hours = ulterra_bits_from_reed['Hrs'].mean()
    ulterra_reed_max_rop = ulterra_bits_from_reed['ROP'].max()
    
    print(f"{'Ulterra':<20} {'Reed Data':<15} {len(ulterra_bits_from_reed):<8,} "
          f"{ulterra_reed_distance:<12.0f} {ulterra_reed_rop:<10.1f} {ulterra_reed_max_rop:<10.1f} {ulterra_reed_hours:<10.1f}")
    
    # Ulterra TD bits from Ulterra dataset
    if 'DepthDrilled (m)' in ulterra_td_bits.columns:
        ulterra_td_distance = ulterra_td_bits['DepthDrilled (m)'].mean()
        ulterra_td_rop = ulterra_td_bits['ROP (m/hr)'].mean()
        ulterra_td_hours = ulterra_td_bits['DrillingHours'].mean()
        ulterra_td_max_rop = ulterra_td_bits['ROP (m/hr)'].max()
        
        print(f"{'Ulterra TD':<20} {'Ulterra Data':<15} {len(ulterra_td_bits):<8,} "
              f"{ulterra_td_distance:<12.0f} {ulterra_td_rop:<10.1f} {ulterra_td_max_rop:<10.1f} {ulterra_td_hours:<10.1f}")
    
    # Key insights
    print("\n" + "=" * 80)
    print("KEY PERFORMANCE INSIGHTS")
    print("=" * 80)
    
    # Performance comparison
    rop_advantage = ((ulterra_reed_rop - reed_rop) / reed_rop) * 100
    distance_advantage = ((reed_distance - ulterra_reed_distance) / ulterra_reed_distance) * 100
    
    total_reed_ulterra = len(reed_bits) + len(ulterra_bits_from_reed)
    reed_share = len(reed_bits) / total_reed_ulterra * 100
    ulterra_share = len(ulterra_bits_from_reed) / total_reed_ulterra * 100
    
    insights = [
        f"1. ROP Performance: Ulterra {rop_advantage:+.1f}% vs Reed Hycalog ({ulterra_reed_rop:.1f} vs {reed_rop:.1f} m/hr)",
        f"2. Distance per Run: Reed Hycalog {distance_advantage:+.1f}% vs Ulterra ({reed_distance:.0f} vs {ulterra_reed_distance:.0f}m)",
        f"3. Market Share in Montney: Reed {reed_share:.1f}% vs Ulterra {ulterra_share:.1f}%",
        f"4. Peak Performance: Reed {reed_max_rop:.1f} m/hr vs Ulterra {ulterra_reed_max_rop:.1f} m/hr",
        f"5. Drilling Time: Reed {reed_hours:.1f}h vs Ulterra {ulterra_reed_hours:.1f}h average"
    ]
    
    for insight in insights:
        print(f"  {insight}")
    
    # Yearly trends
    print("\n" + "=" * 80)
    print("YEARLY PERFORMANCE TRENDS (2020-2025)")
    print("=" * 80)
    
    print(f"{'Year':<6} {'Manufacturer':<15} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10} {'Max ROP':<10}")
    print("-" * 65)
    
    # Analyze yearly trends for both manufacturers
    for year in range(2020, 2026):
        # Reed data for this year
        reed_year = reed_bits[reed_bits['Spud_Year'] == year]
        if len(reed_year) > 0:
            print(f"{year:<6} {'Reed Hycalog':<15} {len(reed_year):<8,} "
                  f"{reed_year['Distance'].mean():<10.0f} {reed_year['ROP'].mean():<10.1f} {reed_year['ROP'].max():<10.1f}")
        
        # Ulterra data for this year
        ulterra_year = ulterra_bits_from_reed[ulterra_bits_from_reed['Spud_Year'] == year]
        if len(ulterra_year) > 0:
            print(f"{year:<6} {'Ulterra':<15} {len(ulterra_year):<8,} "
                  f"{ulterra_year['Distance'].mean():<10.0f} {ulterra_year['ROP'].mean():<10.1f} {ulterra_year['ROP'].max():<10.1f}")
    
    # Bit size performance
    print("\n" + "=" * 80)
    print("BIT SIZE PERFORMANCE ANALYSIS")
    print("=" * 80)
    
    # Common bit sizes
    common_sizes = [158.8, 171.5, 222.3]
    
    for size in common_sizes:
        print(f"\n{size}mm Bit Size Performance:")
        print(f"{'Manufacturer':<15} {'Count':<8} {'Avg Dist':<10} {'Avg ROP':<10} {'Max ROP':<10}")
        print("-" * 55)
        
        # Reed performance for this size
        reed_size = reed_bits[reed_bits['Bit Size'] == size]
        if len(reed_size) > 0:
            print(f"{'Reed Hycalog':<15} {len(reed_size):<8,} "
                  f"{reed_size['Distance'].mean():<10.0f} {reed_size['ROP'].mean():<10.1f} {reed_size['ROP'].max():<10.1f}")
        
        # Ulterra performance for this size
        ulterra_size = ulterra_bits_from_reed[ulterra_bits_from_reed['Bit Size'] == size]
        if len(ulterra_size) > 0:
            print(f"{'Ulterra':<15} {len(ulterra_size):<8,} "
                  f"{ulterra_size['Distance'].mean():<10.0f} {ulterra_size['ROP'].mean():<10.1f} {ulterra_size['ROP'].max():<10.1f}")
    
    # Operator analysis
    print("\n" + "=" * 80)
    print("TOP OPERATORS USING EACH MANUFACTURER")
    print("=" * 80)
    
    print("\nTop 5 Operators - Reed Hycalog:")
    reed_operators = reed_bits['Operator'].value_counts().head(5)
    for operator, count in reed_operators.items():
        avg_rop = reed_bits[reed_bits['Operator'] == operator]['ROP'].mean()
        print(f"  {operator}: {count:,} bits (Avg ROP: {avg_rop:.1f} m/hr)")
    
    print("\nTop 5 Operators - Ulterra:")
    ulterra_operators = ulterra_bits_from_reed['Operator'].value_counts().head(5)
    for operator, count in ulterra_operators.items():
        avg_rop = ulterra_bits_from_reed[ulterra_bits_from_reed['Operator'] == operator]['ROP'].mean()
        print(f"  {operator}: {count:,} bits (Avg ROP: {avg_rop:.1f} m/hr)")
    
    # Technology trends analysis
    print("\n" + "=" * 80)
    print("TECHNOLOGY EVOLUTION ANALYSIS")
    print("=" * 80)
    
    # Recent performance (2023+)
    recent_reed = reed_bits[reed_bits['Spud_Year'] >= 2023]
    recent_ulterra = ulterra_bits_from_reed[ulterra_bits_from_reed['Spud_Year'] >= 2023]
    
    if len(recent_reed) > 0 and len(recent_ulterra) > 0:
        print(f"\nRecent Performance (2023+):")
        print(f"  Reed Hycalog: {len(recent_reed):,} bits - "
              f"Avg ROP: {recent_reed['ROP'].mean():.1f} m/hr, "
              f"Max ROP: {recent_reed['ROP'].max():.1f} m/hr")
        print(f"  Ulterra: {len(recent_ulterra):,} bits - "
              f"Avg ROP: {recent_ulterra['ROP'].mean():.1f} m/hr, "
              f"Max ROP: {recent_ulterra['ROP'].max():.1f} m/hr")
    
    # Save comprehensive report
    save_comparison_report(reed_bits, ulterra_bits_from_reed, ulterra_td_bits)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE - Report saved to Output folder")
    print("=" * 80)

def save_comparison_report(reed_bits, ulterra_bits, ulterra_td_bits):
    """Save detailed comparison report to Excel"""
    
    output_path = Path("Output") / f"Reed_vs_Ulterra_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Performance summary
        summary_data = []
        
        # Reed summary
        summary_data.append({
            'Manufacturer': 'Reed Hycalog',
            'Dataset': 'Reed Montney',
            'Count': len(reed_bits),
            'Avg_Distance_m': reed_bits['Distance'].mean(),
            'Avg_ROP_m_hr': reed_bits['ROP'].mean(),
            'Max_ROP_m_hr': reed_bits['ROP'].max(),
            'Avg_Hours': reed_bits['Hrs'].mean(),
            'Total_Distance_km': reed_bits['Distance'].sum() / 1000
        })
        
        # Ulterra summary
        summary_data.append({
            'Manufacturer': 'Ulterra',
            'Dataset': 'Reed Montney',
            'Count': len(ulterra_bits),
            'Avg_Distance_m': ulterra_bits['Distance'].mean(),
            'Avg_ROP_m_hr': ulterra_bits['ROP'].mean(),
            'Max_ROP_m_hr': ulterra_bits['ROP'].max(),
            'Avg_Hours': ulterra_bits['Hrs'].mean(),
            'Total_Distance_km': ulterra_bits['Distance'].sum() / 1000
        })
        
        # Ulterra TD summary
        if 'DepthDrilled (m)' in ulterra_td_bits.columns:
            summary_data.append({
                'Manufacturer': 'Ulterra TD',
                'Dataset': 'Ulterra Dataset',
                'Count': len(ulterra_td_bits),
                'Avg_Distance_m': ulterra_td_bits['DepthDrilled (m)'].mean(),
                'Avg_ROP_m_hr': ulterra_td_bits['ROP (m/hr)'].mean(),
                'Max_ROP_m_hr': ulterra_td_bits['ROP (m/hr)'].max(),
                'Avg_Hours': ulterra_td_bits['DrillingHours'].mean(),
                'Total_Distance_km': ulterra_td_bits['DepthDrilled (m)'].sum() / 1000
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Performance_Summary', index=False)
        
        # Yearly analysis
        yearly_data = []
        for year in range(2020, 2026):
            # Reed data
            reed_year = reed_bits[reed_bits['Spud_Year'] == year]
            if len(reed_year) > 0:
                yearly_data.append({
                    'Year': year,
                    'Manufacturer': 'Reed Hycalog',
                    'Count': len(reed_year),
                    'Avg_Distance': reed_year['Distance'].mean(),
                    'Avg_ROP': reed_year['ROP'].mean(),
                    'Max_ROP': reed_year['ROP'].max()
                })
            
            # Ulterra data
            ulterra_year = ulterra_bits[ulterra_bits['Spud_Year'] == year]
            if len(ulterra_year) > 0:
                yearly_data.append({
                    'Year': year,
                    'Manufacturer': 'Ulterra',
                    'Count': len(ulterra_year),
                    'Avg_Distance': ulterra_year['Distance'].mean(),
                    'Avg_ROP': ulterra_year['ROP'].mean(),
                    'Max_ROP': ulterra_year['ROP'].max()
                })
        
        yearly_df = pd.DataFrame(yearly_data)
        yearly_df.to_excel(writer, sheet_name='Yearly_Trends', index=False)
        
        # Raw data samples
        reed_bits.head(1000).to_excel(writer, sheet_name='Reed_Sample', index=False)
        ulterra_bits.head(1000).to_excel(writer, sheet_name='Ulterra_Sample', index=False)
    
    print(f"\nDetailed report saved to: {output_path}")

if __name__ == "__main__":
    compare_reed_vs_ulterra_corrected()
