"""
Data Analysis and Visualization
Performs calculations on space data and creates visualizations
Authors: Isaiah Ramirez & Sajjad Majeed
"""

import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

try:
    import pandas as pd
    import seaborn as sns
    HAS_SEABORN = True
    sns.set_style("whitegrid")
except ImportError:
    import warnings
    warnings.warn("Seaborn not found. Install with: pip install seaborn pandas")
    HAS_SEABORN = False

# Set style for better-looking plots
plt.rcParams['figure.figsize'] = (12, 6)
plt.style.use('ggplot')

def get_data_from_db():
    """
    Connects to database and retrieves data.
    Returns connection object.
    """
    conn = sqlite3.connect('space_data.db')
    return conn

def check_data_distribution():
    """
    Checks how your data is distributed to help with meaningful analysis.
    """
    conn = get_data_from_db()
    cur = conn.cursor()
    
    print("\n" + "=" * 60)
    print("DATA DISTRIBUTION ANALYSIS")
    print("=" * 60)
    
    # Check date range for approaches
    cur.execute('''
        SELECT 
            MIN(approach_date) as earliest,
            MAX(approach_date) as latest,
            COUNT(DISTINCT strftime('%Y-%m', approach_date)) as unique_months,
            COUNT(DISTINCT strftime('%Y-%m-%d', approach_date)) as unique_days
        FROM approaches
    ''')
    
    result = cur.fetchone()
    print(f"\nNEO Approaches Date Range:")
    print(f"  Earliest: {result[0]}")
    print(f"  Latest: {result[1]}")
    print(f"  Unique Months: {result[2]}")
    print(f"  Unique Days: {result[3]}")
    
    # Check date range for APOD
    cur.execute('''
        SELECT 
            MIN(date) as earliest,
            MAX(date) as latest,
            COUNT(DISTINCT strftime('%Y-%m', date)) as unique_months
        FROM apod_images
    ''')
    
    result = cur.fetchone()
    print(f"\nAPOD Date Range:")
    print(f"  Earliest: {result[0]}")
    print(f"  Latest: {result[1]}")
    print(f"  Unique Months: {result[2]}")
    
    conn.close()
    
    return result

def calculate_approaches_by_day():
    """
    CALCULATION 1: Approaches by DAY (more granular than month)
    Shows daily patterns in asteroid approaches.
    Uses JOIN to get asteroid details with approach data.
    """
    conn = get_data_from_db()
    
    query = '''
        SELECT 
            approaches.approach_date as date,
            COUNT(*) as approach_count,
            AVG(approaches.miss_distance_km) as avg_distance,
            AVG(approaches.rel_vel_km_h) as avg_velocity,
            AVG(asteroids.estimated_diameter_max) as avg_size,
            SUM(asteroids.is_potentially_hazardous) as hazardous_count
        FROM approaches
        JOIN asteroids ON approaches.asteroid_id = asteroids.id
        GROUP BY approaches.approach_date
        ORDER BY approaches.approach_date
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 60)
    print("CALCULATION 1: Asteroid Approaches By Day")
    print("=" * 60)
    print(df.to_string(index=False))
    print(f"\nTotal days with approaches: {len(df)}")
    print(f"Average approaches per day: {df['approach_count'].mean():.2f}")
    print(f"Max approaches in a day: {df['approach_count'].max()}")
    print(f"Days with hazardous asteroids: {(df['hazardous_count'] > 0).sum()}")
    
    return df

def calculate_velocity_vs_distance():
    """
    CALCULATION 2: Velocity vs miss distance for all asteroids
    Analyzes relationship between speed and proximity.
    Uses JOIN to combine asteroid and approach data.
    """
    conn = get_data_from_db()
    
    query = '''
        SELECT 
            approaches.rel_vel_km_h AS velocity_kmh,
            approaches.miss_distance_km,
            asteroids.name,
            asteroids.estimated_diameter_max,
            asteroids.is_potentially_hazardous,
            approaches.approach_date
        FROM approaches
        JOIN asteroids ON approaches.asteroid_id = asteroids.id
        ORDER BY approaches.miss_distance_km
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 60)
    print("CALCULATION 2: Velocity vs Miss Distance Analysis")
    print("=" * 60)
    print(f"Total asteroid approaches: {len(df)}")
    print(f"\nClosest 10 approaches:")
    print(df.head(10).to_string(index=False))
    print(f"\nAverage velocity: {df['velocity_kmh'].mean():.2f} km/h")
    print(f"Average miss distance: {df['miss_distance_km'].mean():.2f} km")
    
    # Calculate correlation
    correlation = df['velocity_kmh'].corr(df['miss_distance_km'])
    print(f"Correlation coefficient: {correlation:.4f}")
    
    # Statistics by hazard status
    print(f"\nHazardous asteroids: {df['is_potentially_hazardous'].sum()}")
    print(f"Non-hazardous asteroids: {(df['is_potentially_hazardous'] == 0).sum()}")
    
    return df

def calculate_asteroid_size_distribution():
    """
    CALCULATION 3: Asteroid size distribution and hazard analysis
    Groups asteroids by size categories and analyzes hazard patterns.
    """
    conn = get_data_from_db()
    
    query = '''
        SELECT 
            CASE 
                WHEN estimated_diameter_max < 0.1 THEN 'Tiny (< 0.1 km)'
                WHEN estimated_diameter_max < 0.5 THEN 'Small (0.1-0.5 km)'
                WHEN estimated_diameter_max < 1.0 THEN 'Medium (0.5-1.0 km)'
                ELSE 'Large (> 1.0 km)'
            END as size_category,
            COUNT(*) as count,
            AVG(estimated_diameter_max) as avg_diameter,
            SUM(is_potentially_hazardous) as hazardous_count,
            AVG(absolute_magnitude) as avg_magnitude
        FROM asteroids
        GROUP BY size_category
        ORDER BY avg_diameter
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 60)
    print("CALCULATION 3: Asteroid Size Distribution & Hazard Analysis")
    print("=" * 60)
    print(df.to_string(index=False))
    
    return df

def calculate_apod_keywords_by_day():
    """
    CALCULATION 4: APOD keyword analysis by day
    Tracks space-related terms in APOD posts.
    """
    conn = get_data_from_db()
    
    query = '''
        SELECT 
            date,
            title,
            CASE WHEN LOWER(explanation) LIKE '%asteroid%' THEN 1 ELSE 0 END as has_asteroid,
            CASE WHEN LOWER(explanation) LIKE '%meteor%' THEN 1 ELSE 0 END as has_meteor,
            CASE WHEN LOWER(explanation) LIKE '%comet%' THEN 1 ELSE 0 END as has_comet,
            CASE WHEN LOWER(title) LIKE '%asteroid%' 
                   OR LOWER(title) LIKE '%meteor%' 
                   OR LOWER(title) LIKE '%comet%' THEN 1 ELSE 0 END as space_object_title
        FROM apod_images
        ORDER BY date
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 60)
    print("CALCULATION 4: APOD Space Object Keywords")
    print("=" * 60)
    
    total_mentions = df['has_asteroid'].sum() + df['has_meteor'].sum() + df['has_comet'].sum()
    print(f"Total APOD entries: {len(df)}")
    print(f"Entries mentioning 'asteroid': {df['has_asteroid'].sum()}")
    print(f"Entries mentioning 'meteor': {df['has_meteor'].sum()}")
    print(f"Entries mentioning 'comet': {df['has_comet'].sum()}")
    print(f"Titles about space objects: {df['space_object_title'].sum()}")
    print(f"\nTotal space object mentions: {total_mentions}")
    
    return df

def create_visualization_1(data):
    """
    VISUALIZATION 1: Asteroid approaches over time (by day)
    Shows daily patterns with trend line
    """
    plt.figure(figsize=(14, 7))
    
    # Convert date strings to datetime
    data['date_dt'] = pd.to_datetime(data['date'])
    
    # Main line plot
    plt.plot(data['date_dt'], data['approach_count'], 
             marker='o', linewidth=2, markersize=6, 
             color='#FF6B6B', label='Daily Approaches', alpha=0.8)
    
    # Add moving average if we have enough data
    if len(data) > 5:
        data['moving_avg'] = data['approach_count'].rolling(window=min(7, len(data)//2), center=True).mean()
        plt.plot(data['date_dt'], data['moving_avg'], 
                linewidth=3, color='#2E86AB', 
                label=f'{min(7, len(data)//2)}-day Moving Average', alpha=0.7)
    
    plt.title('Near Earth Object Approaches Over Time', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Number of Approaches', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    plt.savefig('viz1_approaches_over_time.png', dpi=300, bbox_inches='tight')
    print("\n✓ Saved: viz1_approaches_over_time.png")
    plt.close()

def create_visualization_2(data):
    """
    VISUALIZATION 2: Scatter plot of velocity vs miss distance
    Color by hazard status, size by diameter
    """
    plt.figure(figsize=(12, 8))
    
    # Separate hazardous and non-hazardous
    hazardous = data[data['is_potentially_hazardous'] == 1]
    non_hazardous = data[data['is_potentially_hazardous'] == 0]
    
    # Plot non-hazardous (background)
    if len(non_hazardous) > 0:
        sizes_nh = [s * 100 if s > 0 else 20 for s in non_hazardous['estimated_diameter_max']]
        plt.scatter(non_hazardous['velocity_kmh'], non_hazardous['miss_distance_km'], 
                    c='#4A90E2', s=sizes_nh, alpha=0.5, 
                    edgecolors='black', linewidth=0.5, label='Non-Hazardous')
    
    # Plot hazardous (foreground)
    if len(hazardous) > 0:
        sizes_h = [s * 100 if s > 0 else 20 for s in hazardous['estimated_diameter_max']]
        plt.scatter(hazardous['velocity_kmh'], hazardous['miss_distance_km'], 
                    c='#FF4444', s=sizes_h, alpha=0.7, 
                    edgecolors='darkred', linewidth=1, label='Potentially Hazardous')
    
    plt.title('Asteroid Velocity vs Miss Distance', fontsize=16, fontweight='bold')
    plt.xlabel('Velocity (km/h)', fontsize=12)
    plt.ylabel('Miss Distance (km)', fontsize=12)
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    
    # Add correlation text
    correlation = data['velocity_kmh'].corr(data['miss_distance_km'])
    plt.text(0.02, 0.98, f'Correlation: {correlation:.3f}', 
             transform=plt.gca().transAxes, fontsize=11,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig('viz2_velocity_vs_distance.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: viz2_velocity_vs_distance.png")
    plt.close()

def create_visualization_3(apod_data):
    """
    VISUALIZATION 3: APOD Content Categories - Pie Chart
    Shows breakdown of space-related topics in NASA's daily astronomy posts
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    
    # LEFT PIE: Space Object Mentions in APOD Content
    asteroid_count = apod_data['has_asteroid'].sum()
    meteor_count = apod_data['has_meteor'].sum()
    comet_count = apod_data['has_comet'].sum()
    other_count = len(apod_data) - (asteroid_count + meteor_count + comet_count)
    
    # Handle case where counts might be zero
    if asteroid_count + meteor_count + comet_count == 0:
        other_count = len(apod_data)
    
    categories = []
    counts = []
    colors = []
    
    if asteroid_count > 0:
        categories.append(f'Asteroid\n({asteroid_count})')
        counts.append(asteroid_count)
        colors.append('#FF6B6B')
    
    if meteor_count > 0:
        categories.append(f'Meteor\n({meteor_count})')
        counts.append(meteor_count)
        colors.append('#4ECDC4')
    
    if comet_count > 0:
        categories.append(f'Comet\n({comet_count})')
        counts.append(comet_count)
        colors.append('#45B7D1')
    
    if other_count > 0:
        categories.append(f'Other Space\nTopics\n({other_count})')
        counts.append(other_count)
        colors.append('#95E1D3')
    
    # Create pie chart
    wedges, texts, autotexts = ax1.pie(counts, labels=categories, colors=colors,
                                         autopct='%1.1f%%', startangle=90,
                                         textprops={'fontsize': 11, 'weight': 'bold'},
                                         explode=[0.05] * len(counts))
    
    # Make percentage text more visible
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(12)
        autotext.set_weight('bold')
    
    ax1.set_title('APOD Space Object Mentions\n(% of Total Posts)', 
                  fontsize=14, fontweight='bold', pad=20)
    
    # RIGHT PIE: Media Type Distribution
    media_types = {}
    conn = get_data_from_db()
    cur = conn.cursor()
    cur.execute('SELECT media_type, COUNT(*) FROM apod_images GROUP BY media_type')
    for row in cur.fetchall():
        media_types[row[0]] = row[1]
    conn.close()
    
    media_labels = []
    media_counts = []
    media_colors_list = ['#FFB6C1', '#FFA07A', '#98D8C8', '#F7DC6F']
    
    for i, (media_type, count) in enumerate(media_types.items()):
        media_labels.append(f'{media_type.title()}\n({count})')
        media_counts.append(count)
    
    wedges2, texts2, autotexts2 = ax2.pie(media_counts, labels=media_labels,
                                            colors=media_colors_list[:len(media_counts)],
                                            autopct='%1.1f%%', startangle=90,
                                            textprops={'fontsize': 11, 'weight': 'bold'},
                                            explode=[0.05] * len(media_counts))
    
    for autotext in autotexts2:
        autotext.set_color('white')
        autotext.set_fontsize(12)
        autotext.set_weight('bold')
    
    ax2.set_title('APOD Media Type Distribution\n(Images vs Videos)', 
                  fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig('viz3_apod_content_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: viz3_apod_content_analysis.png")
    plt.close()

def create_visualization_4(size_data):
    """
    VISUALIZATION 4: Asteroid Size Categories vs Hazard Rate
    
    Combines:
      - Bar chart: number of asteroids in each size category
      - Line plot: percentage of asteroids that are potentially hazardous
    
    This helps answer:
      "Are larger near-Earth asteroids more likely to be classified as hazardous?"
    """
    # Safety copy so we don't mutate the original df
    data = size_data.copy()

    # Compute hazard rate (% hazardous in each size category)
    # Avoid division by zero just in case
    data['hazard_rate'] = data.apply(
        lambda row: (row['hazardous_count'] / row['count'] * 100) if row['count'] > 0 else 0,
        axis=1
    )

    # Sort categories by average diameter so the x-axis flows logically
    data = data.sort_values('avg_diameter')

    fig, ax1 = plt.subplots(figsize=(12, 7))

    # ---- Bars: total asteroids per size category ----
    bar_positions = range(len(data))
    bars = ax1.bar(
        bar_positions,
        data['count'],
        color='#4A90E2',
        alpha=0.8,
        label='Number of Asteroids'
    )

    ax1.set_xlabel('Asteroid Size Category', fontsize=12)
    ax1.set_ylabel('Number of Asteroids', fontsize=12, color='#4A90E2')
    ax1.tick_params(axis='y', labelcolor='#4A90E2')
    ax1.set_xticks(bar_positions)
    ax1.set_xticklabels(data['size_category'], rotation=20, ha='right')

    # Annotate bars with counts
    for bar in bars:
        height = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f'{int(height)}',
            ha='center',
            va='bottom',
            fontsize=10
        )

    # ---- Line: % hazardous (secondary y-axis) ----
    ax2 = ax1.twinx()
    ax2.plot(
        bar_positions,
        data['hazard_rate'],
        color='#FF6B6B',
        marker='o',
        linewidth=2.5,
        label='% Potentially Hazardous'
    )
    ax2.set_ylabel('% Potentially Hazardous', fontsize=12, color='#FF6B6B')
    ax2.tick_params(axis='y', labelcolor='#FF6B6B')
    ax2.set_ylim(0, max(data['hazard_rate'].max() * 1.2, 10))  # some headroom

    # ---- Title and legend ----
    plt.title(
        'Asteroid Size vs Hazard Classification\n'
        '(Counts and % Potentially Hazardous by Size Category)',
        fontsize=16,
        fontweight='bold'
    )

    # Combine legends from both axes
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.tight_layout()
    plt.savefig('viz4_size_vs_hazard.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: viz4_size_vs_hazard.png")
    plt.close()
    
def main():
    """
    Main function - runs all calculations and creates all visualizations
    """
    print("\n" + "=" * 60)
    print("OCCUPY SPACE - DATA ANALYSIS & VISUALIZATION")
    print("=" * 60)
    
    # Check database status
    conn = get_data_from_db()
    cur = conn.cursor()
    
    cur.execute('SELECT COUNT(*) FROM asteroids')
    asteroid_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM approaches')
    approach_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM apod_images')
    apod_count = cur.fetchone()[0]
    
    conn.close()
    
    print(f"\nDatabase Status:")
    print(f"  Asteroids: {asteroid_count}")
    print(f"  Approaches: {approach_count}")
    print(f"  APOD Images: {apod_count}")
    
    if approach_count < 100 or apod_count < 100:
        print("\n⚠ WARNING: Need at least 100 entries from each API!")
        print(f"  Run neoWs_data.py {max(0, (100-approach_count)//20 + 1)} more time(s)")
        print(f"  Run apod_data.py {max(0, (100-apod_count)//25 + 1)} more time(s)")
        print("\n  Continuing with available data for now...\n")
    
    # Check data distribution
    check_data_distribution()
    
    # Perform calculations
    neo_daily = calculate_approaches_by_day()
    velocity_distance = calculate_velocity_vs_distance()
    size_dist = calculate_asteroid_size_distribution()
    apod_keywords = calculate_apod_keywords_by_day()
    
    # Create visualizations
    print("\n" + "=" * 60)
    print("CREATING VISUALIZATIONS")
    print("=" * 60)
    
    create_visualization_1(neo_daily)
    create_visualization_2(velocity_distance)
    create_visualization_3(apod_keywords)
    create_visualization_4(size_dist)
    
    print("\n" + "=" * 60)
    print("✓ ANALYSIS COMPLETE!")
    print("=" * 60)
    print("\nGenerated files:")
    print("  - viz1_approaches_over_time.png (NEO daily approaches)")
    print("  - viz2_velocity_vs_distance.png (NEO velocity analysis)")
    print("  - viz3_apod_content_analysis.png (APOD content breakdown)")
    print("  - viz4_size_vs_hazard.png (Asteroid size vs hazard rate)")
    print("\nCheck these files for your project report!")
    print("\n💡 TIP: Run data collection scripts more times for richer visualizations!")

if __name__ == "__main__":
    main()