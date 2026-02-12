"""
YouTube Abuse Trends - SQL Analysis Queries
Demonstrates advanced SQL for Trust & Safety operational insights

These queries showcase the "translating technical data into executive insights"
capability mentioned in the Engineering Analyst job description.
"""

import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = 'youtube_abuse_trends.db'

print("=" * 80)
print("YOUTUBE ABUSE TRENDS - SQL ANALYSIS")
print("Executive-Level Insights from Platform Data")
print("=" * 80)

def execute_query(query, description):
    """Execute SQL query and display results"""
    print(f"\n{'='*80}")
    print(f"QUERY: {description}")
    print(f"{'='*80}\n")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        print(df.to_string(index=False))
        print(f"\n✓ Returned {len(df)} rows")
    else:
        print("⚠️  No data returned")
    
    return df

# ============================================================================
# QUERY 1: ABUSE PREVALENCE BY VIDEO (EXECUTIVE SUMMARY)
# ============================================================================

query1 = """
-- High-level abuse metrics per video for executive dashboard
SELECT 
    v.title,
    v.channel_title,
    v.view_count,
    v.comment_count,
    COALESCE(am.spam_prevalence, 0) as spam_prevalence_pct,
    COALESCE(am.comment_velocity, 0) as comments_per_minute,
    COALESCE(am.bot_likelihood_score, 0) as coordination_score,
    CASE 
        WHEN am.spam_prevalence > 30 THEN 'HIGH RISK'
        WHEN am.spam_prevalence > 15 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END as risk_level
FROM videos v
LEFT JOIN abuse_metrics am ON v.video_id = am.video_id
ORDER BY spam_prevalence_pct DESC;
"""

df1 = execute_query(query1, "Abuse Prevalence by Video (Executive Summary)")

# ============================================================================
# QUERY 2: SPAM VELOCITY ANALYSIS (THREAT DETECTION)
# ============================================================================

query2 = """
-- Identify potential bot attacks based on comment velocity
SELECT 
    v.title,
    am.comment_velocity,
    am.total_comments,
    am.unique_authors,
    ROUND(CAST(am.total_comments AS FLOAT) / am.unique_authors, 2) as comments_per_author,
    CASE 
        WHEN am.comment_velocity > 10 THEN 'CRITICAL - Likely Bot Attack'
        WHEN am.comment_velocity > 5 THEN 'WARNING - Suspicious Activity'
        ELSE 'NORMAL'
    END as threat_assessment
FROM abuse_metrics am
JOIN videos v ON am.video_id = v.video_id
WHERE am.comment_velocity > 0
ORDER BY am.comment_velocity DESC;
"""

df2 = execute_query(query2, "Velocity-Based Threat Detection")

# ============================================================================
# QUERY 3: COORDINATED MANIPULATION DETECTION
# ============================================================================

query3 = """
-- Detect coordinated abuse campaigns (multiple accounts, similar content)
SELECT 
    v.title,
    am.bot_likelihood_score as coordination_score,
    am.unique_authors,
    am.suspicious_accounts,
    ROUND(CAST(am.suspicious_accounts AS FLOAT) / am.total_comments * 100, 1) as suspicious_account_pct,
    CASE 
        WHEN am.bot_likelihood_score > 0.7 AND am.suspicious_accounts > 10 THEN 'COORDINATED ATTACK'
        WHEN am.bot_likelihood_score > 0.5 THEN 'POSSIBLE COORDINATION'
        ELSE 'ORGANIC ACTIVITY'
    END as activity_type
FROM abuse_metrics am
JOIN videos v ON am.video_id = v.video_id
ORDER BY coordination_score DESC;
"""

df3 = execute_query(query3, "Coordinated Manipulation Detection")

# ============================================================================
# QUERY 4: SPAM PATTERNS ANALYSIS (GRANULAR)
# ============================================================================

query4 = """
-- Analyze specific spam indicators across all comments
SELECT 
    COUNT(*) as total_comments,
    SUM(is_spam) as spam_comments,
    ROUND(CAST(SUM(is_spam) AS FLOAT) / COUNT(*) * 100, 2) as overall_spam_rate,
    SUM(has_url) as comments_with_urls,
    SUM(has_excessive_caps) as excessive_caps_comments,
    SUM(has_repetitive_chars) as repetitive_char_comments,
    ROUND(AVG(spam_score), 3) as avg_spam_score
FROM comments;
"""

df4 = execute_query(query4, "Overall Spam Patterns Analysis")

# ============================================================================
# QUERY 5: AUTHOR BEHAVIOR ANALYSIS (TOP SPAMMERS)
# ============================================================================

query5 = """
-- Identify top spam accounts (repeated policy violators)
SELECT 
    author_name,
    author_channel_id,
    COUNT(*) as total_comments,
    SUM(is_spam) as spam_comments,
    ROUND(CAST(SUM(is_spam) AS FLOAT) / COUNT(*) * 100, 1) as spam_rate,
    ROUND(AVG(spam_score), 3) as avg_spam_score,
    COUNT(DISTINCT video_id) as videos_commented_on
FROM comments
GROUP BY author_channel_id, author_name
HAVING spam_comments > 0
ORDER BY spam_rate DESC, total_comments DESC
LIMIT 20;
"""

df5 = execute_query(query5, "Top Spam Accounts (Repeated Violators)")

# ============================================================================
# QUERY 6: TIME-SERIES TREND ANALYSIS
# ============================================================================

query6 = """
-- Track spam trends over time (for dashboard time-series charts)
SELECT 
    DATE(published_at) as date,
    COUNT(*) as total_comments,
    SUM(is_spam) as spam_comments,
    ROUND(CAST(SUM(is_spam) AS FLOAT) / COUNT(*) * 100, 2) as spam_rate,
    COUNT(DISTINCT author_channel_id) as unique_commenters
FROM comments
GROUP BY DATE(published_at)
ORDER BY date;
"""

df6 = execute_query(query6, "Time-Series Spam Trends")

# ============================================================================
# QUERY 7: ABUSE HEATMAP DATA (FOR TABLEAU VISUALIZATION)
# ============================================================================

query7 = """
-- Prepare data for heatmap: video vs abuse indicators
SELECT 
    v.title as video_title,
    v.channel_title,
    c.is_spam,
    c.has_url,
    c.has_excessive_caps,
    c.has_repetitive_chars,
    c.spam_score,
    DATE(c.published_at) as comment_date
FROM comments c
JOIN videos v ON c.video_id = v.video_id
ORDER BY c.published_at DESC;
"""

df7 = execute_query(query7, "Abuse Heatmap Data (Tableau Export)")

# ============================================================================
# GENERATE EXECUTIVE SUMMARY METRICS
# ============================================================================

def generate_executive_summary():
    """Create high-level KPIs for leadership dashboard"""
    print("\n" + "=" * 80)
    print("EXECUTIVE SUMMARY - KEY PERFORMANCE INDICATORS")
    print("=" * 80 + "\n")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Total platform metrics
    total_videos = pd.read_sql_query("SELECT COUNT(*) as count FROM videos", conn).iloc[0]['count']
    total_comments = pd.read_sql_query("SELECT COUNT(*) as count FROM comments", conn).iloc[0]['count']
    
    # Abuse metrics
    spam_metrics = pd.read_sql_query("""
        SELECT 
            SUM(is_spam) as spam_count,
            AVG(spam_score) as avg_spam_score,
            SUM(has_url) as url_count
        FROM comments
    """, conn).iloc[0]
    
    # Velocity metrics
    velocity_metrics = pd.read_sql_query("""
        SELECT 
            AVG(comment_velocity) as avg_velocity,
            MAX(comment_velocity) as max_velocity,
            AVG(bot_likelihood_score) as avg_coordination
        FROM abuse_metrics
    """, conn).iloc[0]
    
    conn.close()
    
    # Calculate KPIs
    spam_rate = (spam_metrics['spam_count'] / total_comments * 100) if total_comments > 0 else 0
    
    print(f"📊 PLATFORM OVERVIEW:")
    print(f"   Total Videos Monitored:        {total_videos}")
    print(f"   Total Comments Analyzed:       {total_comments:,}")
    print(f"\n🚨 ABUSE DETECTION:")
    print(f"   Spam Comments Detected:        {int(spam_metrics['spam_count'])} ({spam_rate:.2f}%)")
    print(f"   Average Spam Score:            {spam_metrics['avg_spam_score']:.3f}")
    print(f"   Comments with URLs:            {int(spam_metrics['url_count'])}")
    print(f"\n⚡ VELOCITY ANALYSIS:")
    print(f"   Average Comment Velocity:      {velocity_metrics['avg_velocity']:.2f} comments/min")
    print(f"   Peak Velocity Detected:        {velocity_metrics['max_velocity']:.2f} comments/min")
    print(f"   Average Coordination Score:    {velocity_metrics['avg_coordination']:.3f}")
    
    if velocity_metrics['max_velocity'] > 10:
        print(f"\n⚠️  ALERT: High velocity detected - Possible bot attack!")
    
    if spam_rate > 20:
        print(f"\n⚠️  ALERT: Spam rate exceeds 20% threshold - Enhanced monitoring recommended")
    
    print("\n" + "=" * 80)

generate_executive_summary()

# ============================================================================
# EXPORT DATA FOR TABLEAU DASHBOARD
# ============================================================================

def export_for_tableau():
    """Export cleaned datasets for Tableau visualization"""
    print("\n" + "=" * 80)
    print("EXPORTING DATA FOR TABLEAU DASHBOARD")
    print("=" * 80 + "\n")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Export 1: Video-level metrics
    video_metrics = pd.read_sql_query("""
        SELECT 
            v.video_id,
            v.title,
            v.channel_title,
            v.view_count,
            v.comment_count,
            am.spam_prevalence,
            am.comment_velocity,
            am.bot_likelihood_score,
            am.unique_authors,
            am.suspicious_accounts
        FROM videos v
        LEFT JOIN abuse_metrics am ON v.video_id = am.video_id
    """, conn)
    video_metrics.to_csv('tableau_video_metrics.csv', index=False)
    print("✓ Exported: tableau_video_metrics.csv")
    
    # Export 2: Comment-level details
    comment_details = pd.read_sql_query("""
        SELECT 
            c.comment_id,
            c.video_id,
            v.title as video_title,
            c.author_name,
            c.text_display,
            c.is_spam,
            c.spam_score,
            c.has_url,
            c.has_excessive_caps,
            c.published_at
        FROM comments c
        JOIN videos v ON c.video_id = v.video_id
    """, conn)
    comment_details.to_csv('tableau_comment_details.csv', index=False)
    print("✓ Exported: tableau_comment_details.csv")
    
    # Export 3: Time-series data
    timeseries = pd.read_sql_query("""
        SELECT 
            datetime(published_at) as timestamp,
            video_id,
            is_spam,
            spam_score
        FROM comments
        ORDER BY published_at
    """, conn)
    timeseries.to_csv('tableau_timeseries.csv', index=False)
    print("✓ Exported: tableau_timeseries.csv")
    
    conn.close()
    
    print("\n✅ All datasets exported for Tableau visualization")
    print("\nTABLEAU DASHBOARD SETUP:")
    print("1. Open Tableau Desktop")
    print("2. Connect to Data > Text File")
    print("3. Load: tableau_video_metrics.csv (main data source)")
    print("4. Add: tableau_comment_details.csv (detail drill-down)")
    print("5. Add: tableau_timeseries.csv (trend analysis)")
    print("\nRECOMMENDED VISUALIZATIONS:")
    print("- Heatmap: Spam prevalence by video")
    print("- Line chart: Spam rate over time")
    print("- Bar chart: Top videos by abuse volume")
    print("- Scatter plot: Velocity vs. Coordination score")
    print("=" * 80)

export_for_tableau()

print("\n✅ SQL ANALYSIS COMPLETE")
print(f"Database: {DB_PATH}")
print("Ready for Tableau dashboard creation!")
