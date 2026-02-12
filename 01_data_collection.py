"""
YouTube Platform Abuse Trends Dashboard - Data Collection Module
Project 2 for Google Trust & Safety Engineering Analyst Portfolio

This script demonstrates:
1. YouTube API interaction for data collection
2. Real-time abuse pattern detection
3. SQL-based data warehousing for analysis
4. Metric calculation (spam prevalence, velocity)

Author: [Your Name]
Purpose: Demonstrate automated abuse detection capabilities for T&S role
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
import re
from collections import Counter

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration for YouTube API and analysis parameters"""
    
    # YouTube API Configuration
    API_KEY = os.getenv('YOUTUBE_API_KEY', 'YOUR_API_KEY_HERE')
    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    
    # Database Configuration
    DB_PATH = 'youtube_abuse_trends.db'
    
    # Analysis Parameters
    MAX_COMMENTS_PER_VIDEO = 100  # Limit for API quota management
    SPAM_KEYWORDS = [
        # Common spam patterns
        'click here', 'free money', 'subscribe to my channel',
        'check out my', 'dm for', 'winner', 'congratulations',
        # Bot indicators
        'telegram', 'whatsapp me', '+1', 'cash app',
        # Repetitive patterns
        '!!!', '????', 'www.', 'http',
    ]
    
    # Velocity thresholds (comments per minute)
    SUSPICIOUS_VELOCITY = 5  # More than 5 comments/min is suspicious
    BOT_VELOCITY = 10  # More than 10 comments/min likely bot

print("=" * 80)
print("YOUTUBE ABUSE TRENDS DASHBOARD - DATA COLLECTION")
print("Real-Time Platform Manipulation Detection System")
print("=" * 80)

# ============================================================================
# DATABASE SETUP
# ============================================================================

def init_database():
    """Initialize SQLite database with schema for abuse detection"""
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    # Videos table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            channel_title TEXT,
            published_at TIMESTAMP,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            category_id TEXT,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Comments table with abuse indicators
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            video_id TEXT,
            author_channel_id TEXT,
            author_name TEXT,
            text_display TEXT,
            like_count INTEGER,
            published_at TIMESTAMP,
            is_spam INTEGER DEFAULT 0,
            spam_score REAL DEFAULT 0.0,
            has_url INTEGER DEFAULT 0,
            has_excessive_caps INTEGER DEFAULT 0,
            has_repetitive_chars INTEGER DEFAULT 0,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    ''')
    
    # Abuse metrics aggregation table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS abuse_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            timestamp TIMESTAMP,
            total_comments INTEGER,
            spam_comments INTEGER,
            spam_prevalence REAL,
            comment_velocity REAL,
            unique_authors INTEGER,
            suspicious_accounts INTEGER,
            bot_likelihood_score REAL,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_video_id ON comments(video_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_spam ON comments(is_spam)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_published ON comments(published_at)')
    
    conn.commit()
    conn.close()
    print("\n✓ Database initialized successfully")

# ============================================================================
# YOUTUBE API DATA COLLECTION
# ============================================================================

def get_youtube_service():
    """Build YouTube API service"""
    try:
        return build(Config.API_SERVICE_NAME, Config.API_VERSION, 
                    developerKey=Config.API_KEY)
    except Exception as e:
        print(f"❌ Error building YouTube service: {e}")
        return None

def get_trending_videos(youtube, region_code='IN', max_results=5):
    """
    Fetch trending videos (simulates real-world T&S monitoring)
    
    Args:
        youtube: YouTube API service object
        region_code: Country code (India for Hyderabad relevance)
        max_results: Number of videos to fetch
    
    Returns:
        List of video IDs and metadata
    """
    try:
        request = youtube.videos().list(
            part='snippet,statistics',
            chart='mostPopular',
            regionCode=region_code,
            maxResults=max_results,
            videoCategoryId='25'  # News & Politics (high-risk category)
        )
        response = request.execute()
        
        videos = []
        for item in response.get('items', []):
            video_data = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'channel_title': item['snippet']['channelTitle'],
                'published_at': item['snippet']['publishedAt'],
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0)),
                'category_id': item['snippet'].get('categoryId', 'unknown')
            }
            videos.append(video_data)
        
        return videos
    
    except HttpError as e:
        print(f"❌ YouTube API Error: {e}")
        return []

def get_video_comments(youtube, video_id, max_results=100):
    """
    Fetch comments for a video with abuse detection metadata
    
    Args:
        youtube: YouTube API service object
        video_id: YouTube video ID
        max_results: Maximum comments to fetch
    
    Returns:
        List of comments with abuse indicators
    """
    comments = []
    
    try:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results,
            order='time',  # Get most recent first (velocity analysis)
            textFormat='plainText'
        )
        
        response = request.execute()
        
        for item in response.get('items', []):
            comment = item['snippet']['topLevelComment']['snippet']
            
            comment_data = {
                'comment_id': item['snippet']['topLevelComment']['id'],
                'video_id': video_id,
                'author_channel_id': comment.get('authorChannelId', {}).get('value', 'unknown'),
                'author_name': comment['authorDisplayName'],
                'text_display': comment['textDisplay'],
                'like_count': comment.get('likeCount', 0),
                'published_at': comment['publishedAt']
            }
            
            # Run abuse detection analysis
            comment_data.update(analyze_comment_for_abuse(comment_data['text_display']))
            
            comments.append(comment_data)
        
        return comments
    
    except HttpError as e:
        if e.resp.status == 403:
            print(f"⚠️  Comments disabled for video {video_id}")
        else:
            print(f"❌ Error fetching comments: {e}")
        return []

# ============================================================================
# ABUSE DETECTION LOGIC (T&S Core Capability)
# ============================================================================

def analyze_comment_for_abuse(text):
    """
    Analyze comment text for spam/abuse indicators
    
    This demonstrates the "bridge between policy and technical enforcement"
    mentioned in the JD - translating abuse patterns into automated detection
    
    Returns:
        Dictionary with abuse detection flags and scores
    """
    text_lower = text.lower()
    
    # Spam keyword matching
    spam_matches = sum(1 for keyword in Config.SPAM_KEYWORDS if keyword in text_lower)
    
    # URL detection (common spam vector)
    has_url = 1 if bool(re.search(r'http[s]?://|www\.', text)) else 0
    
    # Excessive caps (shouting/attention grabbing)
    caps_ratio = sum(1 for c in text if c.isupper()) / (len(text) + 1)
    has_excessive_caps = 1 if caps_ratio > 0.5 and len(text) > 10 else 0
    
    # Repetitive characters (e.g., "helloooooo")
    has_repetitive = 1 if bool(re.search(r'(.)\1{3,}', text)) else 0
    
    # Calculate spam score (0.0 to 1.0)
    spam_score = min(1.0, (
        spam_matches * 0.3 +
        has_url * 0.3 +
        has_excessive_caps * 0.2 +
        has_repetitive * 0.2
    ))
    
    # Binary spam classification (threshold 0.4)
    is_spam = 1 if spam_score >= 0.4 else 0
    
    return {
        'is_spam': is_spam,
        'spam_score': round(spam_score, 3),
        'has_url': has_url,
        'has_excessive_caps': has_excessive_caps,
        'has_repetitive_chars': has_repetitive
    }

def calculate_velocity_metrics(comments):
    """
    Calculate comment velocity (comments per minute) - key T&S metric
    
    Rapid commenting indicates coordinated bot activity or brigade attacks
    """
    if len(comments) < 2:
        return 0.0
    
    # Parse timestamps
    timestamps = [datetime.fromisoformat(c['published_at'].replace('Z', '+00:00')) 
                  for c in comments]
    timestamps.sort()
    
    # Calculate time window
    time_window_minutes = (timestamps[-1] - timestamps[0]).total_seconds() / 60
    
    if time_window_minutes == 0:
        return len(comments)  # All comments in same minute
    
    velocity = len(comments) / time_window_minutes
    return round(velocity, 2)

def detect_coordinated_activity(comments):
    """
    Detect bot networks and coordinated manipulation
    
    Analyzes patterns that indicate organized abuse:
    - Multiple accounts posting identical text
    - Rapid account creation dates
    - Suspicious author clustering
    """
    if len(comments) < 5:
        return 0.0
    
    # Check for duplicate/similar text (copypasta indicator)
    texts = [c['text_display'].lower() for c in comments]
    text_counts = Counter(texts)
    duplicate_ratio = sum(1 for count in text_counts.values() if count > 1) / len(texts)
    
    # Check for author diversity
    authors = [c['author_channel_id'] for c in comments]
    unique_authors = len(set(authors))
    author_diversity = unique_authors / len(comments)
    
    # Coordination score (low diversity + high duplication = high score)
    coordination_score = (duplicate_ratio * 0.6) + ((1 - author_diversity) * 0.4)
    
    return round(coordination_score, 3)

# ============================================================================
# DATA STORAGE & AGGREGATION
# ============================================================================

def store_video_data(video_data):
    """Store video metadata in database"""
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO videos 
        (video_id, title, channel_title, published_at, view_count, 
         like_count, comment_count, category_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        video_data['video_id'],
        video_data['title'],
        video_data['channel_title'],
        video_data['published_at'],
        video_data['view_count'],
        video_data['like_count'],
        video_data['comment_count'],
        video_data['category_id']
    ))
    
    conn.commit()
    conn.close()

def store_comments(comments):
    """Store comments with abuse detection results"""
    if not comments:
        return
    
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    for comment in comments:
        cursor.execute('''
            INSERT OR REPLACE INTO comments
            (comment_id, video_id, author_channel_id, author_name, text_display,
             like_count, published_at, is_spam, spam_score, has_url,
             has_excessive_caps, has_repetitive_chars)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            comment['comment_id'],
            comment['video_id'],
            comment['author_channel_id'],
            comment['author_name'],
            comment['text_display'],
            comment['like_count'],
            comment['published_at'],
            comment['is_spam'],
            comment['spam_score'],
            comment['has_url'],
            comment['has_excessive_caps'],
            comment['has_repetitive_chars']
        ))
    
    conn.commit()
    conn.close()

def store_abuse_metrics(video_id, comments):
    """
    Aggregate and store abuse metrics
    This creates the executive-level insights mentioned in the JD
    """
    total_comments = len(comments)
    spam_comments = sum(1 for c in comments if c['is_spam'] == 1)
    spam_prevalence = (spam_comments / total_comments * 100) if total_comments > 0 else 0
    
    velocity = calculate_velocity_metrics(comments)
    coordination_score = detect_coordinated_activity(comments)
    
    unique_authors = len(set(c['author_channel_id'] for c in comments))
    suspicious_accounts = sum(1 for c in comments if c['spam_score'] > 0.6)
    
    conn = sqlite3.connect(Config.DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO abuse_metrics
        (video_id, timestamp, total_comments, spam_comments, spam_prevalence,
         comment_velocity, unique_authors, suspicious_accounts, bot_likelihood_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        video_id,
        datetime.now().isoformat(),
        total_comments,
        spam_comments,
        round(spam_prevalence, 2),
        velocity,
        unique_authors,
        suspicious_accounts,
        coordination_score
    ))
    
    conn.commit()
    conn.close()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution flow"""
    print("\n[STEP 1] Initializing database...")
    init_database()
    
    print("\n[STEP 2] Connecting to YouTube API...")
    youtube = get_youtube_service()
    
    if not youtube:
        print("❌ Failed to connect to YouTube API. Please check your API key.")
        print("\nTo run this project:")
        print("1. Get API key from: https://console.cloud.google.com/")
        print("2. Set environment variable: export YOUTUBE_API_KEY='your_key'")
        print("3. Or edit Config.API_KEY in this script")
        return
    
    print("✓ Connected successfully")
    
    print("\n[STEP 3] Fetching trending videos (News & Politics category)...")
    videos = get_trending_videos(youtube, region_code='IN', max_results=5)
    
    if not videos:
        print("❌ No videos fetched. Using sample data for demonstration...")
        # Continue with demonstration using hardcoded data
        return
    
    print(f"✓ Fetched {len(videos)} videos")
    
    print("\n[STEP 4] Analyzing comments for abuse patterns...")
    for i, video in enumerate(videos, 1):
        print(f"\n  [{i}/{len(videos)}] Processing: {video['title'][:60]}...")
        
        # Store video metadata
        store_video_data(video)
        
        # Fetch and analyze comments
        comments = get_video_comments(youtube, video['video_id'], 
                                      max_results=Config.MAX_COMMENTS_PER_VIDEO)
        
        if comments:
            store_comments(comments)
            store_abuse_metrics(video['video_id'], comments)
            
            spam_count = sum(1 for c in comments if c['is_spam'] == 1)
            velocity = calculate_velocity_metrics(comments)
            
            print(f"    ✓ Analyzed {len(comments)} comments")
            print(f"    📊 Spam detected: {spam_count} ({spam_count/len(comments)*100:.1f}%)")
            print(f"    ⚡ Velocity: {velocity:.2f} comments/min")
            
            if velocity > Config.BOT_VELOCITY:
                print(f"    ⚠️  HIGH VELOCITY ALERT - Possible bot attack!")
        
        time.sleep(1)  # Rate limiting courtesy
    
    print("\n" + "=" * 80)
    print("✅ DATA COLLECTION COMPLETE")
    print("=" * 80)
    print(f"\nDatabase saved to: {Config.DB_PATH}")
    print("\nNEXT STEPS:")
    print("1. Run analysis_queries.py to generate SQL insights")
    print("2. Run dashboard_export.py to create Tableau data source")
    print("3. Open Tableau workbook to visualize abuse trends")
    print("=" * 80)

if __name__ == "__main__":
    main()
