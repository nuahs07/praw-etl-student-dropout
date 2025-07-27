import praw
import pandas as pd
from textblob import TextBlob
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector
from sqlalchemy import create_engine
import re
from datetime import datetime
from dotenv import load_dotenv
import os
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

# Keywords and subreddits
keywords = [
    'Philippines dropout', 'DepEd', 'public school problems',
    'school dropout Philippines', 'education crisis',
    'modular learning', 'distance learning', 'student dropout',
    'school', 'university', 'college', 'education', 'students',
    'CHED', 'mental health students', 'education reform', 'out of school'
]
subreddits_to_search = ['Philippines', 'studentsph', 'AskPH', 'DepEdTeachersPH', 'Pinoy', 'CollegeAdmissionsPH', 'CollegePhilippines']
post_limit_per_query = 1000

# Extract
seen_ids = set()
posts = []

for subreddit in subreddits_to_search:
    for keyword in keywords:
        logger.info(f"Searching r/{subreddit} for '{keyword}'...")
        try:
            for submission in reddit.subreddit(subreddit).search(keyword, sort='new', limit=post_limit_per_query):
                if submission.id not in seen_ids:
                    seen_ids.add(submission.id)
                    posts.append({
                        'id': submission.id,
                        'content': submission.title + ' ' + submission.selftext,
                        'date': datetime.fromtimestamp(submission.created_utc),
                        'url': submission.url,
                        'subreddit': submission.subreddit.display_name
                    })
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Error with '{keyword}' in r/{subreddit}: {e}")
            time.sleep(5)

logger.info(f"Total posts collected: {len(posts)}")

# Save backup CSV with timestamp
df = pd.DataFrame(posts)
if not df.empty:
    backup_name = f"reddit_posts_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(backup_name, index=False, encoding='utf-8')
    logger.info(f"Backup saved as {backup_name}")

# Transform
def clean_text(text):
    text = re.sub(r"http\S+|www\S+|[^a-zA-Z\s]", "", text)
    return text.lower().strip()

df['clean_content'] = df['content'].apply(clean_text)
df['sentiment'] = df['clean_content'].apply(lambda x: TextBlob(x).sentiment.polarity)
df['sentiment_label'] = df['sentiment'].apply(lambda x: 'positive' if x > 0.1 else 'negative' if x < -0.1 else 'neutral')
df['dropout_mentioned'] = df['clean_content'].str.contains(r'drop[\s-]?out|dropped out', case=False, na=False)
df['year'] = df['date'].apply(lambda x: x.year)

# Load to MySQL (ETL using mysql.connector)
try:
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB", "reddit_education")
    )
    cursor = conn.cursor(buffered=True)

    # DimSubreddit
    subreddit_map = {}
    for sr in df['subreddit'].unique():
        cursor.execute("INSERT IGNORE INTO DimSubreddit (name) VALUES (%s)", (sr,))
        conn.commit()
        cursor.execute("SELECT subreddit_id FROM DimSubreddit WHERE name = %s", (sr,))
        subreddit_map[sr] = cursor.fetchone()[0]

    # DimTime
    year_map = {}
    for y in df['year'].unique():
        y_int = int(y)
        cursor.execute("INSERT IGNORE INTO DimTime (year) VALUES (%s)", (y_int,))
        conn.commit()
        cursor.execute("SELECT time_id FROM DimTime WHERE year = %s", (y_int,))
        year_map[y] = cursor.fetchone()[0]

    # FactPost - Batch Insert
    fact_values = [
        (
            row['id'], row['clean_content'], row['url'], row['sentiment'],
            row['dropout_mentioned'], subreddit_map[row['subreddit']],
            year_map[row['year']]
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany("""
        INSERT IGNORE INTO FactPost (
            post_id, content, url, sentiment, dropout_mentioned,
            subreddit_id, time_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, fact_values)
    conn.commit()

    logger.info(f"ETL Complete. {cursor.rowcount} new posts inserted into database.")

except Exception as db_err:
    logger.error(f"Database error: {db_err}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()

# Visualization using SQLAlchemy
try:
    engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DB')}"
    )
    query = """
        SELECT f.sentiment, f.dropout_mentioned, t.year, s.name AS subreddit
        FROM FactPost f
        JOIN DimSubreddit s ON f.subreddit_id = s.subreddit_id
        JOIN DimTime t ON f.time_id = t.time_id
    """
    df_viz = pd.read_sql(query, engine)

    df_viz['sentiment_label'] = df_viz['sentiment'].apply(
        lambda x: 'Positive' if x > 0.1 else 'Negative' if x < -0.1 else 'Neutral'
    )
    df_viz['year'] = df_viz['year'].astype(int)

    # Sort DataFrame by year for consistency
    df_viz = df_viz.sort_values(by='year')
    year_order = sorted(df_viz['year'].unique()) 

    # 1. Sentiment Distribution
    plt.figure(figsize=(8, 6))
    sns.countplot(x='sentiment_label', hue='sentiment_label', data=df_viz, palette='coolwarm', legend=False)
    plt.title('Sentiment Distribution of Reddit Posts')
    plt.xlabel('Sentiment')
    plt.ylabel('Number of Posts')
    plt.tight_layout()
    plt.savefig("sentiment_distribution.png")
    plt.show()

    # 2. Dropout Mentions Over Time
    plt.figure(figsize=(10, 6))
    sns.countplot(x='year', hue='dropout_mentioned', data=df_viz, palette='Set2', order=year_order)
    plt.title('Dropout Mentions by Year')
    plt.xlabel('Year')
    plt.ylabel('Number of Posts')
    plt.legend(title="Dropout Mentioned")
    plt.tight_layout()
    plt.savefig("dropout_mentions_by_year.png")
    plt.show()

    # 3. Sentiment per Subreddit (Heatmap)
    sentiment_by_sub = df_viz.groupby(['subreddit', 'sentiment_label']).size().unstack().fillna(0)
    plt.figure(figsize=(10, 6))
    sns.heatmap(sentiment_by_sub, annot=True, fmt='g', cmap='YlGnBu')
    plt.title("Sentiment per Subreddit")
    plt.xlabel("Sentiment")
    plt.ylabel("Subreddit")
    plt.tight_layout()
    plt.savefig("sentiment_heatmap_subreddit.png")
    plt.show()

    # 4. Insight Summary
    total_posts = len(df_viz)
    dropout_count = df_viz['dropout_mentioned'].sum()
    neutral_pct = round((df_viz['sentiment_label'] == 'Neutral').mean() * 100, 2)
    most_active_year = df_viz['year'].value_counts().idxmax()
    top_subreddit = df_viz['subreddit'].value_counts().idxmax()

    print("\nINSIGHTS:")
    print(f"• Total Reddit Posts Analyzed: {total_posts}")
    print(f"• Posts Mentioning Dropout: {dropout_count} ({(dropout_count / total_posts) * 100:.2f}%)")
    print(f"• Neutral Sentiment Dominates: {neutral_pct}% of posts")
    print(f"• Year with Most Posts: {most_active_year}")
    print(f"• Most Active Subreddit: r/{top_subreddit}")

except Exception as viz_err:
    logger.error(f"Visualization error: {viz_err}")
