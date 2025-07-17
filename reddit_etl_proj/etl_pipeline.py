import praw
import pandas as pd
from textblob import TextBlob
import mysql.connector
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

# Load to MySQL
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