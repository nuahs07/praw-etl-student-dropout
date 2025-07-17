import praw
import pandas as pd
from textblob import TextBlob
import mysql.connector
import re
from datetime import datetime
from dotenv import load_dotenv
import os
import time

load_dotenv()

# 1. Extract Data using PRAW
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

keywords = [
    'Philippines dropout', 'DepEd', 'public school problems',
    'school dropout Philippines', 'education crisis',
    'modular learning', 'distance learning', 'student dropout',
    'school', 'university', 'college', 'education', 'students'
]
subreddits_to_search = ['Philippines', 'studentsph', 'AskPH']

post_limit_per_query = 200  # per keyword/subreddit combination
seen_ids = set()
posts = []

for subreddit in subreddits_to_search:
    for keyword in keywords:
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
            time.sleep(1)  # avoid rate limit
        except Exception as e:
            print(f"Error on keyword '{keyword}' in subreddit '{subreddit}': {e}")
            time.sleep(5)

print(f"Total posts collected: {len(posts)}")

df = pd.DataFrame(posts)

# 2. Transform Data
def clean_text(text):
    text = re.sub(r"http\S+|www\S+|[^a-zA-Z\s]", "", text)
    return text.lower().strip()

df['clean_content'] = df['content'].apply(clean_text)
df['sentiment'] = df['clean_content'].apply(lambda x: TextBlob(x).sentiment.polarity)
df['dropout_mentioned'] = df['clean_content'].str.contains('dropout|drop out|dropped out', case=False, na=False)
df['year'] = df['date'].apply(lambda x: x.year)

# 3. Load to MySQL
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="somallari01",
    database="reddit_education"
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

# FactPost
for _, row in df.iterrows():
    cursor.execute("""
        INSERT IGNORE INTO FactPost (
            post_id, content, url, sentiment, dropout_mentioned,
            subreddit_id, time_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        row['id'], row['clean_content'], row['url'], row['sentiment'],
        row['dropout_mentioned'], subreddit_map[row['subreddit']],
        year_map[row['year']]
    ))
    conn.commit()

cursor.close()
conn.close()

print("ETL Complete.")
