Reddit Education Dropout Analysis Project

This project extracts Reddit posts related to education and school dropouts, transforms them with sentiment and dropout detection, stores the data in a MySQL database, and generates insights through visualizations.


INSTRUCTIONS TO RUN THE CODE

1. download the project and open it in Visual Studio Code or any IDE

2. Make sure MySQL server (MySQL Workbench) is running and the database is set up. 

To setup: 

CREATE DATABASE IF NOT EXISTS reddit_education;

USE reddit_education;


'CREATE TABLE dimsubreddit (
  subreddit_id int NOT NULL AUTO_INCREMENT,
  name varchar(255) DEFAULT NULL,
  PRIMARY KEY (subreddit_id),
  UNIQUE KEY name (name)
) ENGINE=InnoDB AUTO_INCREMENT=133 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci'

'CREATE TABLE dimtime (
  time_id int NOT NULL AUTO_INCREMENT,
  year int DEFAULT NULL,
  PRIMARY KEY (time_id)
) ENGINE=InnoDB AUTO_INCREMENT=361 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci'

'CREATE TABLE factpost (
  post_id varchar(20) NOT NULL,
  content text,
  url text,
  sentiment float DEFAULT NULL,
  dropout_mentioned tinyint(1) DEFAULT NULL,
  subreddit_id int DEFAULT NULL,
  time_id int DEFAULT NULL,
  PRIMARY KEY (post_id),
  KEY subreddit_id (subreddit_id),
  KEY time_id (time_id),
  CONSTRAINT factpost_ibfk_1 FOREIGN KEY (subreddit_id) REFERENCES dimsubreddit (subreddit_id),
  CONSTRAINT factpost_ibfk_2 FOREIGN KEY (time_id) REFERENCES dimtime (time_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci'


3. pip install commands to run the code

Library					Supported Python Versions
praw					3.7 – 3.12 ✅
pandas					3.8 – 3.12 ✅
textblob				3.7 – 3.11 ✅
seaborn					3.8 – 3.12 ✅
matplotlib				3.7 – 3.12 ✅
mysql.connector				3.7 – 3.12 ✅
sqlalchemy				3.7 – 3.12 ✅
python-dotenv				3.6+ ✅
logging, os, re, datetime, time		Built-in

pip install praw
pip install pandas
pip install textblob
pip install seaborn
pip install matplotlib
pip install mysql-connector-python
pip install SQLAlchemy
pip install python-dotenv


4. Ensure you have .env that consist of:

REDDIT_CLIENT_ID=puWwrb2HDM5BGm15rd3-dw
REDDIT_CLIENT_SECRET=mK_KRB3d8igVhpHqGVa8wNEZJVAhFg
REDDIT_USER_AGENT=student-dropout-analysis

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DB=reddit_education


5. Run the main script to:

- Extract and transform Reddit posts
- Analyze sentiment and dropout relevance
- Load the data into MySQL
- Generate visualizations and summary insights

