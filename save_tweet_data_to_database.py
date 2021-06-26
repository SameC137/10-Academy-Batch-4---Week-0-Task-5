import os
import pandas as pd
import mysql.connector as mysql

from mysql.connector import Error
import logging
import sys

from extract_dataframe import TweetDfExtractor
from extract_dataframe import read_json
from clean_tweets_dataframe import Clean_Tweets



logger = logging.getLogger()
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"))
logging.basicConfig(level=logging.INFO)
log_handler.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

def DBConnect(dbName=None):
    
    conn = mysql.connect(host='localhost', user='10_acadamy', password="password",
                         database=dbName, buffered=True)
    cur = conn.cursor()
    logger.info("Database Connected")
    return conn, cur

def emojiDB(dbName: str) -> None:
    conn, cur = DBConnect(dbName)
    dbQuery = f"ALTER DATABASE {dbName} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"
    cur.execute(dbQuery)
    conn.commit()

def createDB(dbName: str) -> None:
    conn, cur = DBConnect()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbName};")
    conn.commit()
    cur.close()

def createTables(dbName: str) -> None:
    conn, cur = DBConnect(dbName)
    sqlFile = 'data_schema.sql'
    fd = open(sqlFile, 'r')
    readSqlFile = fd.read()
    fd.close()

    sqlCommands = readSqlFile.split(';')

    for command in sqlCommands:
        try:
            res = cur.execute(command)
        except Exception as ex:
            logger.error("Command skipped: ", command)
            logger.error(ex)
    conn.commit()
    cur.close()

    return

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    
    cols_2_drop = ['original_text',"possibly_sensitive"]
    try:
        df = df.drop(columns=cols_2_drop, axis=1)
        df = df.fillna("NULL")
    except KeyError as e:
        logger.error("Error:", e)

    return df


def insert_to_tweet_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    conn, cur = DBConnect(dbName)

    df = preprocess_df(df)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (created_at, source, polarity, subjectivity, language,
                    favorite_count, retweet_count, original_author, followers_count, friends_count,
                    hashtags, user_mentions, place, clean_text)
             VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        data = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], str(row[10]), str(row[11]),
                row[12], row[13])

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            logger.info("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            logger.error("Error: ", e)
    return

def db_execute_fetch(*args, many=False, tablename='', rdf=True, **kwargs) -> pd.DataFrame:
    connection, cursor1 = DBConnect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    # get column names
    field_names = [i[0] for i in cursor1.description]

    # get column values
    res = cursor1.fetchall()

    # get row count and show info
    nrow = cursor1.rowcount
    if tablename:
        logger.info(f"{nrow} recrods fetched from {tablename} table")

    cursor1.close()
    connection.close()

    # return result
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res


if __name__ == "__main__":
    createDB(dbName='Tweets')
    emojiDB(dbName='Tweets')
    createTables(dbName='Tweets')

    _, tweet_list = read_json("data/covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 
    
    clean_tweets=Clean_Tweets(tweet_df)
    clean_df= clean_tweets.clean_hashtags(tweet_df)
    clean_df= clean_tweets.clean_mentions(tweet_df)
    clean_df= clean_tweets.remove_mentions_from_frame(clean_df)  
    clean_df= clean_tweets.remove_hastags_from_tweet(clean_df)
    clean_df=clean_tweets.convert_to_numbers(clean_df)
    # clean_df=clean_tweets.convert_to_datetime(clean_df)
    
    df=clean_tweets.convert_to_str(clean_df)
    df= clean_tweets.convert_to_lists(clean_df)
    
    df.to_csv('processed_tweet_data.csv', index=False)
    
    # df = pd.read_csv('processed_tweet_data.csv')
    

    insert_to_tweet_table(dbName='Tweets', df=df, table_name='TweetData')
