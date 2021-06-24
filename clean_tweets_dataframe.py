import pandas as pd
from pandas.core.frame import DataFrame
from extract_dataframe import read_json,TweetDfExtractor
import string

import re
class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        # df = df[df['polarity'] != 'polarity']
        
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        
        df["original_text"]=df["original_text"].astype(str)
        df.drop_duplicates(subset=["original_text"] , inplace=True)

        return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        df['created_at']=pd.to_datetime(df['created_at'], format='%a %b %d %H:%M:%S %z %Y')
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        df['polarity'] = pd.to_numeric(df['polarity'],errors='coerce')
        df['subjectivity'] = pd.to_numeric(df['subjectivity'],errors='coerce')
        df['retweet_count'] = pd.to_numeric(df['retweet_count'],errors='coerce')
        df['favorite_count'] = pd.to_numeric(df['favorite_count'],errors='coerce')
        
        df['followers_count'] = pd.to_numeric(df['followers_count'],errors='coerce')
        
        df['friends_count'] = pd.to_numeric(df['friends_count'],errors='coerce')
        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        unwanted_rows=df[df['lang'] != "en"].index

        df.drop(unwanted_rows ,inplace=True)
        
        return df
    def remove_mentions_from_frame(self,df:pd.DataFrame)->pd.DataFrame:
        def remove_mention_from_tweet(p)->str:
            text_with_mentions_removed= re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', p)
            return text_with_mentions_removed
        df["clean_text"]= df.original_text.apply(remove_mention_from_tweet)
        return df
    def remove_hastags_from_tweet(self,df:pd.DataFrame)->pd.DataFrame:
        def remove_hashtag_from_tweet(p)->str:
            text_with_mentions_removed= re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', p)
            return text_with_mentions_removed
        df["clean_text"]= df.clean_text.apply(remove_hashtag_from_tweet)
        return df
    def remove_null(self,df:pd.DataFrame)->pd.DataFrame:
            df.dropna(inplace=True,)
            return df
    def clean_hashtags(self,df:pd.DataFrame)->pd.DataFrame:
        def extract_hashtag(p)->list:
            hashtags=[]
            if len(p)>0:
                for i in p:
                    hashtags.append("#"+ i["text"])
            return hashtags
        df["hashtags"]=df.hashtags.apply(extract_hashtag)
        return df
    def convert_to_lists(self,df:pd.DataFrame)->pd.DataFrame:
        df['clean_text'] =  df['clean_text'].to_list()
        df['lang'] = df['lang'].to_list()
        df['hashtags'] = df['hashtags'].to_list()
        return df
    def convert_to_str(self,df:pd.DataFrame)->pd.DataFrame:

        df['clean_text']=df['clean_text'].astype(str)
        df['clean_text'] = df['clean_text'].apply(lambda x: x.lower())
        df['clean_text']= df['clean_text'].apply(lambda x: x.translate(str.maketrans(' ', ' ', string.punctuation)))
        return df






if __name__ == "__main__":
    _, tweet_list = read_json("data/covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 
    clean_tweets=Clean_Tweets(tweet_df)

    tweet_df=clean_tweets.remove_null(tweet_df)

    clean_df= clean_tweets.clean_hashtags(tweet_df)
    clean_df= clean_tweets.remove_mentions_from_frame(clean_df)  
    clean_df= clean_tweets.remove_hastags_from_tweet(clean_df)
    
    listed= clean_tweets.convert_to_lists(clean_df)


