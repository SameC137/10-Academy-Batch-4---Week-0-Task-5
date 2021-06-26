import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from wordcloud import WordCloud
import plotly.express as px
from save_tweet_data_to_database import db_execute_fetch

from clean_tweets_dataframe import Clean_Tweets

st.set_page_config(page_title="Day 5", layout="wide")

def loadData():
    query = "select * from TweetData"
    df = db_execute_fetch(query, dbName="tweets", rdf=True)
    return df

def selectHashTag():
    df = loadData()
    hashTags_list=[   ]
    
    df["hashtags"]=df["hashtags"].apply(lambda p : p.strip('][').replace("'","").split(', '))
    for hashtags in df["hashtags"]:
        for hashtag in hashtags:
            if hashtag!='':
                hashTags_list.append(hashtag.strip("'"))
    
    hashTags = st.multiselect("choose combination of hashtags",  list(set(hashTags_list)))
    if hashTags:
        # np.frompyfunc(lambda  a,b: any(x in a for x in b))
        # df = df[df['hashtags'].isin(hashTags)]
        # df=df[np.logical_and.reduce([df.isin(x).any(1) for x in hashTags])]
        # df=df[df.stack().str.contains('|'.join(hashTags)).any(level=0)]
        df=df[df.apply(lambda r: any([kw in r["hashtags"] for kw in hashTags]), axis=1)]

        st.write(df)

def selectLoc():
    df = loadData()
    location = st.multiselect("choose Location of tweets", list(df['place'].unique()))
    if location:
        df = df[np.isin(df, location).any(axis=1)]
        st.write(df)
def selectUser():
    df = loadData()
    author = st.multiselect("choose tweet user", list(df['original_author'].unique()))
    if author:
        df = df[np.isin(df, author).any(axis=1)]
        st.write(df)
def selectMentions():
    df = loadData()
    mentions_list=[   ]
    
    df["user_mentions"]=df["user_mentions"].apply(lambda p : p.strip('][').replace("'","").split(', '))
    for mentions in df["user_mentions"]:
        for mention in mentions:
            if mention!='':
                mentions_list.append(mention.strip("'"))
    
    hashTags = st.multiselect("choose combination of mentions",  list(set(mentions_list)))
    if hashTags:

        df=df[df.apply(lambda r: any([kw in r["user_mentions"] for kw in hashTags]), axis=1)]

        st.write(df)

def barChart(data, title, X, Y):
    title = title.title()
    st.title(f'{title} Chart')
    msgChart = (alt.Chart(data).mark_bar().encode(alt.X(f"{X}:N", sort=alt.EncodingSortField(field=f"{Y}", op="values",
                order='ascending')), y=f"{Y}:Q"))
    st.altair_chart(msgChart, use_container_width=True)

def wordCloud():
    df = loadData()
    cleanText = ''
    for text in df['clean_text']:
        tokens = str(text).lower().split()

        cleanText += " ".join(tokens) + " "

    wc = WordCloud(width=650, height=450, background_color='white', min_font_size=5).generate(cleanText)
    st.title("Tweet Text Word Cloud")
    st.image(wc.to_array())

def stBarChart():
    df = loadData()
    dfCount = pd.DataFrame({'Tweet_count': df.groupby(['original_author'])['clean_text'].count()}).reset_index()
    dfCount["original_author"] = dfCount["original_author"].astype(str)
    dfCount = dfCount.sort_values("Tweet_count", ascending=False)

    num = st.slider("Select number of Rankings", 0, 50, 5)
    title = f"Top {num} Ranking By Number of tweets"
    barChart(dfCount.head(num), title, "original_author", "Tweet_count")

def hashtagBarChart():
    
    data=loadData()
    
    data["hashtags"]=data["hashtags"].apply(lambda p : p.strip('][').replace("'","").split(', '))
    hashtags_list_df = data.loc[
    data.hashtags.apply(
                        lambda hashtags_list: hashtags_list !=[] and  hashtags_list !=['']
                    ),['hashtags']]
    flattened_hashtags_df = pd.DataFrame(
        [hashtag for hashtags_list in hashtags_list_df.hashtags
        for hashtag in hashtags_list],
        columns=['hashtag'])
    dfCount = pd.DataFrame({'Tweet_count': flattened_hashtags_df.groupby(['hashtag'])['hashtag'].count()}).reset_index()
    
    dfCount = dfCount.sort_values("Tweet_count", ascending=False)

    num = st.slider("Select number of Rankings", 0, 50, 5, key="1")
    title = f"Top {num} Ranking By Number of tweets"

    barChart(dfCount.head(num), title, "hashtag", "Tweet_count")

def langPie():
    df = loadData()
    dfLangCount = pd.DataFrame({'Tweet_count': df.groupby(['language'])['clean_text'].count()}).reset_index()
    dfLangCount["language"] = dfLangCount["language"].astype(str)
    dfLangCount = dfLangCount.sort_values("Tweet_count", ascending=False)
    dfLangCount.loc[dfLangCount['Tweet_count'] < 10, 'lang'] = 'Other languages'
    st.title(" Tweets Language pie chart")
    fig = px.pie(dfLangCount, values='Tweet_count', names='language', width=500, height=350)
    fig.update_traces(textposition='inside', textinfo='percent+label')

    st.plotly_chart(fig)
        
def retweetBar():
    df = loadData()
    
    df = df.sort_values("retweet_count", ascending=False)
    fig=px.histogram(df, x="retweet_count",title='Histogram of retweets log scale',log_y=True)
    st.plotly_chart(fig)
def followeBar():
    df = loadData()
    
    df=df.loc[df['followers_count'] < 100000] 
    dfFollower = pd.DataFrame({'Users_Count': df.groupby(['followers_count'])['clean_text'].count()}).reset_index()
    # dfFollower["followers_count"] = dfFollower["followers_count"].astype(int)
    # dfFollower = dfFollower.sort_values("followers_count", ascending=False)
    # dfFollower.loc[dfFollower['followers_count'] > 30000, 'followers_count'] = 'Followers > 30000'  
    dfFollower = dfFollower.sort_values("Users_Count", ascending=False)
    fig=px.histogram(dfFollower, y="Users_Count",x="followers_count",title='Log Histogram of follower counts less than 100,000',nbins=10000,log_y=True)
    st.plotly_chart(fig)

st.title("Data Display")
selectHashTag()
st.markdown("<p style='padding:10px; background-color:#000000;color:#00ECB9;font-size:16px;border-radius:10px;'>Section Break</p>", unsafe_allow_html=True)
selectLoc()
selectMentions()
selectUser()
st.title("Data Visualizations")
wordCloud()
with st.beta_expander("Show More Graphs"):
    hashtagBarChart()
    retweetBar()
    stBarChart()
    langPie()
    followeBar()
