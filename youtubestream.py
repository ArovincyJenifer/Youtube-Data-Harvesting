
import os

import googleapiclient.discovery
from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from streamlit import connections
import pymysql
import json
import streamlit as st
from tabulate import tabulate

import mysql.connector

mydb = mysql.connector.connect(host="localhost",user="root", password="")
mycursor = mydb.cursor(buffered=True)

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="",
                               db="guvi"))



api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyCh-Q1hHR5sl3FDutZujx_tTW3wfHc_57Q'
#channel_id = 'UCnr5rRnroTWAAbO9_sIhghg'

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def channel_information(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,status,statistics",id=channel_id)
    response = request.execute()
      
    snippet = response['items'][0]['snippet']
    statistics = response['items'][0]['statistics']
    content_details = response['items'][0]['contentDetails']
    
    channel_data = {
        "channel_id": channel_id, 
        "channel_name": snippet['title'],
        "channel_type": response['items'][0]['kind'],
        "channel_views": statistics.get('viewCount', 0),
        "channel_description": snippet['description'],
        "channel_status" : response['items'][0]['status']['privacyStatus'],
        "channel_published": snippet['publishedAt'],
        
        "channel_subscription": statistics.get('subscriberCount', 0),  # Using .get() to handle missing keys
        
        "channel_vc": statistics.get('videoCount', 0),
              
    }
    #channel_playlistID = content_details['relatedPlaylists']['uploads']
    channel_df = pd.DataFrame(channel_data,index=[0])
    channel_df.to_sql('channel', con=engine, if_exists='append', index=False)

    return channel_df

def play(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,status,statistics",id=channel_id)
    response = request.execute()

    snippet = response['items'][0]['snippet']
    statistics = response['items'][0]['statistics']
    content_details = response['items'][0]['contentDetails']

    playlistID =content_details['relatedPlaylists']['uploads']

    return playlistID

def playlistitems(channel_id):
    playlist_id = play(channel_id)
    request = youtube.playlistItems().list(part='snippet,contentDetails', playlistId=playlist_id)
    response = request.execute()

    playlist_id = response['items'][0]['snippet']['playlistId']
    channel_id = response['items'][0]['snippet']['channelId']
    playlist_name = response['items'][0]['snippet']['title']

    sql = "INSERT INTO guvi.playlist (playlist_id, channel_id, playlist_name) VALUES (%s, %s, %s)"

    mydb.commit()

    return playlist_id

#Func for PlaylistItems and get no of videos:

def get_video_ids(playlist_id):
    video_ids = []
    next_page = None
    while True:
        request = youtube.playlistItems().list(part = 'snippet,contentDetails',playlistId=playlist_id,
        maxResults=50,pageToken=next_page)
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page = response.get('nextPageToken')
        if next_page is None:
            break
    return video_ids

# # No of videos in playlistId

#no_of_videos = get_video_ids(playlist_id)
# #print(no_of_videos)

def fetch_videos_data(no_of_videos):
    all_videos_data = []

    for i in no_of_videos:
        request = youtube.videos().list(part="snippet,statistics,contentDetails",   id=i)
        response = request.execute()
        if 'items' in response and response['items']:
            video_data = response['items'][0]
            snippet = video_data.get('snippet', {})
            content_details = video_data.get('contentDetails', {})
            statistics = video_data.get('statistics', {})

            vc_data =  {
                "video_id": video_data.get('id', ''),
                "playlist_id": content_details.get('relatedPlaylists', {}).get('uploads', ''),
                "video_name": snippet.get('title', ''),
                "video_description": snippet.get('description', ''),
                "published_date": snippet.get('publishedAt', ''),
                "view_count": statistics.get('viewCount', ''),
                "like_count": statistics.get('likeCount', ''),
                "favorite_count": statistics.get('favoriteCount', ''),
                "comment_count": statistics.get('commentCount', ''),
                "duration": content_details.get('duration', ''),
                "thumbnails": snippet.get('thumbnails', {}).get('default', {}).get('url', ''),
                "caption_status": content_details.get('caption', '')       
            }

            all_videos_data.append(vc_data)
        else:
            print(f"Video with ID {i} not found.")

    return all_videos_data


#Func for calling the comments Data:

def fetch_comments_data(no_of_videos):
    all_comments = []


    for vid in no_of_videos:
        request = youtube.commentThreads().list(part="snippet", maxResults=100, videoId=vid)
        response = request.execute()
    
        for item in response.get('items', []):
            comment_information = {
            "comment_id": item['snippet']['topLevelComment']['id'],
            "video_id": item['snippet']['videoId'],
            "comment_text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
            "comment_author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            "comment_published_date": item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            all_comments.append(comment_information)
    #pprint(all_comments)


    return all_comments



def main():
    st.title("Youtube Channel Data Extraction")

    channel_id = st.text_input("Enter your channel id here:")

    if st.button("Click Me"):
        if not channel_id:
            st.warning("Enter valid channel id here")
        else:
            channel_df = channel_information(channel_id)
            st.subheader("Channel Information")
            st.write(channel_df)
            st.success("Channel data successfully saved to the database.")

            # Insert playlist items into the database
            playlist_id = playlistitems(channel_id)
            st.success("Playlist data successfully saved to the database.")

            # Fetch video IDs
            no_of_video_ids = get_video_ids(playlist_id)

            # Fetch video details
            videos_data_list = fetch_videos_data(no_of_video_ids)
            videos_df = pd.DataFrame(videos_data_list)
            videos_df.to_sql('video', con=engine, if_exists='append', index=False)
            st.success("Videos data successfully saved to the database.")

            # Fetch comments data
            try:
                comment_data = fetch_comments_data(no_of_video_ids)
                comment_df = pd.DataFrame(comment_data)
                comment_df.to_sql('comment', con=engine, if_exists='append', index=False)
                st.success("Comments data successfully saved to the database.")
                st.write('Thank You! Please perform SQL retrieval Operation')
            except Exception as e:
                st.error("An error occurred: {}".format(e))

if __name__ == "__main__":
    main()
    st.sidebar.title("SQL Query Retrieval")
    selected_option = st.sidebar.selectbox(
    'Choose any one of the SQL Query',
    ('---Select a Query----',
        '1.What are the names of all the videos and their corresponding channels?','2.Which channels have the most number of videos, and how many videos do   they have?','3.What are the top 10 most viewed videos and their respective channels?','4.How many comments were made on each video, and what are their corresponding video names?','5.Which videos have the highest number of likes, and what are their corresponding channel names?','6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?','7.What is the total number of views for each channel, and what are their corresponding channel names?','8.What are the names of all the channels that have published videos in the year 2022?','9.What is the average duration of all videos in each channel, and what are their corresponding channel names?','10.Which videos have the highest number of comments, and what are their corresponding channel names?'))
# Display the selected option

    if selected_option == '1.What are the names of all the videos and their corresponding channels?':

        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               SELECT Distinct c.channel_name, v.  video_name        FROM             guvi.channel c  JOIN guvi.playlist p ON c.channel_id = p.channel_id 
                                JOIN guvi.video v ON p.playlist_id = v.playlist_id""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '2.Which channels have the most number of videos, and how many videos do   they have?':

        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               SELECT c.channel_name, COUNT(v.video_id) AS video_count     FROM guvi.channel c JOIN guvi.playlist p ON c.channel_id = p.channel_id JOIN guvi.video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name ORDER BY video_count DESC;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '3.What are the top 10 most viewed videos and their respective channels?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               SELECT c.channel_name, v.video_name, v.view_count
                               FROM guvi.channel c
                               JOIN guvi.playlist p ON c.channel_id = p.channel_id
                                JOIN guvi.video v ON p.playlist_id = v.playlist_id
                                ORDER BY v.view_count DESC
                                LIMIT 10;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '4.How many comments were made on each video, and what are their corresponding video names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               select video_name,comment_count from video GROUP by video_id;""",engine)
        results = st.dataframe(query_df)
       
    elif selected_option == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               SELECT DISTINCT c.channel_name, v.video_name, v.like_count FROM video v JOIN playlist p ON p.playlist_id = v.playlist_id JOIN channel c ON c.channel_id = p.channel_id WHERE v.like_count = (SELECT MAX(v1.like_count) FROM video v1 WHERE v1.playlist_id = v.playlist_id) order by like_count asc;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               SELECT video_name,like_count from video  order by video_name asc;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                               select channel_name,channel_views from channel order by channel_name asc;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '8.What are the names of all the channels that have published videos in the year 2022?':
         st.write('You selected:', selected_option)
         query_df = pd.read_sql("""
                               SELECT DISTINCT c.channel_name, YEAR(v.published_date) FROM video v JOIN playlist p ON p.playlist_id = v.playlist_id JOIN channel c ON c.channel_id = p.channel_id WHERE YEAR(v.published_date) = 2022;""",engine)
         results = st.dataframe(query_df)
         
    elif selected_option == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                                SELECT c.channel_name,AVG(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'H', -1), 'PT', -1) * 3600 + SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', 1), 'T', -1) * 60 + SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'M', -1)) AS avg_duration_seconds FROM video v JOIN playlist p ON p.playlist_id = v.playlist_Id JOIN channel c ON c.channel_id = p.channel_id GROUP BY c.channel_name;""",engine)
        results = st.dataframe(query_df)
        
    elif selected_option == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""
                                SELECT c.channel_name, v.video_name, v.comment_count FROM video v JOIN playlist p on p.playlist_id = v.playlist_Id JOIN channel c on c.channel_id = p.channel_id WHERE v.comment_count = (SELECT MAX(comment_count) FROM video);""",engine)
        results = st.dataframe(query_df)
       






