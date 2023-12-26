import streamlit as st 
from urllib.parse import quote_plus
from pymongo import MongoClient
from googleapiclient.discovery import build
import mysql.connector as sql
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from streamlit_option_menu import option_menu
import time
import re
import pandas as pd
from database import ischannelidexists,getdatacollection

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCZbaYXujDA8jBQ5KT0DMSa2IF-r69E_oM"
comment_data=[]

#connecting youtube
youtube =build(api_service_name, api_version,developerKey=api_key)


#connecting mongodb
username = "yaminimohan1998"
password = "Faith@0808"
cluster_uri = "cluster0.vmi2h7z.mongodb.net"

# Escape the username and password
escaped_username = quote_plus(username)
escaped_password = quote_plus(password)

# Construct the updated connection string
connection_string = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster_uri}/"
myclient = MongoClient(connection_string)

#myclient = MongoClient("mongodb://localhost:27017/")
db = myclient["youtubedata"] 
collection_channeldetails = db["channel_details"]
collection_playlistdetails = db["playlist_details"]
collection_videodetails = db["video_details"]

#getting channel details
def getchanneldata(channelid):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channelid
        )
    response = request.execute()
    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscription_Count=i["statistics"]["subscriberCount"],
                  Channel_Views=i["statistics"]["viewCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"],
                 )
    return data


#getting playlist details
def getplaylistdata(channelid):
  playlist_data=[]
  next_page_token = None
  while True:
      try:
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channelid,
            maxResults = 50,
            pageToken = next_page_token
        )

        playlistres=request.execute()
        for i in playlistres["items"]:
            data=dict(Playlist_Id=i["id"],
                        Playlist_Name=i["snippet"]["title"],
                        channel_Id=i["snippet"]["channelId"])
            playlist_data.append(data)
        next_page_token = playlistres.get("nextPageToken")
        if next_page_token is None:
                break
      except HttpError as e:
          if e.resp.status == 403 and 'quotaExceed' in str(e):
                time.sleep(60)
  return playlist_data


#getting video details
def getvideodata(channelid):
    channelres = youtube.channels().list(id=channelid, part="contentDetails").execute()
    Playlist_Id = channelres["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None
    video_ids = []
    video_data = []

    while True:
        playlistres = youtube.playlistItems().list(part="snippet", playlistId=Playlist_Id, maxResults=50,
                                                   pageToken=next_page_token).execute()

        for i in range(len(playlistres['items'])):
            video_ids.append(playlistres["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = playlistres.get("nextPageToken")

        if next_page_token is None:
            break

    for j in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",
                                        id=j)
        videores = request.execute()
        for k in videores["items"]:
            data = dict(Channel_Name=k["snippet"]["channelTitle"],
                        Channel_Id=channelid,
                        Video_Id=k["id"],
                        Video_Name=k["snippet"]["title"],
                        Video_Description=k["snippet"]["description"],
                        Tags=k["snippet"].get("tags", []),
                        PublishedAt=k["snippet"]["publishedAt"],
                        View_Count=k["statistics"]["viewCount"],
                        Like_Count=k["statistics"].get("likeCount", 0),
                        Dislike_Count=k["statistics"].get("dislikeCount", 0),
                        Favourite_Count=k["statistics"].get("favoriteCount",0),
                        Comment_Count=k["statistics"].get("commentCount",0),
                        Duration=getduration(k["contentDetails"]["duration"]),
                        Thumbnail=k["snippet"]["thumbnails"]["default"]["url"],
                        Caption_Status=k["contentDetails"]["caption"],
                        Comments=getcommentdata(k["id"])
                        )
            video_data.append(data)
    return video_data

#converting duration in time format
def getduration(duration_str):
    duration_pattern = re.compile(r'P(?:T(?:(?:(\d+)H)?(\d+)M)?(\d+)S)?')
    match = duration_pattern.match(duration_str)

    if match:
        hours, minutes, seconds = map(int, match.groups(default='0'))
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        return str(duration)
    else:
        return "0:00:00"

      

#getting comment details
def getcommentdata(videoid):
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=videoid,
        maxResults=50
    )

    try:
        commentres = request.execute()
    except HttpError as e:
        if e.resp.status == 403:
            return []

    comment_data = []
    for y in commentres["items"]:
        data = dict(
            Comment_Id=y["snippet"]["topLevelComment"]["id"],
            Comment_Text=y["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
            Comment_Autho=y["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
            Comment_PublishedAt=y["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        )
        comment_data.append(data)

    return comment_data

#moving dats to mongodb
def getyoutubedata(channelid):
    #  try:
  if collection_channeldetails.find_one({'Channel_Id':channelid}):
    st.warning("Channel details exists!")
  else:
      with st.spinner("Inserting Data into MongoDB..."):
        channeldata=getchanneldata(channelid)
        collection_channeldetails.insert_one(channeldata)
        playlistdata=getplaylistdata(channelid)
        collection_playlistdetails.insert_many(playlistdata)
        videodata=getvideodata(channelid)
        collection_videodetails.insert_many(videodata)
        st.success("Successfully Inserted in MongoDB",)
        st.balloons()
      
    #  except:
    #      st.warning("Something went wrong...")

#streamlit UI
def scrapdatapage():
 st.title("Youtube data harvesting")
 tabs_title = [
        "Scrap Youtube data to MongoDB",
        "Move to MySQL"
    ]
 tab=st.tabs(tabs_title)
 with tab[0]:
  channel_id=st.text_input("Channel Id")
  isbuttonclicked = st.button("Move to MongoDB")
  if(isbuttonclicked):
   getyoutubedata(channel_id)
    
 with tab[1]:
    channel_data_get = collection_channeldetails.find({},{"Channel_Id":1,"Channel_Name":1}) 
    channel_data = list(channel_data_get)
    channel_ids = [channel["Channel_Id"] for channel in channel_data]
    channel_names = [channel["Channel_Name"] for channel in channel_data]
    selected_option_index = st.selectbox("Select Channel",range(len(channel_names)),format_func=lambda i:channel_names[i])
    buttonclicked = st.button("Export data to MySQL")
    selected_channel_id = channel_ids[selected_option_index]
    if buttonclicked:
        ischannelexist = ischannelidexists(selected_channel_id)
        if ischannelexist:
            st.warning("This channel records already exists!")
        else:
            getdatacollection(selected_channel_id)