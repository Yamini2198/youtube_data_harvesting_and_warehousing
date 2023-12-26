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

#connecting mysql
mysql_connection = sql.connect(
    host='localhost',
    user='root',
    password='Faith@0808',
    database='youtubedata',
    port=3306
)

mysql_cursor = mysql_connection.cursor()


def getdatacollection(channelid):
  with st.spinner("Inserting Data into SQL..."):
    insertchanneldetails(channelid)
    insertplaylistdetails(channelid)
    insertvideodetails(channelid)
     
    st.success("Records Inserted Successfully in mysql!")
    st.snow()

def insertchanneldetails(channelid):
    if not mysql_connection.is_connected():
        mysql_connection.reconnect()
        channelcursor = mysql_connection.cursor()
    channeldata = collection_channeldetails.find_one({'Channel_Id':channelid})
    channelquery="insert into channeldetail(channel_id,channel_name,subscription_count,channel_views,channel_description) values(%s,%s,%s,%s,%s)"
    channelvalues = (channeldata["Channel_Id"],channeldata["Channel_Name"],channeldata["Subscription_Count"],channeldata["Channel_Views"],channeldata["Channel_Description"])
    channelcursor.execute(channelquery,channelvalues)
    mysql_connection.commit()
    channelcursor.close()
    mysql_connection.close()

def insertplaylistdetails(channelid):
    mysql_connection.reconnect()
    playlistcursor = mysql_connection.cursor()
    playlistdata = collection_playlistdetails.find({'channel_Id':channelid})
    for i in playlistdata:
          playlistquery = "insert into playlistdetail(channel_id,playlist_id,playlist_name) values(%s,%s,%s)"
          playlistvalues = (i["channel_Id"],i["Playlist_Id"],i["Playlist_Name"])
          playlistcursor.execute(playlistquery,playlistvalues)
    mysql_connection.commit()
    playlistcursor.close()
    mysql_connection.close()

def insertvideodetails(channelid):
    mysql_connection.reconnect()
    videocursor = mysql_connection.cursor()
    videodata = collection_videodetails.find({'Channel_Id':channelid})
    for j in videodata:
         published_datetime = datetime.strptime(j["PublishedAt"], '%Y-%m-%dT%H:%M:%SZ')
         formatted_published_datetime = published_datetime.strftime('%Y-%m-%d %H:%M:%S')
         videodataquery = "insert into videodetail(channel_id,video_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_comment,comment_count,duration,thumbnail,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
         videodatavalues = (j["Channel_Id"],j["Video_Id"],j["Video_Name"],j["Video_Description"],formatted_published_datetime,j["View_Count"],j["Like_Count"],j["Dislike_Count"],j["Favourite_Count"],j["Comment_Count"],j["Duration"],j["Thumbnail"],j["Caption_Status"])
         videocursor.execute(videodataquery,videodatavalues)
         for k in j["Comments"]:
                commentdataquery = "insert into commentdetail(video_id,comment_id,comment_text,comment_author,comment_published_date) values(%s,%s,%s,%s,%s)"
                commentdatavalues = (j["Video_Id"],k["Comment_Id"],k["Comment_Text"],k["Comment_Autho"], formatted_published_datetime)
                videocursor.execute(commentdataquery, commentdatavalues)
    mysql_connection.commit()
    videocursor.close()
    mysql_connection.close()

def getdatasfrommysql(query):
        mysql_connection.reconnect()
        channelcursor = mysql_connection.cursor()
        channelcursor.execute(query)
        header_names =[i[0] for i in channelcursor.description]
        rows = channelcursor.fetchall()
        data = pd.DataFrame(rows, columns = header_names)
        channelcursor.close()
        mysql_connection.close()
        return data
       
    
def ischannelidexists(channelid):
        mysql_connection.reconnect()
        existschannelcursor = mysql_connection.cursor()
        query = f"select  *from channeldetail where channel_id='{channelid}';"
        existschannelcursor.execute(query)
        result = existschannelcursor.fetchone()
        existschannelcursor.close()                    
        mysql_connection.close()
        return result