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
from database import getdatasfrommysql

def queriespage():
    questions=["What are the names of all the videos and their corresponding channels ?",
        "Which channels have the most number of videos, and how many videos do they have ?",
        "What are the top 10 most viewed videos and their respective channels ?",
        "How many comments were made on each video, and what are their corresponding video names ?",
        "Which videos have the highest number of likes, and what are their corresponding channel names ?",
        "What is the total number of likes and dislikes for each video, and what are  their corresponding video names ?",
        "What is the total number of views for each channel, and what are their corresponding channel names ?",
        "What are the names of all the channels that have published videos in the year 2022 ?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names ?",
        "Which videos have the highest number of comments, and what are their corresponding channel names ?"
        ]
    sql_queries =["""select c.channel_name as 'Channel Name' , v.video_name as 'Video Name' from channeldetail 
        as c join videodetail as v on c.channel_id = v.channel_id order by c.channel_name;""",
        """SELECT c.channel_name as 'Channel Name',COUNT(v.video_id) AS VideoCount
        FROM channeldetail c JOIN videodetail v ON c.channel_id = v.channel_id
        GROUP BY c.channel_id, c.channel_name
        ORDER BY VideoCount DESC;""",	
        """select c.channel_name as 'Channel Name' , v.view_count as 'Viewed Videos' 
        from channeldetail as c 
        join videodetail as v on c.channel_id = v.channel_id 
        order by v.view_count desc limit 10;""",
        """select v.video_name as 'Video Name', count(c.comment_id) as CommentCount
        from videodetail as v
        join commentdetail as c on v.video_id = c.video_id
        GROUP BY v.video_name
        order by CommentCount ;""",
        """SELECT c.channel_name as 'Channel Name', MAX(v.like_count) as 'Most liked videos'
        FROM channeldetail as c
        JOIN videodetail as v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name
        ORDER BY 'Most liked videos';""",
        """SELECT
        v.video_name AS 'Video Name',
        COALESCE(SUM(v.like_count), 0) AS 'Total Likes',
        COALESCE(SUM(v.dislike_count), 0) AS 'Total Dislikes'
        FROM
        videodetail AS v
        GROUP BY
        v.video_id, v.video_name;""",
        """select c.channel_name as 'Channel Name',
        sum(c.channel_views) as 'View Count'
        from channeldetail as c
        group by c.channel_id,c.channel_name;""",
        """SELECT DISTINCT c.channel_name
        FROM channeldetail AS c
        JOIN videodetail AS v ON c.channel_id = v.channel_id
        WHERE YEAR(v.published_date) = 2022;""",
        """select distinct c.channel_name as 'Channel Name',
        avg(v.duration) as 'Average Duration'
        from channeldetail as c
        join videodetail as v on c.channel_id = v.channel_id
        group by c.channel_name
        order by 'Average Duration' ;""",
        """SELECT v.video_name,c.channel_name,v.comment_count
        FROM videodetail v
        JOIN channeldetail c ON v.channel_id = c.channel_id
        ORDER BY v.comment_count DESC;"""]
    selected_question = st.selectbox("Questions",questions)
    selected_index = questions.index(selected_question)
    selected_query = sql_queries[selected_index]
    data = getdatasfrommysql(selected_query)
    st.dataframe(data)