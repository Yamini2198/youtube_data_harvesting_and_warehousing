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
from scrapping import scrapdatapage,getyoutubedata
from queries import queriespage
from database import getdatacollection

def homepage():
  st.markdown(""" ### Youtube data harvesting and warehousing...""",True)
  local_image_path = "dataharvest.jpg"
  st.image(local_image_path, caption='Data Harvest', use_column_width=True)
  st.write("""This application allows user to access and analyze data 
          from multiple youtube channels.
          It has the ability to insert and retrieve 
          all the relevant datas of a youtube channel.""")


with st.sidebar:
    selected = option_menu(
        menu_title ="Main Menu",
        options=["Home","Scrap Data","Queries"],
        icons=["house","database-add","list-check"]
    )

if selected == "Home":
 homepage()
if selected == "Scrap Data":
 scrapdatapage()
if selected == "Queries":
 queriespage()


  