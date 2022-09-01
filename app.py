import json
import pandas as pd
from flask import Flask, jsonify, request
from flask import render_template
import requests
from selenium import webdriver
from googleapiclient.discovery import build
import scrapetube
import pytube
import time
import mysql.connector as connection
import pymongo
from flask_cors import CORS, cross_origin
#mport pdfkit as pdf

app = Flask(__name__)

API_KEY = "AIzaSyDqyEQIKUTf2mx-JqQ5WjJR-XIkJnnnsS0"
Youtube = build('youtube', 'v3', developerKey=API_KEY)

@app.route("/", methods=['POST','GET'])
@cross_origin()
def index():
    return render_template('index.html')

def get_channel_id(Youtube, channel):
    channel_id = requests.get(
        f'https://www.googleapis.com/youtube/v3/search?part=id&q={channel}&type=channel&key={API_KEY}').json()[
        'items'][0]['id']['channelId']

    channels_response = Youtube.channels().list(id=channel_id,
                                            part='id, snippet, statistics, contentDetails, topicDetails').execute()
    yc_id = channels_response['items'][0]['id']
    return yc_id

@app.route('/viewchannel', methods=['POST', 'GET'])
@cross_origin()
def Browse_channel_videos():
    channel_id = request.form.get("id")
    # path = "C:/Users/bramb/Downloads/chromedriver"
    path = "C:/Users/bramb/PycharmProjects/YoutubeScrapping/chromedriver.exe"
    url = f"https://www.youtube.com/channel/{channel_id}/videos"
    driver = webdriver.Chrome(path)
    driver.get(url)
    time.sleep(300)
    return "Time Over Please, you can browse for 5 minutes only"

def get_all_video_ids(channel_id, max_result):
    videos = scrapetube.get_channel(f"{channel_id}")
    video_id = []
    for video in videos:
        if len(video_id)<max_result:
            video_id.append(video['videoId'])
    return video_id

def get_video_link(video_ids):
    links = []
    for i in range(len(video_ids)):
        video_url = "https://www.youtube.com/watch?v="+video_ids[i]
        links.insert(i,video_url)
    return links

def get_video_details(Youtube, video_ids, max_commnts):
    titles = []
    Thumbnails = []
    Likes_counts = []
    Comments_counts = []
    comments = []
    comment_authers = []
    for i in video_ids:
        video_response = Youtube.videos().list(part=['snippet', 'statistics'], id=f"{i}", maxResults=max_commnts).execute()
        comment_response = Youtube.commentThreads().list(part='snippet', videoId=f"{i}", maxResults=max_commnts).execute()
        data = video_response
        data1 = comment_response
        comment = []
        comment_auther = []
        titles.append(data['items'][0]['snippet']['title'])
        Thumbnails.append(data['items'][0]['snippet']["thumbnails"]['default']['url'])
        Likes_counts.append(data['items'][0]['statistics']['likeCount'])
        Comments_counts.append(data['items'][0]['statistics']['commentCount'])

        for i in range(len(data1['items'])):
            comment.append(data1['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'])
            comment_auther.append(data1['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'])

        comments.append(comment)
        comment_authers.append(comment_auther)
    return {"titles":titles}, {"Thumbnails":Thumbnails}, {"LikeCount":Likes_counts}, {"CommentCount":Comments_counts},{"comments":comments},{"comment_authers":comment_authers}

@app.route("/response", methods=['POST','GET'])
@cross_origin()
def response():
    channel = request.form.get("channel")
    max_result = int(request.form.get("record"))
    max_comments = int(request.form.get("comments"))
    height = (max_comments*30)+20
    #key = request.form.get("key")
    channel_id = get_channel_id(Youtube, channel)
    video_id = get_all_video_ids(channel_id, max_result)
    Video_links = get_video_link(video_id)
    data = get_video_details(Youtube, video_id, max_comments)
    cid = {"cid":channel_id}
    vid = {"video_id":video_id}
    link = {"Video_links":Video_links}
    DBdata = dict()
    DBdata["cid"] = channel_id
    DBdata["vid"] = video_id
    DBdata["link"] = Video_links
    for i in range(len(data)):
        if i==0:
            title = data[i]
            DBdata.update(title)
        elif i==1:
            Thumb = data[i]
            DBdata.update(Thumb)
        elif i==2:
            LikeCount = data[i]
            DBdata.update(LikeCount)
        elif i==3:
            commentCount = data[i]
            DBdata.update(commentCount)
        elif i==4:
            comment = data[i]
            DBdata.update(comment)
        elif i==5:
            comment_auther = data[i]
            DBdata.update(comment_auther)
    return render_template('response.html', link = link, title=title, Thumb= Thumb, LikeCount = LikeCount, commentCount=commentCount, comment=comment, comment_auther=comment_auther, channel=channel, id=vid, channel_id=channel_id, height=height, DBdata = json.dumps(DBdata))

@app.route('/mangodb', methods=['POST','GET'])
@cross_origin()
def Insertmangodb():
    data = request.form.get("mangodata")
    channel_id = request.form.get("id")
    mango_id = request.form.get("mango_id")
    #"mongodb+srv://maharaj:maharaj@cluster0.6kcud2v.mongodb.net/?retryWrites=true&w=majority"
    mango_data = json.loads(data)
    client = pymongo.MongoClient(f"{mango_id}")
    db = client.test
    db = client['Youtube']
    collection = db[f'{channel_id}']
    try:
        collection.insert_one(mango_data)
    except Exception as e:
        return "Error"
    time.sleep(3)
    return "Inserted to MangoDB"

@app.route('/sql', methods=['POST','GET'])
@cross_origin()
def Insertsql():
    data = request.form.get("sqldata")
    channel_id = request.form.get("id")
    host = request.form.get('host')
    DB = request.form.get('db')
    channel = channel_id.replace(" ","_").replace("-","_")
    user = request.form.get("user")
    password = request.form.get("password")
    conn = connection.connect(host=f'{host}', user=f'{user}', password=f'{password}')
    cursor = conn.cursor()
    cursor.execute("""CREATE DATABASE IF NOT EXISTS Youtube""")
    cursor.execute(f"""use {DB}""")
    cursor.execute(f"""CREATE Table IF NOT EXISTS {channel}(Channel_ID varchar(100),Video_ID varchar(100),Video_Link varchar(100), Title Varchar(500), Thumbnail_Link varchar(200), Like_Count varchar(100), Comment_Count varchar(100), Comments varchar(5000), Comment_Auther varchar(100))""")
    try:
        cursor.execute(f"""truncate {channel}""")
    except:
        pass
    data = json.loads(data)
    data_list=[]
    for i in range(len(data['vid'])):
        l = []
        for j in data.values():
            if type(j) == str:
                l.append(data['cid'])
            elif type(j) == list:
                l.append(j[i])
            else:
                pass
        data_list.append(l)

    final_list = []
    for i in data_list:
        row = []
        for j in i:
            if type(j)==list:
                s = "___".join(map(str,j))
                row.append(s)
            else:
                row.append(j)
        final_list.append(row)
    for i in final_list:
        print(i)
        cursor.execute(f"""Insert into {channel} values("{i[0]}","{i[1]}","{i[2]}","{i[3]}","{i[4]}","{i[5]}","{i[6]}","{i[7]}","{i[8]}")""")
        conn.commit()
    time.sleep(3)
    df = pd.DataFrame(final_list, columns=("Channel_ID ","Video_ID ","Video_Link "," Title "," Thumbnail_Link "," Like_Count "," Comment_Count "," Comments "," Comment_Auther "))
    df.to_csv(f"C:/Users/Public/Downloads/{channel}"+".csv")
    return "Data Inserted to MYSQL"

@app.route('/excel', methods=['POST','GET'])
@cross_origin()
def toExcel():
    data = request.form.get("sqldata")
    channel_id = request.form.get("id")
    channel = channel_id.replace(" ","_").replace("-","_")
    data = json.loads(data)
    data_list=[]
    for i in range(len(data['vid'])):
        l = []
        for j in data.values():
            if type(j) == str:
                l.append(data['cid'])
            elif type(j) == list:
                l.append(j[i])
            else:
                pass
        data_list.append(l)

    final_list = []
    for i in data_list:
        row = []
        for j in i:
            if type(j)==list:
                s = "___".join(map(str,j))
                row.append(s)
            else:
                row.append(j)
        final_list.append(row)
    time.sleep(3)
    df = pd.DataFrame(final_list, columns=("Channel_ID ","Video_ID ","Video_Link "," Title "," Thumbnail_Link "," Like_Count "," Comment_Count "," Comments "," Comment_Auther "))
    path = f"C:/Users/Public/Downloads/{channel}"+".xlsx"
    df.to_excel(path)
    return f"Excel saved at {path}"

@app.route('/csv', methods=['POST','GET'])
@cross_origin()
def toCSV():
    data = request.form.get("sqldata")
    channel_id = request.form.get("id")
    channel = channel_id.replace(" ","_").replace("-","_")
    data = json.loads(data)
    data_list=[]
    for i in range(len(data['vid'])):
        l = []
        for j in data.values():
            if type(j) == str:
                l.append(data['cid'])
            elif type(j) == list:
                l.append(j[i])
            else:
                pass
        data_list.append(l)

    final_list = []
    for i in data_list:
        row = []
        for j in i:
            if type(j)==list:
                s = "___".join(map(str,j))
                row.append(s)
            else:
                row.append(j)
        final_list.append(row)
    time.sleep(3)

    df = pd.DataFrame(final_list, columns=("Channel_ID ","Video_ID ","Video_Link "," Title "," Thumbnail_Link "," Like_Count "," Comment_Count "," Comments "," Comment_Auther "))
    path = f"C:/Users/Public/Downloads/{channel}"+".csv"
    df.to_csv(path)
    return f"CSV saved at {path}"

@app.route('/html', methods=['POST','GET'])
@cross_origin()
def toPDF():
    data = request.form.get("sqldata")
    channel_id = request.form.get("id")
    channel = channel_id.replace(" ","_").replace("-","_")
    data = json.loads(data)
    data_list=[]
    for i in range(len(data['vid'])):
        l = []
        for j in data.values():
            if type(j) == str:
                l.append(data['cid'])
            elif type(j) == list:
                l.append(j[i])
            else:
                pass
        data_list.append(l)

    final_list = []
    for i in data_list:
        row = []
        for j in i:
            if type(j)==list:
                s = "___".join(map(str,j))
                row.append(s)
            else:
                row.append(j)
        final_list.append(row)
    time.sleep(3)

    df = pd.DataFrame(final_list, columns=("Channel_ID ","Video_ID ","Video_Link "," Title "," Thumbnail_Link "," Like_Count "," Comment_Count "," Comments "," Comment_Auther "))
    path = f"C:/Users/Public/Downloads/{channel}"+".html"
    df.to_html(path)
    return f"HTML File saved at {path}"

@app.route("/downloads", methods=['POST','GET'])
@cross_origin()
def downloadVideo():
    link = request.form.get("link")
    video_link = f"https://www.youtube.com/watch?v={link}"
    SAVE_PATH = r"C:/Users/Public/Downloads"
    youTube = pytube.YouTube(video_link)
    stream = youTube.streams.get_highest_resolution()
    stream.download(SAVE_PATH)
    return "Video Downloaded"

if __name__=='__main__':
    app.debug = True
    app.run()