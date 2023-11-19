import boto3
import datetime
import yt_dlp as youtube_dl
from utils import secrets_util
from utils import file_util
from utils import athena_util
import requests
import urllib
import json
import os
import re

apikey_json = secrets_util.get_secret("/amplify/d22dk4zcipyt2f/main/youtube_apikey")
apikey_json = json.loads(apikey_json)

apikey = apikey_json["youtube_apikey"]

def yt_time(duration="P1W2DT6H21M32S"):
    """
    Converts YouTube duration (ISO 8061)
    into Seconds

    see http://en.wikipedia.org/wiki/ISO_8601#Durations
    """
    ISO_8601 = re.compile(
        'P'   # designates a period
        '(?:(?P<years>\d+)Y)?'   # years
        '(?:(?P<months>\d+)M)?'  # months
        '(?:(?P<weeks>\d+)W)?'   # weeks
        '(?:(?P<days>\d+)D)?'    # days
        '(?:T' # time part must begin with a T
        '(?:(?P<hours>\d+)H)?'   # hours
        '(?:(?P<minutes>\d+)M)?' # minutes
        '(?:(?P<seconds>\d+)S)?' # seconds
        ')?')   # end of time part
    # Convert regex matches into a short list of time units
    units = list(ISO_8601.match(duration).groups()[-3:])
    # Put list in ascending order & remove 'None' types
    units = list(reversed([int(x) if x != None else 0 for x in units]))
    # Do the maths
    return sum([x*60**units.index(x) for x in units])


def get_channel_metadata(cid, use_cache=True):

    if use_cache == True:
        cached = athena_util.get_channel_metadata(cid)
        if cached is not None:
            return cached

    base_search_url = 'https://www.googleapis.com/youtube/v3/channels?'
    url = f'{base_search_url}key={apikey}&id={cid}&part=id,snippet,contentDetails,statistics,brandingSettings,topicDetails'
    
    result = requests.get(url)
    
    metadata = json.loads(result.content)

    channel_title = metadata['items'][0]['snippet']['title']
    channel_description = metadata['items'][0]['snippet']['description']
    channel_keywords = metadata['items'][0]['brandingSettings']['channel']['keywords']

    view_count = metadata['items'][0]['statistics']['viewCount']
    subscriber_count = metadata['items'][0]['statistics']['subscriberCount']
    video_count = metadata['items'][0]['statistics']['videoCount']
    topic_categories = metadata['items'][0]['topicDetails']

    if 'topicCategories' in topic_categories.keys():

        topic_categories = ', '.join(topic_categories['topicCategories'])



    result = {
    "channel_id": cid,
    "channel_title": channel_title,
    "channel_description": channel_description,
    "channel_keywords": channel_keywords,
    "channel_view_count": int(view_count),
    "channel_subscriber_count": int(subscriber_count),
    "channel_video_count": int(video_count),
    "channel_topic_categories": topic_categories}
    
    return result

def get_video_metadata(vid, use_cache=True):

    if use_cache == True:
        cached = athena_util.get_video_metadata(vid)
        if cached is not None:
            return cached


    result = requests.get(f'https://www.googleapis.com/youtube/v3/videos?part=id,contentDetails,statistics%2C+snippet&id={vid}&key={apikey}')
    metadata = json.loads(result.content)

    if metadata['items'][0]['kind'] != "youtube#video":
        raise Exception("Not a Youtube video!")

    etag = metadata['items'][0]['etag']
    vid = metadata['items'][0]['id']
    cid = metadata['items'][0]['snippet']['channelId']
    title = metadata['items'][0]['snippet']['title']
    description = metadata['items'][0]['snippet']['description']


    pubtime = metadata['items'][0]['snippet']['publishedAt']
    ctitle = metadata['items'][0]['snippet']['channelTitle']

    tags = ""
    if 'tags' in  metadata['items'][0]['snippet']:
        tags = metadata['items'][0]['snippet']['tags']
        tags = ', '.join(tags)

    category = metadata['items'][0]['snippet']['categoryId']
    thumbnails = metadata['items'][0]['snippet']['thumbnails']

    view_count = metadata['items'][0]['statistics']['viewCount']
    like_count = metadata['items'][0]['statistics']['likeCount']
    comment_count = metadata['items'][0]['statistics']['commentCount']

    duration = yt_time(metadata['items'][0]['contentDetails']['duration'])

    result = {
        "channel_id": cid,
        "channel_title": ctitle,
        "video_id": vid,
        "video_title": title,
        "video_description": description,
        "video_duration": duration,
        "video_tags": tags,
        "video_category": category,
        "video_view_count": int(view_count),
        "video_like_count": int(like_count),
        "video_comment_count": int(comment_count),
        "video_pubish_time": pubtime}

    return result

def get_all_videos_in_channel(channel_id):

    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'

    first_url = base_search_url+'key={}&channelId={}&part=snippet,id&order=date&maxResults=25'.format(apikey, channel_id)
    
    video_ids = []
    url = first_url
    while True:
        inp = requests.get(url).content
        resp = json.loads(inp)

        for i in resp['items']:
            if i['id']['kind'] == "youtube#video":
                video_ids.append(i['id']['videoId'])

        try:
            next_page_token = resp['nextPageToken']
            url = first_url + '&pageToken={}'.format(next_page_token)
        except:
            break
    return video_ids

class MyLogger(object):
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        print(msg)


def my_hook(d):
    if d['status'] == 'finished':
        print('Done downloading, now converting ...')

def download(vid, cid):

	output_file = f'/root/audio/{vid}'
	
	ydl_opts = {
		'format': 'bestaudio/best',
		'postprocessors': [{
			'key': 'FFmpegExtractAudio',
			'preferredcodec': 'wav',
			'preferredquality': '192',
		}],
		'logger': MyLogger(),
		'progress_hooks': [my_hook],
		'outtmpl': output_file,
		'postprocessor_args': [
			'-ar', '16000'
		],
	}
	with youtube_dl.YoutubeDL(ydl_opts) as ydl:
		ydl.download([f'https://www.youtube.com/watch?v={vid}'])
		
	return output_file



	