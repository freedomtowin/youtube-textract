from flask import Flask, jsonify, request
from flask_cors import CORS
import boto3 
import subprocess
import json
import os 
import time

from utils import secrets_util
from utils import youtube_util
from utils import file_util

# Currently this ENV variable is harded coded to `prd` in the Docker file
build_env = os.environ['BUILD_ENV']

s3_client = boto3.client('s3')

app = Flask(__name__)
CORS(app)

@app.route("/")
def hello_from_root():
    return jsonify(message='Hello from root!')

@app.route('/youtube_video_summary_pipeline', methods=['POST'])
def youtube_video_summary_pipeline():

	apikey = request.get_json()['apikey']
	vid = request.get_json()['video_id']
	
	secret_str = secrets_util.get_secret("/amplify/d22dk4zcipyt2f/main/frontend_apikey")
	secret = json.loads(secret_str)['frontend_apikey']
	
	if secret != apikey:
		raise Exception("Invalid API key")
		
	# Use awswrangler to create athena table in youtube_db
	
	# TODO: check if data exists before calling the API
	# Get and save video metadata
	video_metadata = youtube_util.get_video_metadata(vid)
	file_util.save_metadata_athena(video_metadata, table_name = 'video_metadata')
	
	# Get and save channel metadata
	cid = video_metadata['channel_id']
	channel_metadata = youtube_util.get_channel_metadata(cid)
	file_util.save_metadata_athena(channel_metadata, table_name = 'channel_metadata')

	audio_exists, audio_local = file_util.download_if_audio_exists_s3(vid, cid)

	print('audio_exists', audio_exists)

	# Download audio file with youtube_dl
	if audio_exists == False:
		print('downloading from S3')
		audio_local = youtube_util.download(vid, cid)
		# Output location in S3
		file_util.save_audio_s3(vid, cid)

	subprocess.run([
		"python",
		"models/whisper_transcriber.py",
		f"--video_id={vid}"
		])

	subprocess.run([
		"python",
		"models/paragrapher.py",
		f"--video_id={vid}",
    	f"--channel_id={cid}"
		])
		
	# subprocess.run([
	# 	"python",
	# 	"models/summarizer.py",
	# 	f"--video_id={vid}",
    # 	f"--channel_id={cid}"
	# 	])
	
	subprocess.run([
		"python",
		"models/doctran_extractor.py",
		f"--video_id={vid}",
    	f"--channel_id={cid}"
		])
		
	file_util.save_text_athena(vid, table_name = 'video_transcript')
	file_util.save_text_athena(vid, table_name = 'video_doctran')
	# file_util.save_text_athena(vid, table_name = 'video_summary')

	file_util.remove_instance_files(vid)
	
		
	return True

@app.route('/youtube_webscrape_all_video_metadata_for_channel', methods=['POST'])
def youtube_webscrape_all_video_metadata_for_channel():

	apikey = request.get_json()['apikey']
	cid = request.get_json()['channel_id']
	
	# Get and save channel metadata
	channel_metadata = youtube_util.get_channel_metadata(cid, use_cache=False)
	file_util.save_metadata_athena(channel_metadata, table_name = 'channel_metadata')

	vids = youtube_util.get_all_videos_in_channel(cid)

	for vid in vids:
		video_metadata = youtube_util.get_video_metadata(vid, use_cache=False)
		file_util.save_metadata_athena(video_metadata, table_name = 'video_metadata')
	


if __name__ == '__main__':
	
	# Currently this ENV variable is harded coded to `prd` in the Docker file
	if build_env == 'dev':
		app.debug=True
		portnum = 5000
	if build_env == 'prd':
		portnum = 80

	app.run(host='0.0.0.0',port=portnum)





