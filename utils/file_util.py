import os
import shutil
import pandas as pd
import requests
import awswrangler as wr
import boto3
from utils import athena_util

database_info = {
	"video_metadata": {"database": "youtube_db", "s3_data_loc": "s3://www.foolproof.world.data/video_metadata/", "partition_cols": ['channel_id', 'video_id']},
	"channel_metadata": {"database": "youtube_db", "s3_data_loc": "s3://www.foolproof.world.data/channel_metadata/", "partition_cols": ['channel_id']},
	"video_transcript": {"database": "youtube_db", "s3_data_loc": "s3://www.foolproof.world.data/video_transcript/", "partition_cols": ['channel_id', 'video_id']},
	"video_summary": {"database": "youtube_db", "s3_data_loc": "s3://www.foolproof.world.data/video_summary/", "partition_cols": ['channel_id', 'video_id']},
	"video_doctran": {"database": "youtube_db", "s3_data_loc": "s3://www.foolproof.world.data/video_doctran/", "partition_cols": ['channel_id', 'video_id']}
	}


def save_text_athena(vid, table_name):

	data_loc = f'/root/data/{table_name}/{vid}.csv'
	df = pd.read_csv(data_loc)

	table_info = database_info[table_name]
	s3_data_loc = table_info["s3_data_loc"]
	database_name = table_info["database"]
	partition_cols = table_info["partition_cols"]

	_ = wr.s3.to_parquet(
		df = df,
		path = s3_data_loc,
		index = False,
		dataset=True,
		database = database_name,
		table = table_name,
		partition_cols = partition_cols,
		mode = 'overwrite_partitions'

	)  # Athena/Glue table

def download_if_text_exists_s3(table_name, vid, cid, download=True):

    table_info = database_info[table_name]
    s3_data_loc = table_info["s3_data_loc"]
    database_name = table_info["database"]
    partition_cols = table_info["partition_cols"]
    # partition_path = '/'.join(partition_cols)

    local_file = f'/root/data/{table_name}/{vid}.csv'
    s3_file_key = f'{table_name}/channel_id={cid}/video_id={vid}'
    
    s3_client = boto3.client('s3')
    results = s3_client.list_objects(Bucket='www.foolproof.world.data', Prefix=s3_file_key)
    
    file_exists_s3 = 'Contents' in results
    file_exists_local = os.path.exists(local_file)
    
    if ~file_exists_local  and file_exists_s3:
        
        qry = f"""select * from {table_name} where video_id = '{vid}' and channel_id = '{cid}'"""
        df = wr.athena.read_sql_query(sql = qry, database = database_name, ctas_approach = True, unload_approach = False)
        df.to_csv(f'/root/data/{table_name}/{vid}.csv', index=False)

    return file_exists_s3, local_file


def save_metadata_athena(vm, table_name):

	table_info = database_info[table_name]

	s3_data_loc = table_info["s3_data_loc"]
	database_name = table_info["database"]
	partition_cols = table_info["partition_cols"]

	vm_df = pd.DataFrame(vm, index=[0])

	_ = wr.s3.to_parquet(
		df = vm_df,
		path = s3_data_loc,
		index = False,
		dataset=True,
		database = database_name,
		table = table_name,
		partition_cols = partition_cols,
		mode = 'overwrite_partitions'

	)  # Athena/Glue table

	
	
def save_audio_s3(vid, cid):

	bucket = 'www.foolproof.world.data'
	local_file = f'/root/audio/{vid}.wav'
	s3_file = f'audio/{cid}/{vid}.wav'

	s3 = boto3.client('s3')
	s3.upload_file(local_file,bucket,s3_file)
	
def download_if_audio_exists_s3(vid, cid, download=True):

	audio_key = f'audio/{cid}/{vid}.wav'
	audio_local = f'/root/audio/{vid}.wav'
	s3_client = boto3.client('s3')
	results = s3_client.list_objects(Bucket='www.foolproof.world.data', Prefix=audio_key)

	file_exists_s3 = 'Contents' in results
	file_exists_local = os.path.exists(audio_local)
	
	if ~file_exists_local  and file_exists_s3:
		s3_client.download_file('www.foolproof.world.data', audio_key, audio_local)

	return 'Contents' in results, audio_local
	
def get_audio_file_location_s3(vid):

	cid = athena_util.get_channel_id_for_video(vid)
	
	if cid is None:
		raise Exception("youtubedl failed or data was not saved to S3")
		
	bucket = 'www.foolproof.world.data'
	s3_file = f'audio/{cid}/{vid}.wav'
	
	return s3_file

def check_transcript_length(vid):
	transcript = f'/root/data/raw/{vid}.txt'

	with open(transcript, 'r') as fh:
		text = ''.join(fh.readlines())

	text = text.replace(' ', '')
	return len(text) > 100

def remove_instance_files(vid):
	print('removing files')
	audio_local = f'/root/audio/{vid}.wav'
	transcript = f'/root/data/raw/{vid}.txt'
	paragraphed = f'/root/data/video_transcript/{vid}.csv'
	summary = f'/root/data/video_summary/{vid}.csv'
	summary = f'/root/data/video_doctran/{vid}.csv'

	remove_files = [audio_local, transcript, paragraphed, summary]
	for f in remove_files:
		os.unlink(f)