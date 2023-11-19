import time
import boto3
import io
import csv

class QueryAthena:

    def __init__(self, query, database):
        self.database = database
        self.folder = 'athena_queries/'
        self.bucket = 'www.foolproof.world.athena'
        self.s3_output =  's3://' + self.bucket + '/' + self.folder
        self.region_name = 'us-east-1'
        self.query = query

    def load_conf(self, q):
        try:
            self.client = boto3.client('athena', 
                              region_name = self.region_name)
                              
            response = self.client.start_query_execution(
                QueryString = q,
                    QueryExecutionContext={
                    'Database': self.database
                    },
                    ResultConfiguration={
                    'OutputLocation': self.s3_output,
                    }
            )
            
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + response['QueryExecutionId'])

        except Exception as e:
            print(e)
            response = e
            
        return response                

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:              
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                response = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']
                query_status = response['Status']['State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    print(response['Status']['StateChangeReason'])
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(0.5)
            print('Query "{}" finished.'.format(self.query))

            df = self.obtain_data()
            return df

        except Exception as e:
            print(e)      

    def obtain_data(self):
        try:
            s3_resource = boto3.resource('s3', 
                                  region_name = self.region_name)
                                  
            s3_obj = s3_resource.Object(self.bucket, self.folder + self.filename + '.csv')
            
            data = s3_obj.get()['Body'].read()
            
            reader = csv.DictReader(data.decode('utf-8').split('\n'), delimiter=',')

            result = []
            for row in reader:
                result.append(row)
                
            return result   
        except Exception as e:
            print(e)  


def get_channel_metadata(cid):

	qry = f"""select * from channel_metadata where channel_id = '{cid}'"""

	qa = QueryAthena(query=qry, database='youtube_db')

	data = qa.run_query()

	if len(data) == 0:
		return None
		
	data = data[0]
	
	return data

def get_video_metadata(vid):

	qry = f"""select * from video_metadata where video_id = '{vid}'"""

	qa = QueryAthena(query=qry, database='youtube_db')

	data = qa.run_query()

	if len(data) == 0:
		return None
		
	data = data[0]
	
	return data

def get_channel_id_for_video(vid):

	qry = f"""select distinct channel_id from video_metadata where video_id = '{vid}'"""

	qa = QueryAthena(query=qry, database='youtube_db')

	data = qa.run_query()

	if len(data) == 0:
		return None
		
	data = data[0]
	
	return data['channel_id']

def get_video_ids_for_channel(cid):

    qry = f"""select distinct video_id from video_metadata where channel_id = '{cid}'"""

    qa = QueryAthena(query=qry, database='youtube_db')

    data = qa.run_query()


    if len(data) == 0:
        return None

    vids = [x['video_id'] for x in data]
    
    return vids
		
def get_top_videos(cid):
    qry = f"""
    with 
    video_channel_sub as (
    select * from video_metadata where channel_id = {cid}
    ),
    avg_view_count as (
        select avg("video_view_count") + stddev("video_view_count")  avg_view_count from video_channel_sub
    ),
    avg_like_count as (
        select avg("video_like_count") + stddev("video_like_count")  avg_like_count from video_channel_sub
    ),
    avg_comment_count as (
        select avg("video_comment_count") + stddev("video_comment_count") avg_comment_count from video_channel_sub
    )
    select video_id from video_channel_sub, avg_view_count, avg_comment_count, avg_like_count
    where video_comment_count >= avg_comment_count
    and video_like_count >= avg_like_count
    and video_view_count >= avg_view_count
    """
    
    qa = QueryAthena(query=qry, database='youtube_db')

    data = qa.run_query()


    if len(data) == 0:
        return None

    vids = [x['video_id'] for x in data]

    return vids
