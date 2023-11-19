import time
import boto3
import io

from boto3.dynamodb.conditions import Key, Attr

class DynamoDBHandler:
    def __init__(self, table_name):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def read_item(self, key):
        response = self.table.get_item(Key=key)
        item = response.get('Item')
        return item

    def write_item(self, item):
        self.table.put_item(Item=item)

    def update_item(self, key, update_expression, expression_attribute_values):
        self.table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def delete_item(self, key):
        self.table.delete_item(Key=key)

    def query_items(self, key_condition_expression, expression_attribute_values=None, filter_expression=None,
                    projection_expression=None, limit=None):
        params = {
            'KeyConditionExpression': key_condition_expression,
        }

        if expression_attribute_values is not None:
            params['ExpressionAttributeValues'] = expression_attribute_values

        if filter_expression is not None:
            params['FilterExpression'] = filter_expression

        if projection_expression is not None:
            params['ProjectionExpression'] = projection_expression

        if limit is not None:
            params['Limit'] = limit

        response = self.table.query(**params)
        items = response.get('Items')
        return items

def write_to_dynamodb(json_data, table_name ):
    dynamodb_handler = DynamoDBHandler(table_name)
    dynamodb_handler.write_item(json_data)

def get_channel_id_for_video():
    vid = 'wafi94q8gPg'

    dynamodb_handler = DynamoDBHandler('youtube_video_metadata')

    # Define query parameters
    key_condition_expression = Key('video_id').eq(vid)
    projection_expression = 'video_id, channel_id'
    limit = 1

    # Perform the query
    items = dynamodb_handler.query_items(
        key_condition_expression=key_condition_expression,
        projection_expression=projection_expression,
        limit=limit
    )
        
    item = items[0]

    return item['channel_id']
    
		
