import boto3
import io
import base64
import requests
import json


def get_data(event, s3):
    bucket_id = event['messages'][0]['details']['bucket_id']
    object_id = event['messages'][0]['details']['object_id']
    image = io.BytesIO()
    s3.download_fileobj(bucket_id, object_id, image)
    content_data = base64.b64encode(image.getvalue()).decode('utf-8')
    return {
        "folderId": "b1gc07q0mvfv1hom4tu4",
        "analyze_specs": [{
            "content": content_data,
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }


def handler(event, context):
    session = boto3.session.Session(region_name='ru-central1')
    s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

    token = context.token['access_token']
    headers = {'Authorization': 'Bearer ' + token}
    stt_url = 'https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze'
    json_data = get_data(event, s3)

    r = requests.post(
        url=stt_url,
        headers=headers,
        json=json_data)
    object_id = event['messages'][0]['details']['object_id']
    bucket_id = event['messages'][0]['details']['bucket_id']

    response_json = r.json()['results'][0]['results'][0]
    response_json['orig_object'] = object_id
    response_json['orig_bucket'] = bucket_id
    
    client_queue = boto3.client(service_name='sqs', endpoint_url='https://message-queue.api.cloud.yandex.net', region_name='ru-central1')
    queue_url = client_queue.get_queue_url(QueueName='vvot31-tasks').get('QueueUrl')
    client_queue.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(response_json)
    )
    return {
        'statusCode': 200,
        'body': ''
    }