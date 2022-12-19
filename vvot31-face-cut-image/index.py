import boto3
import io
import PIL.Image as Image
import os
import json
from sanic import Sanic, Request, response
import ydb
import uuid
import requests

app = Sanic(__name__)

auth_url = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'
auth_header = {"Metadata-Flavor": "Google"}

face_bucket_name = 'itis-2022-2023-vvot31-faces'

FillDataQuery = """PRAGMA TablePathPrefix("{}");

DECLARE $ydb_facesData AS List<Struct<
    record_id: Utf8,
    face_bucket: Utf8,
    face_photo: Utf8,
    source_bucket: Utf8,
    source_photo: Utf8,
    face_name: Utf8>>;
    
REPLACE INTO ydb_faces
SELECT
    record_id,
    face_bucket,
    face_photo,
    source_bucket,
    source_photo,
    face_name
FROM AS_TABLE($ydb_facesData);
"""


class FaceRecord(object):
    __slots__ = ("record_id", "face_bucket", "face_photo", "source_bucket", "source_photo", "face_name")

    def __init__(self, record_id, face_bucket, face_photo, source_bucket, source_image_name, face_name):
        self.record_id = record_id
        self.face_bucket = face_bucket
        self.face_photo = face_photo
        self.source_bucket = source_bucket
        self.source_photo = source_image_name
        self.face_name = face_name


async def execute_query(records):
    r = requests.get(url=auth_url, headers=auth_header)
    a_token = r.json()['access_token']
    driver_config = ydb.DriverConfig(
        os.environ['YDB_ENDPOINT'], os.environ['YDB_DATABASE'],
        root_certificates=ydb.load_ydb_root_certificate(),
        auth_token=a_token
    )
    driver = ydb.aio.Driver(driver_config)
    pool = ydb.aio.SessionPool(driver, size=10)
    session = await pool.acquire()
    prepared_query = await session.prepare(FillDataQuery.format(os.environ['YDB_DATABASE']))
    await session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        {
            "$ydb_facesData": records
        },
        commit_tx=True
    )
    await pool.release(session)
    await pool.stop()
    await driver.stop()


@app.post("/")
async def main(request: Request):
    json_messages = json.loads(request.body.decode('utf-8'))
    records = []
    for message in json_messages['messages']:
        json_data = message['details']['message']['body']
        json_data = json.loads(json_data)
        s3session = boto3.session.Session(region_name='ru-central1')
        s3 = s3session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')
        faces = json_data['faceDetection']['faces']
        for face in faces:
            source_image = io.BytesIO()
            source_bucket = json_data['orig_bucket']
            source_image_name = json_data['orig_object']
            s3.download_fileobj(json_data['orig_bucket'], json_data['orig_object'], source_image)
            image = Image.open(source_image)

            upper_coord = face['boundingBox']['vertices'][0]
            lower_coord = face['boundingBox']['vertices'][2]
            face_crop = image.crop(
                (int(upper_coord['x']), int(upper_coord['y']), int(lower_coord['x']), int(lower_coord['y'])))
            id = str(uuid.uuid4())
            new_name = json_data['orig_object'].split('.')[0] + '-face-' + str(id) + '.jpg'
            img_byte_arr = io.BytesIO()
            face_crop.save(img_byte_arr, format='JPEG')
            s3.upload_fileobj(io.BytesIO(img_byte_arr.getvalue()), face_bucket_name, new_name)
            records.append(FaceRecord(id, face_bucket_name, new_name, source_bucket, source_image_name, ""))
    await execute_query(records)
    return response.json({"created": True}, status=200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ['PORT']), motd=False, access_log=False)
