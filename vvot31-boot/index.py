import os
import requests

import ydb
import telebot


auth_url = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'
auth_header = {"Metadata-Flavor": "Google"}
bot_token = os.environ['BOT_TOKEN']
bot = telebot.TeleBot(bot_token)

chats_photo = {}
photo_url = os.environ['ALBUM_GATEWAY'] + '?face='

get_empty_face_query = """
        PRAGMA TablePathPrefix("{}");
        DECLARE $limit AS Uint32;
        DECLARE $face_name AS Utf8;
        SELECT * FROM ydb_faces
        WHERE face_name == $face_name
        LIMIT $limit;
        """.format(os.environ['YDB_DATABASE'])

get_all_face_query = """
        PRAGMA TablePathPrefix("{}");
        DECLARE $face_name AS Utf8;
        SELECT * FROM ydb_faces
        WHERE face_name == $face_name;
        """.format(os.environ['YDB_DATABASE'])

update_name_face_query = """
        PRAGMA TablePathPrefix("{}");
        DECLARE $record_id AS Utf8;
        DECLARE $face_name AS Utf8;
        UPDATE ydb_faces SET face_name = $face_name
        WHERE record_id == $record_id;
        """.format(os.environ['YDB_DATABASE'])


def get_record_with_empty_face_name():
    r = requests.get(url=auth_url, headers=auth_header)
    a_token = r.json()['access_token']
    driver_config = ydb.DriverConfig(os.environ['YDB_ENDPOINT'], 
        database=os.environ['YDB_DATABASE'],
        root_certificates=ydb.load_ydb_root_certificate(),
        auth_token=a_token
    )
    try:
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("Connect failed to YDB")

    try:
        session = driver.table_client.session().create()
        prepared_query = session.prepare(get_empty_face_query)
        results = session.transaction(ydb.SerializableReadWrite()).execute(prepared_query,
                                                                           {"$face_name": "", "$limit": 1},
                                                                           commit_tx=True)
        return results[0]
    finally:
        driver.stop()


def update_record(record_id, face_name):
    r = requests.get(url=auth_url, headers=auth_header)
    a_token = r.json()['access_token']
    driver_config = ydb.DriverConfig(os.environ['YDB_ENDPOINT'], 
        database=os.environ['YDB_DATABASE'],
        root_certificates=ydb.load_ydb_root_certificate(),
        auth_token=a_token
    )
    try:
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("Connect failed to YDB")

    try:
        session = driver.table_client.session().create()
        prepared_query = session.prepare(update_name_face_query)
        session.transaction(ydb.SerializableReadWrite()).execute(prepared_query,
                                                                           {"$face_name": face_name,
                                                                            "$record_id": record_id},
                                                                           commit_tx=True)
    finally:
        driver.stop()


def get_all_records(face_name):
    r = requests.get(url=auth_url, headers=auth_header)
    a_token = r.json()['access_token']
    driver_config = ydb.DriverConfig(os.environ['YDB_ENDPOINT'], 
        database=os.environ['YDB_DATABASE'],
        root_certificates=ydb.load_ydb_root_certificate(),
        auth_token=a_token
    )
    try:
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("Connect failed to YDB")

    try:
        session = driver.table_client.session().create()
        prepared_query = session.prepare(get_all_face_query)
        results = session.transaction(ydb.SerializableReadWrite()).execute(prepared_query,
                                                                           {"$face_name": face_name},
                                                                           commit_tx=True)
        return results[0]
    finally:
        driver.stop()


@bot.message_handler(commands=['getface'])
def get_face(message):
    record = get_record_with_empty_face_name().rows[0]
    bot.send_message(message.chat.id, 'Try to send image with name' + str(record.face_photo))
    chats_photo[message.chat.id] = record.record_id
    photo = requests.get(photo_url + record.face_photo).content
    bot.send_photo(message.chat.id, photo)


@bot.message_handler(commands=['find'])
def find(message):
    print(message.text)
    face_name = message.text.split(' ')[1]
    result = get_all_records(face_name)
    for record in result.rows:
        photo = requests.get(photo_url + record.face_photo).content
        bot.send_photo(message.chat.id, photo)


@bot.message_handler(func=lambda message: True)
def save_name(message):
    if message.chat.id not in chats_photo:
        bot.send_message(message.chat.id, 'Unknown command')
    face_name = message.text
    record_id = chats_photo[message.chat.id]
    update_record(record_id, face_name)
    bot.send_message(message.chat.id, 'Successful save new name')


def handler(event, context):
    message = telebot.types.Update.de_json(event['body'])
    bot.process_new_updates([message])
    return {
        'statusCode': 200,
        'body': "!",
    }
