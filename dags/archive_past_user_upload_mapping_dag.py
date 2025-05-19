import json
import re

import pendulum
from dataclasses import dataclass, asdict
from typing import List, Union

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.exceptions import ClientError
from pendulum import datetime

POSTGRES_CONNECTION_ID = 'postgres_read_replica'
BATCH_SIZE = 10000


@dataclass
class UserUploadMapping:
    id: int
    hash: str
    type: int
    content_type: str
    object_id: str
    device_type: int
    created_at: datetime
    user_upload_id: int
    user_upload_hash: str
    user_id: int
    upload: str
    name: str
    user_upload_created_at: datetime


def get_s3_resource():
    return boto3.resource(
            's3',
            aws_access_key_id=Variable.get('USER_UPLOAD_ARCHIVE_AWS_S3_ACCESS_KEY'),
            aws_secret_access_key=Variable.get('USER_UPLOAD_ARCHIVE_AWS_S3_SECRET_KEY'),
    )


def get_s3_bucket():
    return get_s3_resource().Bucket(Variable.get('USER_UPLOAD_ARCHIVE_AWS_S3_BUCKET_NAME'))


def process_user_upload_mapping(user_upload_mapping_row: List[Union[str, int]]) -> UserUploadMapping:
    return UserUploadMapping(
            id=user_upload_mapping_row[0],
            hash=user_upload_mapping_row[1],
            type=user_upload_mapping_row[2],
            content_type=user_upload_mapping_row[3],
            object_id=user_upload_mapping_row[4],
            device_type=user_upload_mapping_row[5],
            created_at=user_upload_mapping_row[6],
            user_upload_id=user_upload_mapping_row[7],
            user_upload_hash=user_upload_mapping_row[8],
            user_id=user_upload_mapping_row[9],
            upload=user_upload_mapping_row[10],
            name=user_upload_mapping_row[11],
            user_upload_created_at=user_upload_mapping_row[12],
    )


def archive_user_upload_mappings(archive_from: datetime, archive_till: datetime, s3_bucket):
    def _get_entity_key(user_upload_mapping: UserUploadMapping):
        return f'{user_upload_mapping.content_type}_{user_upload_mapping.object_id}'

    def _archive_grouped_user_upload_mappings(user_upload_mappings: List[UserUploadMapping]):
        user_upload_mapping = user_upload_mappings[0]
        user_upload_mapping_created_at = user_upload_mapping.user_upload_created_at
        object_id = user_upload_mapping.object_id
        content_type = user_upload_mapping.content_type
        archive_file_prefix = (f'data/year={user_upload_mapping_created_at.year}/month={user_upload_mapping_created_at.month}/'
                               f'day={user_upload_mapping_created_at.day}')
        json_data = json.dumps(
                {'user_upload_mappings': [asdict(user_upload_mapping) for user_upload_mapping in user_upload_mappings]}, indent=4,
                default=str
        )
        s3_bucket.put_object(
                Key=f'{archive_file_prefix}/content_type={content_type}/{object_id}.json',
                Body=json_data,
                ContentType='application/json'
        )

    retrieve_query = (f"SELECT uum.id AS id, uum.hash AS hash, uum.type AS type, uum.content_type_id AS content_type, "
                      f"uum.object_id AS object_id, uum.device_type AS device_type, uum.created_at AS created_at, "
                      f"uu.id AS user_upload_id, uu.hash AS user_upload_hash, uu.user_id AS user_id, uu.upload AS upload, "
                      f"uu.name AS name, uu.created_at AS user_upload_created_at "
                      f"FROM uploads_useruploadmapping uum JOIN "
                      f"uploads_userupload uu ON uum.user_upload_id = uu.id WHERE uu.created_at >= '{archive_from}' AND uu.created_at < '"
                      f"{archive_till}' ORDER BY uum.content_type_id, uum.object_id")

    entity_user_upload_mapping = {}
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(retrieve_query)
    rows = cursor.fetchmany(BATCH_SIZE)

    while rows:
        for row in rows:
            user_upload_mapping = process_user_upload_mapping(row)
            key = _get_entity_key(user_upload_mapping)
            entity_user_upload_mapping.setdefault(key, []).append(user_upload_mapping)
        for entity_key in list(entity_user_upload_mapping.keys())[:-1]:
            user_upload_mappings = entity_user_upload_mapping.pop(entity_key)
            _archive_grouped_user_upload_mappings(user_upload_mappings)
            rows = cursor.fetchmany(BATCH_SIZE)

    for entity_key in entity_user_upload_mapping:
        user_upload_mappings = entity_user_upload_mapping[entity_key]
        _archive_grouped_user_upload_mappings(user_upload_mappings)

    cursor.close()
    connection.close()


@dag(
        dag_id='archive_past_user_upload_mapping_dag',
        schedule_interval='30 22 * * *',
        start_date=datetime(2022, 1, 30),
        catchup=False,
)
def archive_past_user_upload_mapping_dag():
    @task()
    def archive_user_upload_mappings_task():
        s3_bucket = get_s3_bucket()
        archive_date_to = pendulum.today().subtract(days=int(Variable.get('USER_UPLOAD_MAPPING_ARCHIVE_DAYS_THRESHOLD')))
        archive_date_from = archive_date_to.subtract(days=int(Variable.get('USER_UPLOAD_MAPPING_ARCHIVE_DAYS_RANGE')))
        archive_user_upload_mappings(archive_date_from, archive_date_to, s3_bucket)

    archive_user_upload_mappings_task()


@dag(
    dag_id='migrate_user_upload_mapping_path_dag',
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 19, tz='UTC'),
    catchup=False,
)
def migrate_user_upload_mapping_path_dag():
    @task()
    def migrate_paths():
        bucket = get_s3_bucket()
        pattern = re.compile(
            r'data/(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<content_type>\d+)/(?P<filename>.+\.json)$'
        )
        for obj in bucket.objects.all():
            key = obj.key.lstrip('/')
            m = pattern.match(key)
            if not m:
                continue
            parts = m.groupdict()
            new_key = (
                f"data/year={parts['year']}/month={int(parts['month']):02}/"
                f"day={int(parts['day']):02}/content_type={parts['content_type']}/"
                f"{parts['filename']}"
            )
            # Copy and delete original
            bucket.Object(new_key).copy_from(CopySource={'Bucket': bucket.name, 'Key': key})
            # bucket.Object(key).delete()

    migrate_paths()


@dag(
    dag_id='reformat_user_upload_mapping_json_dag',
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 19, tz='UTC'),
    catchup=False,
)
def reformat_user_upload_mapping_json_dag():
    @task()
    def reformat_files():
        bucket = get_s3_bucket()
        pattern = re.compile(
            r'data/year=\d{4}/month=\d{2}/day=\d{2}/content_type=\d+/\d+\.json$'
        )

        for obj in bucket.objects.filter(Prefix='data/'):
            key = obj.key

            if not pattern.match(key):
                continue

            try:
                s3_obj = bucket.Object(key)
                body = s3_obj.get()['Body'].read().decode('utf-8')

                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError as e:
                    print(f"❌ Skipping malformed JSON in {key}: {e}")
                    continue

                compact_json = json.dumps(parsed, separators=(',', ':'), default=str)

                s3_obj.put(
                    Body=compact_json.encode('utf-8'),
                    ContentType='application/json'
                )
                print(f"✅ Rewritten: {key}")

            except ClientError as e:
                print(f"❌ Failed to process {key}: {e}")

    reformat_files()


archive_past_user_upload_mapping_dag = archive_past_user_upload_mapping_dag()
migrate_user_upload_mapping_path_dag = migrate_user_upload_mapping_path_dag()
reformat_user_upload_mapping_json_dag = reformat_user_upload_mapping_json_dag()
