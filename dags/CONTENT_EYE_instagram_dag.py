from datetime import datetime, timedelta

import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
}


def get_posts(instagram_username):
    now = datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    URL = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={instagram_username}&hl=en"

    response = requests.options(URL)
    cookies = response.cookies

    i = 0
    while i < 5:
        response = requests.get(
                URL,
                headers={
                    'x-ig-app-id': '936619743392459',
                },
                cookies=cookies
        )
        if response.status_code == 200:
            break
        i += 1

    if response.status_code != 200:
        return []

    data = response.json().get('data')
    if not data:
        return []
    user = data.get('user')
    if not user:
        return []
    profile_pic_url = user.get('profile_pic_url')
    edge_owner_to_timeline_media = user.get('edge_owner_to_timeline_media')
    if not edge_owner_to_timeline_media:
        return []
    edges = edge_owner_to_timeline_media.get('edges')
    if not edges:
        return []
    for edge in edges:
        node = edge.get('node')
        if not node:
            continue
        try:
            display_url = node.get('display_url')
            codename = node.get('shortcode')
            edge_media_to_caption = node.get('edge_media_to_caption')
            text = edge_media_to_caption['edges'][0]['node']['text']
            post_link = f"https://www.instagram.com/p/{codename}/"
            taken_at_timestamp = node['taken_at_timestamp']
            taken_at_datetime = datetime.fromtimestamp(taken_at_timestamp, pytz.UTC)
            if now - taken_at_datetime > timedelta(hours=200):
                continue
        except Exception as e:
            print(e)
            continue
        print("Duration", now - taken_at_datetime)
        print("Display URL:", display_url)
        print("Post Link:", post_link)
        print("Taken At:", taken_at_datetime)
        print("Text:", text)
        print('-' * 300)


def extract_recent_posts(**kwargs):
    instagram_pages_configuration = Variable.get(
            "CONTENT_EYE_INSTAGRAM_PAGES_TO_WATCH",
            deserialize_json=True,
            default_var=[]
    )
    for instagram_page_configuration in instagram_pages_configuration:
        page_username = instagram_page_configuration['username']
        slack_bot_name = instagram_page_configuration['slack_bot_name']
        slack_bot_icon = instagram_page_configuration['slack_bot_icon']

    return [{"hello": "world"}]


def post_on_slack(**kwargs):
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='extract_instagram_data')
    print(transform_data_output)


dag = DAG(
        'CONTENT_EYE_instagram_dag',
        default_args=default_args,
        description='A DAG to get the latest posts from the official Instagram accounts',
        schedule_interval='10 * * * *',
        catchup=False
)

extract_instagram_data = PythonOperator(
        task_id='extract_instagram_data',
        python_callable=extract_recent_posts,
        provide_context=True,
        dag=dag
)
post_on_slack = PythonOperator(
        task_id='post_on_slack',
        python_callable=post_on_slack,
        provide_context=True,
        dag=dag
)

extract_instagram_data >> post_on_slack
