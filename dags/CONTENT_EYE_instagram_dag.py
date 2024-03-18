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

SLACK_CHANNEL = Variable.get("CONTENT_EYE_SLACK_CHANNEL", default_var="#social-media-eye")


def send_to_slack(message, channel, icon_url=None, bot_name=None):
    token = Variable.get("NEWTON_SCHOOL_HQ_SLACK_BOT_TOKEN")
    if not token:
        raise ValueError("SLACK_BOT_TOKEN is not set")

    data = {"text": message, "channel": channel, "token": token}
    if icon_url:
        data['icon_url'] = icon_url
    if bot_name:
        data['username'] = bot_name
    requests.post(f"https://slack.com/api/chat.postMessage", data=data)


def get_posts(instagram_username):
    print("Getting posts for", instagram_username)
    now = datetime.now()
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
        return [], None, "Failed to fetch data from Instagram. Please check the username."

    data = response.json().get('data')
    if not data:
        return [], None, "Failed to fetch data from Instagram. Please check the username."
    user = data.get('user')
    if not user:
        return [], None, "Failed to fetch data from Instagram. Please check the username."
    profile_pic_url = user.get('profile_pic_url')
    edge_owner_to_timeline_media = user.get('edge_owner_to_timeline_media')
    if not edge_owner_to_timeline_media:
        return [], None, "Failed to fetch data from Instagram. Please check the username."
    edges = edge_owner_to_timeline_media.get('edges')
    if not edges:
        return [], None, "Failed to fetch data from Instagram. Please check the username."
    records = []
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
            taken_at_datetime = datetime.fromtimestamp(taken_at_timestamp)
            if now - taken_at_datetime > timedelta(hours=4):
                continue
        except Exception as e:
            print(e)
            continue
        records.append(
                {
                    "display_url": display_url,
                    "post_link": post_link,
                    "taken_at_datetime": taken_at_datetime,
                    "text": text
                }
        )
        return profile_pic_url, records, None


def extract_recent_posts(**kwargs):
    instagram_pages_configuration = Variable.get(
            "CONTENT_EYE_INSTAGRAM_PAGES_TO_WATCH",
            deserialize_json=True,
            default_var=[]
    )
    print(instagram_pages_configuration)
    slack_messages = []
    for instagram_page_configuration in instagram_pages_configuration:
        page_username = instagram_page_configuration['username']
        slack_bot_name = f"{instagram_page_configuration['slack_bot_name']} - INSTAGRAM"
        profile_pic_url, records, error = get_posts(page_username)
        if error:
            print("Errored", error)
            continue
        slack_messages.extend(
                [
                    {'icon_url': profile_pic_url, 'bot_name': slack_bot_name, **record} for record in records
                ]
        )
    return slack_messages


def post_on_slack(**kwargs):
    ti = kwargs['ti']
    slack_messages = ti.xcom_pull(task_ids='extract_instagram_data')
    for slack_message in slack_messages:
        message = f"*{slack_message['bot_name']}* has a new post on Instagram\n" \
                    f"{slack_message['text']} \n" \
                  f"{slack_message['post_link']}\n" \
                  f"[Display]({slack_message['display_url']})"
        send_to_slack(
                message,
                SLACK_CHANNEL,
                slack_message['icon_url'],
                slack_message['bot_name']
        )


dag = DAG(
        'CONTENT_EYE_instagram_dag',
        default_args=default_args,
        description='A DAG to get the latest posts from the official Instagram accounts',
        schedule_interval='10 */4 * * *',
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
