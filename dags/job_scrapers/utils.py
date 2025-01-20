import string
import random

import requests
from airflow.models import Variable


def newton_api_request(url: str, payload: dict = None, method='POST') -> requests.Response:
    api_base_url = Variable.get('API_BASE_URL')
    newton_api_auth_token = Variable.get('NEWTON_API_AUTH_TOKEN')

    webhook_url = f'{api_base_url}{url}'
    headers = {
        "Authorization": f"Bearer {newton_api_auth_token}",
        "Content-Type": "application/json"
    }
    response = requests.request(
            method,
            webhook_url,
            json=payload,
            headers=headers
    )
    return response


def generate_random_string(length):
    characters = string.ascii_letters + string.digits + string.punctuation
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string
