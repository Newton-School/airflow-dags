from __future__ import annotations

import os
import pickle
import uuid
from typing import Any

from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3CustomBackendXCom(BaseXCom):
	S3_BUCKET_NAME = "newton-airflow-dags-temp"
	S3_PREFIX = "dags-temp/"

	@staticmethod
	def serialize_value(
			value: Any,
			*,
			key: str | None = None,
			task_id: str | None = None,
			dag_id: str | None = None,
			run_id: str | None = None,
			map_index: int | None = None,
	) -> Any:
		s3_hook = S3Hook(aws_conn_id="s3_aws_credentials")
		key = f"{str(uuid.uuid4())}.pickle"
		filename = key
		with open(key, 'wb') as b:
			pickle.dump(value, b)

		s3_hook.load_file(
			bucket_name=S3CustomBackendXCom.S3_BUCKET_NAME,
			key=f"{S3CustomBackendXCom.S3_PREFIX}{key}",
			filename=filename
		)

		return BaseXCom.serialize_value(key)

	@staticmethod
	def deserialize_value(result) -> Any:
		import pickle
		result = BaseXCom.deserialize_value(result)
		s3_hook = S3Hook(aws_conn_id="s3_aws_credentials")
		key = f"{S3CustomBackendXCom.S3_PREFIX}{result}"
		filename = s3_hook.download_file(
				key=key,
				bucket_name=S3CustomBackendXCom.S3_BUCKET_NAME,
		)
		with open(filename, 'rb') as f:
			key_data = pickle.load(f)

		os.remove(filename)

		return key_data


