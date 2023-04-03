from __future__ import annotations

import os
import uuid
from typing import Any

from airflow.models.xcom import BaseXCom, XCom
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
		filename = f"{key}.pickle"
		value.to_pickle(filename, index=False)
		s3_hook.load_file(
			bucket_name="airflow_dags",
			key=f"{S3CustomBackendXCom.S3_PREFIX}/{key}",
			filename=filename
		)

		return BaseXCom.serialize_value(key)

	@staticmethod
	def deserialize_value(result: XCom) -> Any:
		import pickle
		result = BaseXCom.deserialize_value(result)
		s3_hook = S3Hook(aws_conn_id="s3_aws_credentials")
		key = f"{S3CustomBackendXCom.S3_PREFIX}/{result}"
		filename = s3_hook.download_file(
				key=key,
				bucket_name=S3CustomBackendXCom.S3_BUCKET_NAME,
		)
		with open(filename, 'r') as f:
			key_data = pickle.load(f)

		os.remove(filename)

		return key_data


