# Copyright (c) 2026 Luca and Pam. All rights reserved.

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="sqs_populate_inference_queue",
    default_args=default_args,
    description="Send breast cancer test records to SQS for async inference",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task(task_id="send_records_to_sqs")
    def send_records_to_sqs() -> int:
        from sklearn.datasets import load_breast_cancer
        from sklearn.model_selection import train_test_split

        from utils.sqs import SQS

        data = load_breast_cancer()
        X, y = data.data, data.target

        # Use the same split as CancerModel: split_ratio=0.8, random_state=42
        _, X_test, _, _ = train_test_split(X, y, test_size=0.2, random_state=42)

        sqs = SQS()
        for i, features in enumerate(X_test):
            record_id = f"sample_{i:04d}"
            sqs.send_message({"record_id": record_id, "features": features.tolist()})

        print(f"Sent {len(X_test)} records to SQS queue '{sqs.queue_name}'.")
        return len(X_test)

    send_records_to_sqs()
