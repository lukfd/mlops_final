# Copyright (c) 2026 Luca and Pam. All rights reserved.

import json
import os
import tempfile
from datetime import datetime, timezone

import boto3
import joblib

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT_URL", "http://localstack:4566")
BUCKET_NAME = os.getenv("S3_BUCKET", "lab4-model-training")
QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "inference-queue")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")


def _boto_kwargs() -> dict:
    return {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "region_name": AWS_REGION,
    }


def load_model(s3_client):
    """Resolve the promoted model version from S3 and return the loaded classifier."""
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key="models/cancer/promoted_model.json")
    promoted = json.loads(obj["Body"].read())
    version = promoted["version"]
    model_key = f"models/cancer/{version}/cancer.pkl"

    print(f"Loading model version {version} from s3://{BUCKET_NAME}/{model_key}")
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        s3_client.download_fileobj(BUCKET_NAME, model_key, tmp)
        tmp_path = tmp.name

    model = joblib.load(tmp_path)
    os.unlink(tmp_path)
    print("Model loaded successfully.")
    return model


def write_prediction(s3_client, record_id: str, prediction: int) -> None:
    result = {
        "record_id": record_id,
        "prediction": prediction,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    key = f"predictions/{record_id}.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(result).encode(),
        ContentType="application/json",
    )
    print(f"Written prediction for {record_id}: class={prediction}")


def main():
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, **_boto_kwargs())
    sqs = boto3.client("sqs", endpoint_url=SQS_ENDPOINT, **_boto_kwargs())

    model = load_model(s3)

    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    print(f"Polling queue: {queue_url}")

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
        )
        messages = response.get("Messages", [])

        if not messages:
            print("No messages, waiting...")
            continue

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                body = json.loads(msg["Body"])
                record_id = body["record_id"]
                features = [body["features"]]
                prediction = int(model.predict(features)[0])
                write_prediction(s3, record_id, prediction)
                # Delete only after successful write
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception as exc:
                print(f"Error processing message {msg.get('MessageId')}: {exc}")
                # Message remains in queue and becomes visible again after visibility timeout


if __name__ == "__main__":
    main()
