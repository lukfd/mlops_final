# Copyright (c) 2026 Luca and Pam. All rights reserved.

import json
import os

import boto3
from botocore.exceptions import ClientError

QUEUE_NAME = "inference-queue"


class SQS:
    def __init__(
        self,
        queue_name: str = QUEUE_NAME,
        endpoint_url: str | None = None,
    ):
        self.queue_name = queue_name
        self.endpoint_url = endpoint_url or os.getenv("SQS_ENDPOINT_URL", "http://localstack:4566")
        self.client = boto3.client(
            "sqs",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        self.queue_url = self._ensure_queue()

    def _ensure_queue(self) -> str:
        try:
            resp = self.client.get_queue_url(QueueName=self.queue_name)
            return resp["QueueUrl"]
        except ClientError:
            resp = self.client.create_queue(QueueName=self.queue_name)
            return resp["QueueUrl"]

    def send_message(self, body: dict) -> None:
        self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(body),
        )

    def receive_messages(self, max_messages: int = 10, wait_seconds: int = 10) -> list:
        resp = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
        )
        return resp.get("Messages", [])

    def delete_message(self, receipt_handle: str) -> None:
        self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
