#!/usr/bin/env bash
set -euo pipefail

awslocal s3 mb s3://lab4-model-training || true
awslocal sqs create-queue --queue-name inference-queue || true
