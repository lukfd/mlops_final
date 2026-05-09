# ML Inference Pipeline

*By Luca and Pam*

## Requirements
- Docker Desktop (8+ GB RAM recommended)
- Docker Compose
- Make

## Quick Start

```bash
chmod +x scripts/localstack_init.sh
docker compose up -d --build
make kind-get-nodes
```

## Train the Model

- Trigger the `ml_training_pipeline_v2` DAG in the Airflow UI.

## Build and Deploy the Consumer

```bash
make kind-build-consumer
make kind-load-consumer
make kind-apply-consumer
```

Optional scale:

```bash
make kind-scale-consumer
```

## Populate the Queue

- Trigger the `sqs_populate_inference_queue` DAG in the Airflow UI.

## API

Use the `GET /predictions` endpoint to get the latest results or check the S3 bucket.
