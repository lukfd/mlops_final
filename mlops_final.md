
# Final Project: Building an Asynchronous AI Inference System
## Overview
In this final project, you will design and build a simple distributed machine learning system that combines:
- orchestration (Airflow)
- object storage (S3)
- message queues (SQS)
- scalable compute (Kubernetes)

Your system will train a model, generate inference jobs, process them asynchronously, and store results.

This project emphasizes system design and integration, not model complexity.

## System Architecture
You will build an asynchronous AI pipeline with the following flow:
### Training Flow
1. Airflow loads the breast cancer dataset
2. Splits into train/test
3. Trains a simple sklearn model
4. Saves the trained model to S3

### Inference Flow
1. Airflow reads the test dataset
2. Sends one message per record to SQS
3. Kubernetes consumers read from the queue
4. Consumers load the trained model from S3
5. Consumers perform inference
6. Consumers write predictions back to S3

## Requirements
### 1. Airflow Training DAG

Create a DAG that:
- loads the breast cancer dataset
- splits into train and test sets
- trains a simple model (e.g., logistic regression, decision tree)
- serializes the model (e.g., joblib)
- writes the model to S3

Output:
- model.pkl stored in S3

### 2. Airflow Queue Population
Create a task or DAG that:
- reads the test dataset
- sends one message per record to SQS

Each message must include:
`{
"record_id": "sample_001",
"features": [...]
}`

### 3. Kubernetes Consumer
Create a containerized application that:
- polls SQS for messages
- loads the trained model from S3 (on startup)
- performs inference
- writes predictions to S3
- deletes messages only after successful processing

### 4. Output Requirements
Each prediction must be written to S3 as a separate file.

Example:
`{
 "record_id": "sample_001",
 "prediction": 1,
"timestamp": "2026-04-15T12:00:00Z"
}`

### Important:
Write each prediction to a unique file (e.g., predictions/sample_001.json) to avoid conflicts.

### 5. Kubernetes Deployment
Deploy your consumer using a Kubernetes Deployment.

You must:
- run at least 1 replica
- demonstrate the ability to scale (e.g., increase replicas)

## Deliverables
Submit the following:
### Code
- Airflow DAG file(s)
- consumer application code
- Dockerfile
- Kubernetes deployment YAML

### Documentation
- README with setup and run instructions

### Writeup (1–2 pages)

Answer the following:
1. Describe your system end-to-end
2. Why is a queue used instead of direct API calls?
3. What happens if a consumer crashes mid-processing?
4. Where are the bottlenecks in your system?
5. One improvement you would make for production
