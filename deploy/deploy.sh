#!/usr/bin/env bash
set -euo pipefail

# Required variables (override via environment before running)
PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
REGION="${REGION:-europe-west8}"
WORKER_POOL_NAME="${WORKER_POOL_NAME:-memorystore-keyspace-worker}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-memorystore-keyspace-worker}"
PUBSUB_TOPIC_ID="${PUBSUB_TOPIC_ID:-memorystore-keyspace-events}"
MEMORYSTORE_ADDR="${MEMORYSTORE_ADDR:?Set MEMORYSTORE_ADDR (e.g. 10.0.0.5:6379)}"
MEMORYSTORE_MODE="${MEMORYSTORE_MODE:-standalone}" # standalone | cluster
VPC_NETWORK="${VPC_NETWORK:-default}"
VPC_SUBNET="${VPC_SUBNET:-default}"

SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "1. Ensuring Pub/Sub topic exists: ${PUBSUB_TOPIC_ID}"
gcloud pubsub topics describe "${PUBSUB_TOPIC_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${PUBSUB_TOPIC_ID}" --project="${PROJECT_ID}"

echo "2. Ensuring dedicated Service Account exists: ${SA_EMAIL}"
gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --project="${PROJECT_ID}" \
    --display-name="Memorystore Keyspace to Pub/Sub Worker"

echo "3. Granting roles/pubsub.publisher on topic ${PUBSUB_TOPIC_ID}"
gcloud pubsub topics add-iam-policy-binding "${PUBSUB_TOPIC_ID}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher" >/dev/null

echo "4. Deploying Cloud Run Worker Pool (${WORKER_POOL_NAME}) with Direct VPC Egress"
gcloud run worker-pools deploy "${WORKER_POOL_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --source=. \
  --service-account="${SA_EMAIL}" \
  --network="${VPC_NETWORK}" \
  --subnet="${VPC_SUBNET}" \
  --vpc-egress=private-ranges-only \
  --instances=1 \
  --cpu=1 \
  --memory=512Mi \
  --set-env-vars="MEMORYSTORE_ADDR=${MEMORYSTORE_ADDR},MEMORYSTORE_MODE=${MEMORYSTORE_MODE},KEYSPACE_PATTERN=__keyevent@*__:*,PUBSUB_PROJECT_ID=${PROJECT_ID},PUBSUB_TOPIC_ID=${PUBSUB_TOPIC_ID}"

echo "Done. Worker pool ${WORKER_POOL_NAME} deployed in ${REGION}."
