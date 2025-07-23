#!/bin/bash

# This configures a cluster based on
# https://www.anyscale.com/blog/rayturbo-data-improvements.
gcloud container clusters create ray-bench-large \
  --addons=RayOperator
  --enable-ray-cluster-logging \
  --enable-ray-cluster-monitoring \
  --region=us-central1 \
  --enable-autoprovisioning \
  --max-cpu=400 \  # 16 for head, 5*64 for workers, round up to nearest hundred.
  --max-memory=1400 \  # 64+5*256, round up to nearest hundred.
  --machine-type c3 \
  --num-nodes 1

kubectl apply -f ray-cluster-bench-large.yaml
