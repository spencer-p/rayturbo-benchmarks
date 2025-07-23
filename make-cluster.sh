#!/bin/bash

# This configures a cluster based on
# https://www.anyscale.com/blog/rayturbo-data-improvements.
# max cpu is 16 for head, 5*64 for workers, round up to nearest hundred.
# max mem is 64+5*256, round up to nearest hundred.
gcloud container clusters create ray-bench-large \
  --addons=RayOperator \
  --enable-ray-cluster-logging \
  --enable-ray-cluster-monitoring \
  --enable-autoprovisioning \
  --max-cpu=400 \
  --max-memory=1400 \
  --machine-type c3d-standard-90 \
  --num-nodes 1

kubectl apply -f ray-cluster-bench-large.yaml
