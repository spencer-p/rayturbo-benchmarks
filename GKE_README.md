# RayTurbo benchmarks

### Quick start on GKE

You will need the following:
1. A GCP project
2. gcloud, kubectl
3. A python installation with Ray. See [Installing
   Ray](https://docs.ray.io/en/latest/ray-overview/installation.html). Please
   install the "machine learning applications" recommendation.

Reproduction steps:
1. Create your GKE cluster. Modify make-cluster.sh to use your preferred project
   or location if needed.
2. Ensure the ray cluster is applied. The script should create it for you.
   Verify it is running with `kubectl get rayclusters`.
3. Port forward the ray cluster.
   1. Install the [ray kubectl plugin](https://docs.ray.io/en/latest/ray-overview/installation.html).
   2. Run `kubectl ray session benchmark-cluster` in another terminal.
4. Submit the ray job:

```
export RAY_ADDRESS=http://localhost:8265
ray job submit --working-dir ./aggregation-filters -- python basic_aggregation.py --sf 100 --enable_hash_shuffle
```
