# #!/bin/bash
# set -e

# echo "Cleaning up any old stale Minikube contexts safely..."
# # Deletes old cluster instances so we get a clean slate with zero data corruption
# minikube delete || true

# echo "Building local application containers..."
# docker build -t weather-station:latest ./weather-station
# docker build -t central-station:latest ./central-station
# echo "Docker images compiled successfully."

# echo "Booting up localized safe Minikube cluster node..."
# # ENFORCED SAFETIES: Capped at 6GB disk usage to protect your 8.2GB space limit
# minikube start --driver=docker --memory=3000 --cpus=2 --disk-size=6g

# echo "Loading built images into the active cluster container registry..."
# minikube image load weather-station:latest
# minikube image load central-station:latest

# echo "Injecting safe YAML cluster manifests..."
# kubectl apply -f pipeline-deployment.yaml

# echo "---"
# echo "Deployment processing triggered! Track real-time progress using:"
# echo "kubectl get pods -n pipeline -w"