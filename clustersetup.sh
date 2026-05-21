#!/bin/bash

docker build -t weather-station:latest ./weather-station
docker build -t central-station:latest ./central-station
echo "Docker images (weather-station and central-station)built successfully"

echo "starting minikube"
minikube start --driver=docker


echo "loading docker images into minikube"
minikube image load weather-station:latest
minikube image load central-station:latest
echo "Docker images loaded into minikube successfully"

echo "deploying to kubernetesss"
kubectl apply -f pipeline-deployment.yaml

echo "Done, but check pods status by using this command:"
echo "kubectl get pods -n pipeline"