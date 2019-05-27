#!/bin/bash

kubectl apply -f ./kubernetes/spark-master-deployment.yaml
kubectl apply -f ./kubernetes/spark-master-service.yaml

sleep 10

kubectl apply -f ./kubernetes/spark-worker-deployment.yaml
kubectl apply -f ./kubernetes/spark-master-ingress.yaml