Tested on a Minikube cluster (for the moment).

First register config map:  
```shell script
kubectl apply -f stellio-configmap.yaml
```

Then start all pods:  
```shell script
kubectl apply -f api-gateway-deployment.yaml \
-f entity-service-deployment.yaml \
-f kafka-deployment.yaml \
-f neo4j-deployment.yaml \
-f postgres-deployment.yaml \
-f search-service-deployment.yaml \
-f subscription-service-deployment.yaml \
-f zookeeper-deployment.yaml
```

Then start ingress:
```shell script
kubectl apply -f stellio-ingress.yaml
```

To find the ip adress and port exposed from which you should send requests to access Stellio, use :
```shell script
minikube service api-gateway
```
