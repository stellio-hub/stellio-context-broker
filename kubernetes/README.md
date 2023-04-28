Tested on a Minikube cluster (for the moment).
this new version has been tested on rancher RKE, RKE2, OVH managed K8S, Kubernetes 1.20, 1.24...


For instructions on how to install Minikube, please follow https://minikube.sigs.k8s.io/docs/start/.

* Start your cluster

```shell script
minikube start
```

* Register the config map
 
```shell script
kubectl apply -f stellio-configmap.yaml
```

* Start all pods
  
```shell script
kubectl apply -f api-gateway-deployment.yaml \
-f kafka-deployment.yaml \
-f postgres-deployment.yaml \
-f search-service-deployment.yaml \
-f subscription-service-deployment.yaml
```

* add a json-ld context server using kubectl -k => kustomize
```shell script
kubectl apply -k
```

* Start ingress

```shell script
kubectl apply -f stellio-ingress.yaml
```

* To follow the status of the pods:

```shell script
kubectl get pods
```

* To see the logs of a pod:

```shell script
kubectl logs -f <pod_name>
```

* To find the IP and port exposed to which you can send requests to access Stellio, use:

```shell script
minikube service api-gateway
```
