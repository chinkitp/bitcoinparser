kubectl delete secrets storage-key-file
kubectl delete -f ./k8s/gcp-rabbit-server.yaml
kubectl delete -f ./k8s/gcp-service.yaml
kubectl delete -f ./k8s/gcp-master.yaml
kubectl delete -f ./k8s/gcp-worker.yaml