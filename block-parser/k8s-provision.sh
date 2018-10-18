export PROJECT_ID="$(gcloud config get-value project -q)"
kubectl create secret generic storage-key-file --from-file ./Secrets/bitcoin-graph-108593a03cbe.json
docker build ./master/ -t "gcr.io/${PROJECT_ID}/bitcoin-master:v13" --no-cache
docker push "gcr.io/${PROJECT_ID}/bitcoin-master:v13"
kubectl create -f ./k8s/gcp-rabbit-server.yaml
kubectl create -f ./k8s/gcp-service.yaml
sleep 5
kubectl create -f ./k8s/gcp-master.yaml
sleep 5
docker build ./worker/ -t "gcr.io/${PROJECT_ID}/bitcoin-worker:v13" --no-cache
docker push "gcr.io/${PROJECT_ID}/bitcoin-worker:v13"
kubectl create -f ./k8s/gcp-worker.yaml