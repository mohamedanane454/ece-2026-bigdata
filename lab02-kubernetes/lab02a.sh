# Lab 02 / lab-1 - kubectl sur Onyxia (pas de minikube : on travaille dans le namespace $KUBERNETES_NAMESPACE)
# Le quota du namespace impose des limits/requests sur chaque pod : on les ajoute avec kubectl set resources.
mkdir -p ~/work/lab02 && cd ~/work/lab02
# --- 1. Etat du cluster (minikube status / get nodes non disponibles : droits limites au namespace)
kubectl get nodes
kubectl top pods -A --sort-by cpu --sum=true
kubectl top pods --sort-by cpu --sum=true
kubectl get pods -n kube-system
# --- 2. kubectl
kubectl create deployment kubernetes-bootcamp --image=gcr.io/google-samples/kubernetes-bootcamp:v1
kubectl set resources deployment kubernetes-bootcamp --requests=cpu=20m,memory=32Mi --limits=cpu=100m,memory=128Mi
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
kubectl get pods
POD_NAME=$(kubectl get pods -l app=kubernetes-bootcamp -o jsonpath='{.items[0].metadata.name}')
echo "POD_NAME=$POD_NAME"
kubectl logs $POD_NAME
kubectl describe pod $POD_NAME
kubectl exec $POD_NAME -- cat /etc/os-release
kubectl exec $POD_NAME -- ls -la /
kubectl exec $POD_NAME -- cat /server.js
kubectl exec $POD_NAME -- curl -s localhost:8080
# 9. depuis la machine locale (ici le pod VS Code), sans service : pas d'acces direct par nom
curl -s -m 5 http://kubernetes-bootcamp:8080 || echo "-> pas accessible sans Service"
# --- 3. Exposer le deployment
kubectl expose deployments/kubernetes-bootcamp --type="NodePort" --port 8080
kubectl get services
kubectl describe service kubernetes-bootcamp
# minikube ip n'existe pas ici : on interroge le Service depuis le pod VS Code (DNS interne du cluster)
sleep 3
curl -s -m 5 http://kubernetes-bootcamp:8080
# --- 4. Scale up / down
kubectl scale deployments/kubernetes-bootcamp --replicas=5
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
kubectl get pods -o wide
kubectl get deployments
for i in 1 2 3 4 5 6 7 8; do curl -s -m 5 http://kubernetes-bootcamp:8080; done
kubectl scale deployments/kubernetes-bootcamp --replicas=2
sleep 15
kubectl get pods
# --- 5. Mise a jour de l'image (rolling update)
kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
for i in 1 2 3 4; do curl -s -m 5 http://kubernetes-bootcamp:8080; done
kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v3
sleep 40
kubectl get pods
kubectl rollout status deployment/kubernetes-bootcamp --timeout=10s
kubectl describe pods -l app=kubernetes-bootcamp | grep -iE 'image:|reason|failed|back-off' | sort | uniq -c
kubectl rollout undo deployments/kubernetes-bootcamp
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
kubectl get pods
for i in 1 2 3; do curl -s -m 5 http://kubernetes-bootcamp:8080; done
kubectl rollout history deployments/kubernetes-bootcamp
# 7. Retour a l'image de la partie 2 (v1)
kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=gcr.io/google-samples/kubernetes-bootcamp:v1
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
for i in 1 2 3; do curl -s -m 5 http://kubernetes-bootcamp:8080; done
# --- 6. Manifests YAML
kubectl delete service kubernetes-bootcamp
kubectl delete deployment kubernetes-bootcamp
cat > deployment.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kubernetes-bootcamp
  labels:
    app: kubernetes-bootcamp
spec:
  # TO COMPLETE #2 (d'abord 1 replica, puis 3)
  replicas: 1
  selector:
    matchLabels:
      app: kubernetes-bootcamp
  template:
    metadata:
      labels:
        app: kubernetes-bootcamp
    spec:
      containers:
      - name: kubernetes-bootcamp
        # TO COMPLETE #1
        image: gcr.io/google-samples/kubernetes-bootcamp:v1
        ports:
        - containerPort: 8080
        # ajoute pour respecter le quota du namespace Onyxia
        resources:
          requests: {cpu: 20m, memory: 32Mi}
          limits: {cpu: 100m, memory: 128Mi}
EOF
kubectl apply -f deployment.yaml
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
kubectl get pods
cat > service.yaml <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: kubernetes-bootcamp-service
spec:
  type: NodePort
  selector:
    app: kubernetes-bootcamp
  ports:
    - port: 8080
EOF
kubectl apply -f service.yaml
kubectl get services
sleep 3
curl -s -m 5 http://kubernetes-bootcamp-service:8080
sed -i 's/replicas: 1/replicas: 3/' deployment.yaml
kubectl apply -f deployment.yaml
kubectl rollout status deployment/kubernetes-bootcamp --timeout=180s
kubectl get pods
for i in 1 2 3 4 5 6; do curl -s -m 5 http://kubernetes-bootcamp-service:8080; done
# --- 7. Teardown
kubectl delete -f service.yaml
kubectl delete -f deployment.yaml
kubectl get all
true
