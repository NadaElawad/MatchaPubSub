# ☸️ Running MatchaPubSub on Kubernetes

This guide walks you through deploying the **Matcha Waiter Service**, **Customer Jobs**, **CronJobs**, and **ConfigMaps** in Kubernetes.

---

## 1. Verify Kubernetes Cluster

Ensure your cluster is running:
```bash
kubectl get nodes
```
*(You should see `docker-desktop` in status `Ready`)*

---

## 2. Deploy Café Configuration & Waiter Service

Deploy the shared ConfigMap and the Waiter Deployment:
```bash
# 1. Shared Café Configuration (topics, prep time, servers)
kubectl apply -f k8s/cafe-configmap.yaml

# 2. Waiter Service (Deployment with 1 replica)
kubectl apply -f k8s/waiter-deployment.yaml
```

### Inspect the Running Waiter:
```bash
# Check pod status
kubectl get pods -l app=matcha-waiter

# Stream the waiter's live terminal logs from inside Kubernetes!
kubectl logs -f deployment/matcha-waiter
```

---

## 3. The Kubernetes Experiments 🧪

### Experiment A: Batch Customers using a Kubernetes `Job` 👥
Deploy 3 customer pods simultaneously:
```bash
kubectl apply -f k8s/customer-job.yaml
```
Watch the customer pods spawn, order, wait for their drinks, and complete:
```bash
kubectl get pods -l app=matcha-customer
```
You will see:
```text
NAME                   READY   STATUS      RESTARTS   AGE
customer-rush-448xd    0/1     Completed   0          9s
customer-rush-c6k5g    0/1     Completed   0          9s
customer-rush-s8bhj    0/1     Completed   0          9s
```
View one of the customer's logs to see them order and pick up their drink:
```bash
kubectl logs job/customer-rush
```

Clean up the finished job when done:
```bash
kubectl delete -f k8s/customer-job.yaml
```

---

### Experiment B: Scheduled Regulars using a `CronJob` ⏰
Deploy a recurring customer that walks in every 1 minute:
```bash
kubectl apply -f k8s/customer-cronjob.yaml
```
List active cronjobs:
```bash
kubectl get cronjobs
```
Watch the cron scheduler spawn fresh customer pods periodically:
```bash
kubectl get jobs,pods -w
```
Delete the CronJob when finished testing:
```bash
kubectl delete -f k8s/customer-cronjob.yaml
```

---

### Experiment C: Update Settings Dynamically with `ConfigMap` 📋
Want to change the waiter's brew speed from 1.5 seconds to 0.5 seconds?
Edit the ConfigMap:
```bash
kubectl edit configmap cafe-config
```
*(Change `PREP_TIME: "0.5"` and save)*

Restart the deployment to pick up the new configuration:
```bash
kubectl rollout restart deployment matcha-waiter
```

---

### Experiment D: Scale the Waiters During a Rush! 🧑‍🍳🧑‍🍳🧑‍🍳
Scale from 1 Solo Waiter to 3 concurrent Baristas:
```bash
kubectl scale deployment matcha-waiter --replicas=3
```
Now trigger another customer rush:
```bash
kubectl apply -f k8s/customer-job.yaml
```
Kafka automatically balances the 3 partitions among the 3 waiter pods, brewing all drinks in parallel!
