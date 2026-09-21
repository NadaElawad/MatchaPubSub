# ☸️ Running MatchaPubSub on Kubernetes

This guide walks you through deploying the **Matcha Waiter Service** as a self-healing, scalable **Deployment** in Kubernetes.

---

## 1. Enable Kubernetes on Your Mac (One-Time Setup)

Docker Desktop includes a full single-node Kubernetes cluster that can be turned on with one toggle:

1. Open the **Docker Desktop** application on your Mac.
2. Click the **⚙️ (Settings)** icon in the top right.
3. In the left navigation menu, click **Kubernetes**.
4. Check the box: **☑️ Enable Kubernetes**.
5. Click **Apply & restart** (it will take ~1–2 minutes to download and start the cluster).

To verify your cluster is running:
```bash
kubectl get nodes
```
*(You should see `docker-desktop` in status `Ready`)*

---

## 2. Deploy the Waiter to Kubernetes

Apply the Deployment manifest:
```bash
kubectl apply -f k8s/waiter-deployment.yaml
```

### Inspect the Running Waiter Pod:
```bash
# Check pod status
kubectl get pods -l app=matcha-waiter

# Stream the waiter's live terminal logs from inside Kubernetes!
kubectl logs -f deployment/matcha-waiter
```

---

## 3. The Kubernetes Experiments 🧪

### Experiment A: Order from the Mac, Brew in Kubernetes!
Open a separate terminal and place an order:
```bash
source .venv/bin/activate
python3 client.py --name "Nada" --drink "Iced Ceremonial Matcha Latte"
```
👀 Watch your Kubernetes waiter pod logs: it will sift the matcha, whisk it, and broadcast the ready event back to your Mac client!

---

### Experiment B: Self-Healing (Simulate a Crash)
What happens if a waiter pod crashes? Try deleting it:
```bash
# Get the pod name
kubectl get pods

# Kill the pod
kubectl delete pod <pod-name>
```
Run `kubectl get pods` immediately: Kubernetes will have automatically created a brand-new waiter pod within seconds. **Zero downtime.**

---

### Experiment C: Scale the Waiters During a Rush!
Scale from 1 Solo Waiter to 3 concurrent Baristas:
```bash
kubectl scale deployment matcha-waiter --replicas=3
```
Now run:
```bash
python3 rush.py --count 6
```
Because Kafka's consumer group protocol automatically rebalances the 3 partitions among the 3 pods, all 3 Kubernetes waiters will brew the 6 drinks simultaneously in parallel!
