# Deployment Guide — BNCTL Data Warehouse (DEV)

Tài liệu này mô tả cách deploy lại toàn bộ stack từ đầu trên một cluster K8s mới, sử dụng các file đã export trong thư mục này.

---

## Stack Overview

```
MSSQL (CDC source)
    │
    ▼ Kafka (Strimzi KRaft) ─── Kafka UI
    │
    ▼ MinIO (S3-compatible object storage)
    │
    ├─ Spark Operator → Spark Jobs → Hive Metastore (Iceberg catalog)
    │                                      │
    │                               Dremio (query engine)
    │
    ▼ Airflow (orchestration) ─── Jenkins (CI/CD)
    │
    ▼ ELK (Elasticsearch + Kibana + Logstash)
```

| Service | Namespace | Helm Chart | Version | Image |
|---------|-----------|-----------|---------|-------|
| Airflow | bnctl-airflow-development-ns | apache-airflow/airflow | 1.18.0 | haiptjits/dwh-test:airflow-build-185 |
| MinIO | bnctl-minio-development-ns | minio/minio | 5.1.0 | quay.io/minio/minio:RELEASE.2024-03-03T17-50-39Z |
| Dremio | bnctl-dremio-development-ns | bitnami/dremio | 3.0.13 | bitnamilegacy/dremio:26.0.0-debian-12-r5 |
| Kafka (Strimzi) | bnctl-kafka-development-ns | strimzi/strimzi-kafka-operator | 0.45.0 | quay.io/strimzi/operator:0.45.0 |
| MSSQL | bnctl-kafka-development-ns | — | — | mcr.microsoft.com/mssql/server:2022-latest |
| Hive Metastore | bnctl-hive-development-ns | — (raw manifest) | — | tranconggg/hive-metastore:v2 |
| Spark Operator | spark-operator | spark-operator/spark-operator | 2.5.0 | kubeflow/spark-operator/controller:2.5.0 |
| Jenkins | bnctl-jenkins-development-ns | jenkins/jenkins | 5.8.93 | jenkins/jenkins:2.516.3-jdk21 |
| PostgreSQL | bnctl-postgres-development-ns | bitnami/postgresql | 18.0.11 | bitnami/postgresql:latest |
| Redis | bnctl-redis-development-ns | bitnami/redis | 23.1.3 | bitnami/redis:latest |
| Elasticsearch | bnctl-elk-development-ns | elastic/elasticsearch | 7.17.3 | — |
| Kibana | bnctl-elk-development-ns | elastic/kibana | 7.17.3 | — |
| Logstash | bnctl-elk-development-ns | elastic/logstash | 7.17.3 | — |

---

## Prerequisites

```bash
# Tools
kubectl >= 1.28
helm >= 3.12

# Kubeconfig trỏ đúng cluster
kubectl cluster-info
```

### StorageClass — Longhorn (bắt buộc)

Toàn bộ PVC dùng StorageClass `longhorn`. Kiểm tra trước khi deploy:

```bash
kubectl get storageclass
# Phải thấy: longhorn (default)   driver.longhorn.io
```

Nếu chưa có, `deploy.sh` sẽ hỏi cài tự động. Hoặc cài thủ công:

```bash
# Prerequisites trên từng worker node
# Ubuntu/Debian
apt-get install -y open-iscsi nfs-common
# RHEL/CentOS
yum install -y iscsi-initiator-utils nfs-utils && systemctl enable --now iscsid

# Cài Longhorn v1.7.0
kubectl apply -f https://raw.githubusercontent.com/longhorn/longhorn/v1.7.0/deploy/longhorn.yaml

# Chờ ready
kubectl wait pod --all -n longhorn-system --for=condition=ready --timeout=300s

# Set default StorageClass
kubectl patch storageclass longhorn \
  -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

---

## Thứ tự Deploy

> **Bắt buộc** theo thứ tự — mỗi tầng phụ thuộc tầng trước.

```
1. Namespaces + Secrets
2. MinIO          ← storage cho tất cả
3. PostgreSQL      ← Airflow metadata + Hive metastore + ETL state DB
   3b. Init DWH databases  ← tạo airflow/hive_metastore/etl_control/cob_control (BẮT BUỘC)
4. Redis           ← Airflow broker
5. Hive Metastore  ← Iceberg catalog (cần DB hive_metastore)
6. Dremio          ← query engine (cần Hive)
7. Kafka + MSSQL   ← CDC source
8. Spark Operator  ← job executor
9. Airflow         ← orchestration (cần Postgres+DB airflow, Redis, MinIO)
10. Jenkins        ← CI/CD
11. ELK            ← optional logging
```

> ⚠️ **Bước 3b bắt buộc:** Postgres Helm chart chỉ init các DB phụ
> (airbyte/temporal/n8n/...). Các DB lõi DWH (`airflow`, `hive_metastore`,
> `etl_control`, `cob_control`) + role (`airflow`, `hive_user`) phải tạo bằng
> `postgres/init-dwh-databases.sql`. Thiếu → Hive Metastore + Airflow + ETL
> logging không khởi động được. `deploy.sh` tự chạy bước này.

---

## Bước 1 — Tạo Namespaces

```bash
for ns in \
  bnctl-airflow-development-ns \
  bnctl-minio-development-ns \
  bnctl-dremio-development-ns \
  bnctl-kafka-development-ns \
  bnctl-hive-development-ns \
  bnctl-spark2-development-ns \
  bnctl-jenkins-development-ns \
  bnctl-postgres-development-ns \
  bnctl-redis-development-ns \
  bnctl-elk-development-ns \
  spark-operator; do
  kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f -
done
```

---

## Bước 2 — Tạo Secrets (thủ công)

Secrets không được lưu trong repo. Tạo lại từ giá trị thực tế.

### MinIO
```bash
kubectl create secret generic minio \
  --from-literal=rootUser=<user> \
  --from-literal=rootPassword=<password> \
  -n bnctl-minio-development-ns
```

### PostgreSQL
```bash
kubectl create secret generic cluster-postgresql \
  --from-literal=postgres-password=<password> \
  --from-literal=password=<password> \
  -n bnctl-postgres-development-ns
```

### Redis
```bash
kubectl create secret generic redis-ha \
  --from-literal=redis-password=<password> \
  -n bnctl-redis-development-ns
```

### Airflow
```bash
kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key=<fernet-key> \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-metadata \
  --from-literal=connection=postgresql+psycopg2://<user>:<pass>@<host>/airflow \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-minio-conn \
  --from-literal=connection=s3://<access-key>:<secret-key>@<minio-endpoint> \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key=<fernet-key> \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-api-secret-key \
  --from-literal=api-secret-key=<key> \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-jwt-secret \
  --from-literal=jwt-secret=<secret> \
  -n bnctl-airflow-development-ns

kubectl create secret generic airflow-ssh-secret \
  --from-file=gitSshKey=<path-to-private-key> \
  -n bnctl-airflow-development-ns

kubectl create secret generic dockerhub-credentials \
  --from-file=config.json=<path-to-dockerhub-config> \
  -n bnctl-airflow-development-ns

kubectl create secret generic minio-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret> \
  -n bnctl-spark2-development-ns
```

### Kafka / MSSQL
```bash
kubectl create secret generic mssql-credentials \
  --from-literal=MSSQL_PASSWORD=<password> \
  -n bnctl-kafka-development-ns

kubectl create secret generic mssql-sa-credentials \
  --from-literal=SA_PASSWORD=<password> \
  -n bnctl-kafka-development-ns
```

### Hive
```bash
kubectl create secret generic hive-metastore-secret \
  --from-literal=metastore-db-url=<jdbc-url> \
  --from-literal=metastore-db-user=<user> \
  --from-literal=metastore-db-password=<password> \
  -n bnctl-hive-development-ns
```

### Dremio
```bash
kubectl create secret generic cluster-dremio \
  --from-literal=dremio-admin-password=<password> \
  -n bnctl-dremio-development-ns
```

### ELK — Logstash MinIO credentials
```bash
kubectl create secret generic minio-credentials \
  --from-literal=access_key=minioadmin \
  --from-literal=secret_key=<minio-secret-key> \
  -n bnctl-elk-development-ns
```

### Jenkins
```bash
kubectl create secret generic jenkins-casc-secrets \
  --from-literal=GITLAB_API_TOKEN=<gitlab-pat> \
  --from-literal=AIRFLOW_SSH_KEY="$(cat <path-to-airflow-deploy-key>)" \
  --from-literal=KUBECONFIG_B64="$(kubectl config view --raw | base64 -w0)" \
  -n bnctl-jenkins-development-ns
```

---

## Bước 3 — Thêm Helm Repos

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo add minio           https://charts.min.io
helm repo add bitnami         https://charts.bitnami.com/bitnami
helm repo add strimzi         https://strimzi.io/charts
helm repo add jenkins         https://charts.jenkins.io
helm repo add elastic         https://helm.elastic.co
helm repo add spark-operator  https://kubeflow.github.io/spark-operator
helm repo update
```

---

## Quick Start — Script tự động

`deploy.sh` chạy toàn bộ stack đúng thứ tự, tự wait từng service trước khi sang bước tiếp.

> **Bắt buộc hoàn thành Bước 2 (tạo secrets) trước khi chạy script.**

```bash
# Cluster mới — DÙNG HELM (đầy đủ CRD, RBAC, ServiceAccounts, init Jobs)
bash deployment/deploy.sh helm

# Manifest mode — CHỈ để inspect/restore workload trên cluster đã có RBAC
bash deployment/deploy.sh manifest   # sẽ cảnh báo + hỏi xác nhận
```

Script tự động:
- Tạo namespaces
- Add Helm repos + update (chỉ mode helm)
- Cài CRDs trước (Strimzi, Spark — manifest mode)
- Deploy 10 services đúng thứ tự với `kubectl wait` giữa các bước
- Patch Airflow image sau deploy
- Apply DAG ConfigMap patches

> ⚠️ **Helm mode là path deploy được hỗ trợ.** Chart tự xử lý CRD, RBAC,
> ServiceAccount, và init Jobs (Airflow db-migrate / create-user).
> **Manifest mode KHÔNG phải installer cho cluster trống** — `manifests/`
> chỉ là snapshot workload specs (deployment/sts/svc/cm), thiếu RBAC + SA +
> init Jobs. Pod sẽ fail do thiếu serviceAccount, Airflow không init được DB.
> Manifest mode chỉ dùng để khôi phục 1 workload trên cluster đã có sẵn RBAC.

Nếu muốn deploy thủ công từng service, xem Bước 4 bên dưới.

---

## Lựa chọn cách deploy

| Phương án | Lệnh | Dùng khi | Giới hạn |
|-----------|------|----------|----------|
| **A — Helm** (cluster mới) | `helm install -f values-full.yaml` | Deploy lại từ cluster trống | Cần repo + chart version còn tồn tại |
| **B — Raw manifest** | `kubectl apply -f manifests/` | Khôi phục 1 workload trên cluster **đã có RBAC** | **Không tạo CRD/RBAC/SA/init Jobs** — không dùng cho cluster trống |

> **Helm = path deploy đầy đủ.** Chart tự cài CRD (Strimzi/Spark), RBAC, ServiceAccount, và init Jobs (Airflow db-migrate/create-user).
>
> **Manifest = snapshot workload.** `*/manifests/` đã clean (`uid`, `resourceVersion`, `clusterIP`, `nodePort`), nhưng chỉ chứa deployment/statefulset/service/configmap/pvc/ingress. **Thiếu** ServiceAccounts, ClusterRole/RoleBinding, và Airflow init Jobs → pod fail trên cluster trống. CRDs đã được export riêng (`kafka/crds.yaml`, `spark-operator/crds.yaml`) và `deploy.sh manifest` tự apply trước.

---

## Bước 4 — Deploy Services

### MinIO
```bash
# Phương án A — Helm
helm install minio minio/minio --version 5.1.0 \
  -f minio/values-full.yaml \
  -n bnctl-minio-development-ns

# Phương án B — Raw manifest
kubectl apply -f minio/manifests/ -n bnctl-minio-development-ns
```

### PostgreSQL
```bash
# Phương án A — Helm
helm install cluster bitnami/postgresql --version 18.0.11 \
  -f postgres/values-full.yaml \
  -n bnctl-postgres-development-ns

# Phương án B — Raw manifest
kubectl apply -f postgres/manifests/ -n bnctl-postgres-development-ns
```

### Init DWH databases (BẮT BUỘC sau Postgres, trước Hive + Airflow)
```bash
# Tạo airflow / hive_metastore / etl_control / cob_control + roles
# ⚠️ Sửa password trong file cho khớp airflow-metadata secret + hive metastore-site.xml
kubectl exec -i -n bnctl-postgres-development-ns cluster-postgresql-primary-0 \
  -c postgresql -- sh -c 'PGPASSWORD=$(cat $POSTGRES_PASSWORD_FILE) psql -U postgres' \
  < postgres/init-dwh-databases.sql

# Verify
kubectl exec -n bnctl-postgres-development-ns cluster-postgresql-primary-0 -c postgresql -- \
  sh -c 'PGPASSWORD=$(cat $POSTGRES_PASSWORD_FILE) psql -U postgres -c "\l"' | grep -E "airflow|hive_metastore|etl_control"
```

### Redis
```bash
# Phương án A — Helm
helm install redis-ha bitnami/redis --version 23.1.3 \
  -f redis/values-full.yaml \
  -n bnctl-redis-development-ns

# Phương án B — Raw manifest
kubectl apply -f redis/manifests/ -n bnctl-redis-development-ns
```

### Hive Metastore (không có Helm chart)
```bash
kubectl apply -f hive/manifests/
```

### Dremio
```bash
# Chờ Hive Metastore ready trước

# Phương án A — Helm
helm install cluster bitnami/dremio --version 3.0.13 \
  -f dremio/values-full.yaml \
  -n bnctl-dremio-development-ns

# Phương án B — Raw manifest
kubectl apply -f dremio/manifests/ -n bnctl-dremio-development-ns
```

### Kafka (Strimzi)
```bash
# Phương án A — Helm (operator tự cài CRDs)
helm install strimzi-operator strimzi/strimzi-kafka-operator --version 0.45.0 \
  -f kafka/values-full.yaml \
  -n bnctl-kafka-development-ns
# chờ operator ready
kubectl wait deployment/strimzi-cluster-operator \
  -n bnctl-kafka-development-ns --for=condition=available --timeout=120s
# apply Kafka CR + MSSQL/kafka-ui (file tách, KHÔNG đụng operator do Helm quản lý)
kubectl apply -f kafka/kafka-cr.yaml
kubectl apply -f kafka/mssql-kafkaui.yaml -n bnctl-kafka-development-ns

# Phương án B — Raw manifest (CRDs phải cài tay trước)
kubectl apply -f kafka/crds.yaml
kubectl apply -f kafka/manifests/ -n bnctl-kafka-development-ns
kubectl wait deployment/strimzi-cluster-operator \
  -n bnctl-kafka-development-ns --for=condition=available --timeout=120s
kubectl apply -f kafka/kafka-cr.yaml
# manifests/ đã chứa mssql + kafka-ui

# ── Cả 2 phương án: chờ Kafka brokers + MSSQL, rồi deploy Debezium ──
kubectl wait pod -l strimzi.io/name=cdc-kafka \
  -n bnctl-kafka-development-ns --for=condition=ready --timeout=300s
kubectl apply -f kafka/kafka-connect-cr.yaml
```

### Spark Operator
```bash
# Phương án A — Helm (tự cài CRDs SparkApplication)
helm install spark-operator spark-operator/spark-operator --version 2.5.0 \
  -f spark-operator/values-full.yaml \
  -n spark-operator

# Phương án B — Raw manifest (CRDs phải cài tay trước)
kubectl apply -f spark-operator/crds.yaml
kubectl apply -f spark-operator/manifests/ -n spark-operator
```

### Airflow
```bash
# Chờ Postgres + Redis + MinIO ready trước

# Phương án A — Helm
helm install airflow apache-airflow/airflow --version 1.18.0 \
  -f airflow/values-full.yaml \
  -n bnctl-airflow-development-ns \
  --timeout 10m

# Phương án B — Raw manifest
kubectl apply -f airflow/manifests/ -n bnctl-airflow-development-ns

# Patch image thực (CI/CD override Helm image)
kubectl set image deployment/airflow-scheduler \
  scheduler=haiptjits/dwh-test:airflow-build-185 \
  scheduler-log-groomer=haiptjits/dwh-test:airflow-build-185 \
  -n bnctl-airflow-development-ns
kubectl set image deployment/airflow-dag-processor \
  dag-processor=haiptjits/dwh-test:airflow-build-185 \
  dag-processor-log-groomer=haiptjits/dwh-test:airflow-build-185 \
  -n bnctl-airflow-development-ns
kubectl set image deployment/airflow-api-server \
  api-server=haiptjits/dwh-test:airflow-build-185 \
  -n bnctl-airflow-development-ns
kubectl set image statefulset/airflow-triggerer \
  triggerer=haiptjits/dwh-test:airflow-build-185 \
  triggerer-log-groomer=haiptjits/dwh-test:airflow-build-185 \
  -n bnctl-airflow-development-ns

# Apply DAG patch ConfigMaps (pull_bulk, pull_bulk_fresh)
kubectl create configmap pull-bulk-dag-patch \
  --from-file=pull_bulk_dag.py=../dags/pull_bulk_dag.py \
  --from-file=t24_bcp_extract.yaml=../spark-app/extract/t24_bcp_extract.yaml \
  --from-file=t24_bulk_parse.yaml=../spark-app/parser/t24_bulk_parse.yaml \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap pull-bulk-fresh-dag-patch \
  --from-file=pull_bulk_fresh_dag.py=../dags/pull_bulk_fresh_dag.py \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap pull-bulk-fresh-parse-yaml \
  --from-file=t24_bulk_fresh_parse.yaml=../spark-app/parser/t24_bulk_fresh_parse.yaml \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

# Restart scheduler để pick up pod template mới
kubectl rollout restart deployment/airflow-scheduler -n bnctl-airflow-development-ns
kubectl rollout restart deployment/airflow-dag-processor -n bnctl-airflow-development-ns
```

### Jenkins
```bash
# Phương án A — Helm
helm install jenkins jenkins/jenkins --version 5.8.93 \
  -f jenkins/values-full.yaml \
  -n bnctl-jenkins-development-ns

# Phương án B — Raw manifest
kubectl apply -f jenkins/manifests/ -n bnctl-jenkins-development-ns
```

### ELK
```bash
# Phương án A — Helm
helm install elasticsearch elastic/elasticsearch --version 7.17.3 \
  -f elk/elasticsearch-values-full.yaml \
  -n bnctl-elk-development-ns

helm install kibana elastic/kibana --version 7.17.3 \
  -f elk/kibana-values-full.yaml \
  -n bnctl-elk-development-ns

helm install logstash elastic/logstash --version 7.17.3 \
  -f elk/logstash-values-full.yaml \
  -n bnctl-elk-development-ns

# Phương án B — Raw manifest
kubectl apply -f elk/manifests/ -n bnctl-elk-development-ns
```

---

## Bước 5 — Verify

```bash
# Tất cả pods running
kubectl get pods -A | grep -v "Completed\|Running\|kube-system" | grep -v "^NAMESPACE"

# Airflow UI
kubectl port-forward svc/airflow-api-server 8080:8080 -n bnctl-airflow-development-ns
# → http://localhost:8080

# MinIO UI
kubectl port-forward svc/minio-console 9001:9001 -n bnctl-minio-development-ns
# → http://localhost:9001

# Dremio UI
kubectl port-forward svc/cluster-dremio 9047:9047 -n bnctl-dremio-development-ns
# → http://localhost:9047

# Kafka UI
kubectl port-forward svc/kafka-ui 8080:80 -n bnctl-kafka-development-ns
# → http://localhost:8080

# DAGs đã load
POD=$(kubectl get pods -n bnctl-airflow-development-ns -l component=dag-processor -o jsonpath="{.items[0].metadata.name}")
kubectl exec -n bnctl-airflow-development-ns $POD -c dag-processor -- ls /opt/airflow/dags/repo/dags/
```

---

## Lưu ý quan trọng

- **Secrets không có trong repo** — phải tạo thủ công trước khi deploy (Bước 2)
- **DWH databases** (`airflow`, `hive_metastore`, `etl_control`, `cob_control`) KHÔNG do Postgres chart tạo — phải chạy `postgres/init-dwh-databases.sql` sau Postgres, trước Hive + Airflow
- **Airflow image** do Jenkins CI/CD quản lý qua `kubectl set image`, không phải Helm values — sau mỗi build mới cần patch lại
- **DAG patches** (pull_bulk, pull_bulk_fresh) là ConfigMap mount đè lên image — phải apply sau khi Airflow ready
- **Strimzi/Spark CRDs** — Helm chart tự cài; manifest mode phải apply `kafka/crds.yaml` + `spark-operator/crds.yaml` trước
- **Dremio → Hive** không tự động — phải cấu hình thủ công qua Dremio UI sau khi deploy (xem bên dưới)
- **PVC data** không được backup trong repo — storage (Longhorn) phải có sẵn trên cluster mới

---

## Cross-Service Connection Map

Toàn bộ kết nối giữa các services (verified từ cluster):

| Từ | Đến | Endpoint | Ghi chú |
|----|-----|----------|---------|
| Hive Metastore | PostgreSQL | `cluster-postgresql-primary.bnctl-postgres-development-ns:5432/hive_metastore` | user: `hive_user` |
| Hive Metastore | MinIO | `http://minio.bnctl-minio-development-ns:9000` | bucket: `hive-warehouse` |
| Dremio | MinIO | `http://minio.bnctl-minio-development-ns:9000` | key: `minioadmin` |
| Dremio | Hive | `thrift://hive-metastore.bnctl-hive-development-ns:9083` | cấu hình qua UI (xem bên dưới) |
| Airflow | PostgreSQL | `cluster-postgresql-primary.bnctl-postgres-development-ns:5432/airflow` | metadata DB |
| Airflow | MinIO | `http://minio-svc.bnctl-minio-development-ns:9000` | conn id: `minio_conn` |
| Spark jobs | Hive | `thrift://hive-metastore.bnctl-hive-development-ns:9083` | Iceberg catalog |
| Spark jobs | MinIO | `http://minio.bnctl-minio-development-ns:9000` | S3A filesystem |
| Debezium | MSSQL | `mssql.bnctl-kafka-development-ns:1433/testdb` | credentials từ secret `mssql-credentials` |
| Debezium | Kafka | `cdc-kafka-bootstrap:9092` | schema history topic: `schema-changes.t24` |
| Logstash | MinIO | `http://minio-svc.bnctl-minio-development-ns:9000` | bucket: `airflow-logs`, secret `minio-credentials` trong `bnctl-elk-development-ns` (keys: `access_key`/`secret_key`) |
| Logstash | Elasticsearch | `http://elasticsearch-master:9200` | index: `airflow-logs-*` (same namespace) |
| Kibana | Elasticsearch | `http://elasticsearch-master:9200` | same namespace |
| Jenkins | Kubernetes | `https://kubernetes.default` (in-cluster) | agent namespace: `bnctl-jenkins-development-ns`, image: `btthanhk4/inbound-agent:1.0.1` |
| Jenkins | GitLab | via `GITLAB_API_TOKEN` | secret `jenkins-casc-secrets` |

---

## Post-Deploy Steps bắt buộc

### 1. Kết nối Dremio với Hive Metastore

Dremio không đọc Hive config từ `dremio.conf` — phải add source qua UI.

1. Vào Dremio UI: `http://dremio-dwh.bnctl.develop` (hoặc port-forward `kubectl port-forward svc/cluster-dremio 9047:9047 -n bnctl-dremio-development-ns`)
2. **Add Source** → chọn **Hive 2.x / 3.x**
3. Điền:
   - **Hive Metastore Host:** `hive-metastore.bnctl-hive-development-ns.svc.cluster.local`
   - **Port:** `9083`
4. Tab **Storage** → bật **Enable S3-compatible storage** → điền:
   - **Endpoint:** `http://minio.bnctl-minio-development-ns.svc.cluster.local:9000`
   - **Access Key:** `minioadmin`
   - **Secret Key:** `minio-secret-key-change-me`
   - **Path-style access:** ✅
5. Save → Dremio tự sync schemas từ Hive catalog

---

### 2. Deploy Debezium Connect + MSSQL Connector

Sau khi Kafka (Strimzi operator + Kafka CR) đã ready:

```bash
# Deploy KafkaConnect (Debezium) + KafkaConnector (MSSQL CDC)
kubectl apply -f kafka/kafka-connect-cr.yaml

# Verify connector RUNNING
kubectl get kafkaconnector -n bnctl-kafka-development-ns
# Expected: mssql-source-connector   RUNNING

# Check logs nếu lỗi
kubectl logs -n bnctl-kafka-development-ns \
  -l strimzi.io/cluster=debezium-connect --tail=50
```

> **Lưu ý:** `KafkaConnector` dùng `snapshot.mode: initial` — lần đầu chạy sẽ snapshot toàn bộ 39 tables trong `testdb`. Nếu data lớn, có thể mất vài chục phút.

---

### 3. Cấu hình Jenkins

Jenkins dùng JCasC (Configuration as Code) — tự load từ ConfigMap `jenkins-jenkins-jcasc-config`. Sau deploy cần verify:

```bash
# Lấy admin password
kubectl exec -n bnctl-jenkins-development-ns statefulset/jenkins -c jenkins -- \
  cat /run/secrets/additional/chart-admin-password

# Vào Jenkins UI: http://jenkins-dwh.bnctl.develop
# Kiểm tra: Manage Jenkins → Clouds → kubernetes → Test Connection
```

Jenkins cần các credentials sau (load từ `jenkins-casc-secrets`):
- `GITLAB_API_TOKEN` — SCM checkout + pipeline trigger
- `AIRFLOW_SSH_KEY` — deploy key để Airflow git-sync pull DAGs
- `KUBECONFIG_B64` — kubectl access từ trong Kaniko build job
