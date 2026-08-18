#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-helm}"  # helm | manifest
SKIP_SECRETS=false
for arg in "$@"; do [[ "$arg" == "--skip-secrets" ]] && SKIP_SECRETS=true; done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
step()    { echo -e "\n${CYAN}━━━ $* ━━━${NC}"; }
ask()     { read -rp "  $1: " "$2"; }
ask_pass(){ read -rsp "  $1: " "$2"; echo; }

usage() {
  cat <<EOF
Usage: $0 [helm|manifest] [--skip-secrets]

Modes:
  helm            Deploy via Helm charts (default, recommended for fresh cluster)
  manifest        Deploy via raw kubectl manifests (restore only — NOT for fresh cluster)

Flags:
  --skip-secrets  Bỏ qua bước nhập secret (dùng khi secrets đã tồn tại trên cluster)

Examples:
  $0 helm                   # Fresh cluster — nhập secrets rồi deploy
  $0 helm --skip-secrets    # Redeploy — secrets đã có sẵn
  $0 manifest --skip-secrets

Secrets được tạo interactively ở đầu script.
Bỏ trống → bỏ qua secret đó (phải đã tồn tại trên cluster).
EOF
  exit 0
}

[[ "$MODE" == "-h" || "$MODE" == "--help" ]] && usage
[[ "$MODE" != "helm" && "$MODE" != "manifest" ]] && die "Unknown mode: $MODE. Dùng 'helm' hoặc 'manifest'."

# ── Prerequisites ────────────────────────────────────────────────
info "Checking prerequisites..."
command -v kubectl &>/dev/null || die "kubectl not found"
[[ "$MODE" == "helm" ]] && { command -v helm &>/dev/null || die "helm not found"; }
kubectl cluster-info &>/dev/null || die "kubectl cannot reach cluster"

# ── Longhorn StorageClass ─────────────────────────────────────────
if ! kubectl get storageclass longhorn &>/dev/null; then
  warn "StorageClass 'longhorn' không tìm thấy trên cluster."
  warn "Toàn bộ PVC sẽ fail nếu không có Longhorn."
  read -rp "  Cài Longhorn v1.7.0 ngay bây giờ? [y/N] " _ans
  if [[ "$_ans" == "y" || "$_ans" == "Y" ]]; then
    info "Cài Longhorn v1.7.0..."
    kubectl apply -f https://raw.githubusercontent.com/longhorn/longhorn/v1.7.0/deploy/longhorn.yaml
    info "Chờ Longhorn pods ready (timeout 5m)..."
    kubectl wait pod --all -n longhorn-system \
      --for=condition=ready --timeout=300s || \
      warn "Một số pods chưa ready — kiểm tra: kubectl get pods -n longhorn-system"
    kubectl patch storageclass longhorn \
      -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}' \
      2>/dev/null || true
    info "Longhorn installed."
  else
    warn "Bỏ qua cài Longhorn — PVC sẽ pending nếu không có StorageClass phù hợp."
  fi
else
  info "StorageClass longhorn: OK"
fi

if [[ "$MODE" == "manifest" ]]; then
  warn "═══════════════════════════════════════════════════════════════"
  warn "MANIFEST MODE không phải installer hoàn chỉnh cho cluster trống."
  warn "manifests/ chỉ là snapshot workload specs (deployment/sts/svc/cm)."
  warn "THIẾU: ServiceAccounts, ClusterRole/RoleBinding (RBAC), và"
  warn "Airflow init Jobs (db-migrate, create-user — Helm hooks)."
  warn "→ Pod sẽ fail do thiếu serviceAccount; Airflow không init được DB."
  warn ""
  warn "Dùng 'helm' mode để deploy cluster mới (đầy đủ CRD/RBAC/SA/Jobs)."
  warn "Manifest mode chỉ dùng để inspect/restore workload trên cluster"
  warn "ĐÃ có sẵn RBAC (vd: redeploy sau khi xóa nhầm deployment)."
  warn "═══════════════════════════════════════════════════════════════"
  read -rp "Tiếp tục manifest mode? [y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || die "Aborted. Chạy lại với: $0 helm"
fi

# ══════════════════════════════════════════════════════════════════
# SECRET COLLECTION
# ══════════════════════════════════════════════════════════════════
# Biến toàn cục (dùng trong init SQL và apply secrets)
MINIO_USER="" MINIO_PASS=""
PG_PASS=""
REDIS_PASS=""
FERNET_KEY="" AIRFLOW_DB_PASS="" AIRFLOW_DB_HOST=""
API_SECRET="" JWT_SECRET="" AIRFLOW_SSH_KEY_PATH="" DOCKERHUB_CONFIG_PATH=""
MSSQL_SA_PASS="" MSSQL_DEBEZIUM_PASS=""
HIVE_DB_PASS="" HIVE_DB_HOST=""
DREMIO_PASS=""
GITLAB_TOKEN="" JENKINS_SSH_KEY_PATH=""
# MinIO creds — dùng chung cho Airflow, Spark, ELK (không hỏi lại)
# MINIO_USER / MINIO_PASS được set ở section [1/7]

apply_secret() {
  # apply_secret <namespace> <secret-name> [extra kubectl args...]
  local ns=$1 name=$2; shift 2
  kubectl create secret generic "$name" "$@" \
    -n "$ns" --dry-run=client -o yaml 2>/dev/null | kubectl apply -f - >/dev/null 2>&1 && \
    info "  ✓ secret/$name → $ns" || \
    warn "  ✗ secret/$name FAILED — kiểm tra lại giá trị"
}

collect_secrets() {
  echo ""
  echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${CYAN}║           INTERACTIVE SECRET COLLECTION                      ║${NC}"
  echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
  echo -e "  ${YELLOW}Tips:${NC}"
  echo "  • Passwords: input ẩn (không hiển thị trên màn hình)"
  echo "  • Enter để bỏ qua — secret đó phải đã tồn tại trên cluster"
  echo "  • Fernet key / API secret / JWT secret: Enter để tự động generate"
  echo "  • MinIO credentials nhập 1 lần → tự apply cho Airflow, Spark, ELK"
  echo "  • PostgreSQL password → tự dùng cho Airflow DB nếu bỏ trống Airflow DB password"
  echo ""

  # ─── [1/7] MinIO ───────────────────────────────────────────────
  step "[1/7] MinIO credentials  (dùng chung cho MinIO + Airflow + Spark + ELK)"
  ask      "rootUser           [minioadmin]" _v; MINIO_USER="${_v:-minioadmin}"
  ask_pass "rootPassword" MINIO_PASS
  echo ""
  echo -e "  ${CYAN}→ Auto-propagate:${NC}"
  echo "     airflow-minio-conn    (bnctl-airflow-development-ns)"
  echo "     minio-credentials     (bnctl-spark2-development-ns) — keys: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"
  echo "     minio-credentials     (bnctl-elk-development-ns)    — keys: access_key / secret_key"

  # ─── [2/7] PostgreSQL ──────────────────────────────────────────
  step "[2/7] PostgreSQL  (bnctl-postgres-development-ns)"
  ask_pass "postgres-password  (root / superuser)" PG_PASS

  ask_pass "Airflow DB password  (role 'airflow')  [Enter = same as postgres-password]" AIRFLOW_DB_PASS
  [[ -z "$AIRFLOW_DB_PASS" ]] && { AIRFLOW_DB_PASS="$PG_PASS"; info "  → Airflow DB password = postgres-password"; }

  ask      "PostgreSQL host  [cluster-postgresql-primary.bnctl-postgres-development-ns]" _v
  AIRFLOW_DB_HOST="${_v:-cluster-postgresql-primary.bnctl-postgres-development-ns}"

  ask_pass "hive_user DB password  (role 'hive_user')  [Enter = same as postgres-password]" HIVE_DB_PASS
  [[ -z "$HIVE_DB_PASS" ]] && { HIVE_DB_PASS="$PG_PASS"; info "  → hive_user password = postgres-password"; }
  HIVE_DB_HOST="$AIRFLOW_DB_HOST"
  echo ""
  echo -e "  ${CYAN}→ Auto-propagate:${NC}"
  echo "     airflow-metadata      (bnctl-airflow-development-ns) — postgresql+psycopg2://airflow:<pass>@<host>/airflow"
  echo "     hive-metastore-secret (bnctl-hive-development-ns)   — jdbc:postgresql://<host>/hive_metastore"

  # ─── [3/7] Redis ───────────────────────────────────────────────
  step "[3/7] Redis  (bnctl-redis-development-ns)"
  ask_pass "redis-password" REDIS_PASS

  # ─── [4/7] Airflow extras ──────────────────────────────────────
  step "[4/7] Airflow — keys & files  (bnctl-airflow-development-ns)"
  ask_pass "fernet-key  [Enter = auto-generate]" FERNET_KEY
  if [[ -z "$FERNET_KEY" ]]; then
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null \
      || openssl rand -base64 32)
    info "  → Fernet key đã generate tự động"
  fi

  ask_pass "api-secret-key  [Enter = auto-generate]" API_SECRET
  [[ -z "$API_SECRET" ]] && { API_SECRET=$(openssl rand -base64 32); info "  → API secret key đã generate tự động"; }

  ask_pass "jwt-secret  [Enter = auto-generate]" JWT_SECRET
  [[ -z "$JWT_SECRET" ]] && { JWT_SECRET=$(openssl rand -base64 32); info "  → JWT secret đã generate tự động"; }

  ask      "Path tới SSH deploy key  (git-sync pull DAGs từ GitLab)" AIRFLOW_SSH_KEY_PATH
  ask      "Path tới Docker Hub config.json  (pull images)" DOCKERHUB_CONFIG_PATH

  # ─── [5/7] Kafka / MSSQL ───────────────────────────────────────
  step "[5/7] Kafka / MSSQL  (bnctl-kafka-development-ns)"
  echo "  SQL Server yêu cầu: ≥8 ký tự, chữ hoa + chữ thường + số + ký tự đặc biệt"
  ask_pass "MSSQL SA password" MSSQL_SA_PASS
  ask_pass "Debezium connector password  [Enter = same as SA password]" MSSQL_DEBEZIUM_PASS
  [[ -z "$MSSQL_DEBEZIUM_PASS" ]] && { MSSQL_DEBEZIUM_PASS="$MSSQL_SA_PASS"; info "  → Debezium password = SA password"; }

  # ─── [6/7] Dremio ──────────────────────────────────────────────
  step "[6/7] Dremio  (bnctl-dremio-development-ns)"
  ask_pass "dremio-admin-password" DREMIO_PASS

  # ─── [7/7] Jenkins ─────────────────────────────────────────────
  step "[7/7] Jenkins  (bnctl-jenkins-development-ns)"
  ask_pass "GitLab Personal Access Token  (SCM checkout + webhook)" GITLAB_TOKEN
  ask      "Path tới Airflow deploy key  (Jenkins dùng để push deploy key)" JENKINS_SSH_KEY_PATH

  # ─── Apply tất cả secrets ──────────────────────────────────────
  echo ""
  info "Applying secrets lên cluster..."

  [[ -n "$MINIO_PASS" ]] && \
    apply_secret bnctl-minio-development-ns minio \
      --from-literal=rootUser="$MINIO_USER" \
      --from-literal=rootPassword="$MINIO_PASS"

  [[ -n "$PG_PASS" ]] && \
    apply_secret bnctl-postgres-development-ns cluster-postgresql \
      --from-literal=postgres-password="$PG_PASS" \
      --from-literal=password="$PG_PASS"

  [[ -n "$REDIS_PASS" ]] && \
    apply_secret bnctl-redis-development-ns redis-ha \
      --from-literal=redis-password="$REDIS_PASS"

  [[ -n "$FERNET_KEY" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-fernet-key \
      --from-literal=fernet-key="$FERNET_KEY"

  [[ -n "$AIRFLOW_DB_PASS" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-metadata \
      --from-literal=connection="postgresql+psycopg2://airflow:${AIRFLOW_DB_PASS}@${AIRFLOW_DB_HOST}:5432/airflow"

  # MinIO → Airflow connection (auto-propagate từ MinIO creds)
  [[ -n "$MINIO_PASS" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-minio-conn \
      --from-literal=connection="aws://${MINIO_USER}:${MINIO_PASS}@?endpoint_url=http://minio-svc.bnctl-minio-development-ns:9000"

  [[ -n "$API_SECRET" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-api-secret-key \
      --from-literal=api-secret-key="$API_SECRET"

  [[ -n "$JWT_SECRET" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-jwt-secret \
      --from-literal=jwt-secret="$JWT_SECRET"

  [[ -n "$AIRFLOW_SSH_KEY_PATH" && -f "$AIRFLOW_SSH_KEY_PATH" ]] && \
    apply_secret bnctl-airflow-development-ns airflow-ssh-secret \
      --from-file=gitSshKey="$AIRFLOW_SSH_KEY_PATH"

  [[ -n "$DOCKERHUB_CONFIG_PATH" && -f "$DOCKERHUB_CONFIG_PATH" ]] && \
    apply_secret bnctl-airflow-development-ns dockerhub-credentials \
      --from-file=config.json="$DOCKERHUB_CONFIG_PATH"

  # MinIO → Spark (auto-propagate)
  [[ -n "$MINIO_PASS" ]] && \
    apply_secret bnctl-spark2-development-ns minio-credentials \
      --from-literal=AWS_ACCESS_KEY_ID="$MINIO_USER" \
      --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_PASS"

  [[ -n "$MSSQL_SA_PASS" ]] && \
    apply_secret bnctl-kafka-development-ns mssql-sa-credentials \
      --from-literal=SA_PASSWORD="$MSSQL_SA_PASS"

  [[ -n "$MSSQL_DEBEZIUM_PASS" ]] && \
    apply_secret bnctl-kafka-development-ns mssql-credentials \
      --from-literal=MSSQL_PASSWORD="$MSSQL_DEBEZIUM_PASS"

  [[ -n "$HIVE_DB_PASS" ]] && \
    apply_secret bnctl-hive-development-ns hive-metastore-secret \
      --from-literal=metastore-db-url="jdbc:postgresql://${HIVE_DB_HOST}:5432/hive_metastore" \
      --from-literal=metastore-db-user="hive_user" \
      --from-literal=metastore-db-password="$HIVE_DB_PASS"

  [[ -n "$DREMIO_PASS" ]] && \
    apply_secret bnctl-dremio-development-ns cluster-dremio \
      --from-literal=dremio-admin-password="$DREMIO_PASS"

  # MinIO → ELK Logstash (auto-propagate, key names khác Spark)
  [[ -n "$MINIO_PASS" ]] && \
    apply_secret bnctl-elk-development-ns minio-credentials \
      --from-literal=access_key="$MINIO_USER" \
      --from-literal=secret_key="$MINIO_PASS"

  if [[ -n "$GITLAB_TOKEN" ]]; then
    local kubeconfig_b64
    kubeconfig_b64=$(kubectl config view --raw | base64 -w0)
    local jenkins_args=(
      --from-literal=GITLAB_API_TOKEN="$GITLAB_TOKEN"
      --from-literal=KUBECONFIG_B64="$kubeconfig_b64"
    )
    [[ -n "$JENKINS_SSH_KEY_PATH" && -f "$JENKINS_SSH_KEY_PATH" ]] && \
      jenkins_args+=(--from-literal=AIRFLOW_SSH_KEY="$(cat "$JENKINS_SSH_KEY_PATH")")
    apply_secret bnctl-jenkins-development-ns jenkins-casc-secrets "${jenkins_args[@]}"
  fi

  echo ""
  info "Secret collection done."
}

# ── Namespaces ───────────────────────────────────────────────────
info "Creating namespaces..."
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
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done
info "Namespaces ready."

# ── Collect secrets (interactive) ───────────────────────────────
if [[ "$SKIP_SECRETS" == "false" ]]; then
  collect_secrets
else
  info "Skipping secret collection (--skip-secrets)."
fi

# ── Helm repos ───────────────────────────────────────────────────
if [[ "$MODE" == "helm" ]]; then
  info "Adding Helm repos..."
  helm repo add apache-airflow https://airflow.apache.org               2>/dev/null || true
  helm repo add minio          https://charts.min.io                    2>/dev/null || true
  helm repo add bitnami        https://charts.bitnami.com/bitnami       2>/dev/null || true
  helm repo add strimzi        https://strimzi.io/charts                2>/dev/null || true
  helm repo add jenkins        https://charts.jenkins.io                2>/dev/null || true
  helm repo add elastic        https://helm.elastic.co                  2>/dev/null || true
  helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
  helm repo update >/dev/null
  info "Helm repos updated."
fi

# ── Helpers ──────────────────────────────────────────────────────
patch_hive_minio_config() {
  # Auto-propagate MinIO creds vào Hive ConfigMap metastore-cfg (core-site.xml)
  [[ -z "$MINIO_PASS" ]] && return

  info "Patching Hive ConfigMap metastore-cfg — MinIO credentials..."

  local _tmppy
  _tmppy=$(mktemp /tmp/patch_hive_XXXXXX.py)

  cat > "$_tmppy" << 'PYEOF'
import sys, json, re
cm   = json.load(sys.stdin)
user = sys.argv[1]
pw   = sys.argv[2]
for key in cm.get("data", {}):
    v = cm["data"][key]
    v = re.sub(r"(<name>fs\.s3a\.access\.key</name>\s*<value>)[^<]*(</value>)",
               r"\g<1>" + user + r"\g<2>", v)
    v = re.sub(r"(<name>fs\.s3a\.secret\.key</name>\s*<value>)[^<]*(</value>)",
               r"\g<1>" + pw   + r"\g<2>", v)
    cm["data"][key] = v
for f in ("resourceVersion", "uid", "creationTimestamp", "generation"):
    cm.get("metadata", {}).pop(f, None)
print(json.dumps(cm))
PYEOF

  kubectl get configmap metastore-cfg \
    -n bnctl-hive-development-ns -o json 2>/dev/null \
  | python3 "$_tmppy" "$MINIO_USER" "$MINIO_PASS" \
  | kubectl apply -f - >/dev/null \
  && info "  ✓ metastore-cfg patched" \
  || warn "  ✗ patch metastore-cfg FAILED"

  rm -f "$_tmppy"

  kubectl rollout restart deployment/hive-metastore \
    -n bnctl-hive-development-ns >/dev/null 2>&1 \
  && info "  ✓ hive-metastore restarted" || true
}

wait_deploy() {
  local name=$1 ns=$2 timeout=${3:-180s}
  info "Waiting for deployment/$name in $ns..."
  kubectl rollout status deployment/"$name" -n "$ns" --timeout="$timeout" || \
    warn "Timeout waiting for $name — continuing anyway"
}

wait_sts() {
  local name=$1 ns=$2 timeout=${3:-180s}
  info "Waiting for statefulset/$name in $ns..."
  kubectl rollout status statefulset/"$name" -n "$ns" --timeout="$timeout" || \
    warn "Timeout waiting for $name — continuing anyway"
}

helm_install() {
  local release=$1 chart=$2 version=$3 values=$4 ns=$5
  if helm status "$release" -n "$ns" &>/dev/null; then
    info "Upgrading $release..."
    helm upgrade "$release" "$chart" --version "$version" -f "$values" -n "$ns"
  else
    info "Installing $release..."
    helm install "$release" "$chart" --version "$version" -f "$values" -n "$ns"
  fi
}

# ── 1. MinIO ─────────────────────────────────────────────────────
info "==> [1/10] MinIO"
if [[ "$MODE" == "helm" ]]; then
  helm_install minio minio/minio 5.1.0 "$SCRIPT_DIR/minio/values-full.yaml" bnctl-minio-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/minio/manifests/" -n bnctl-minio-development-ns
fi
wait_sts minio bnctl-minio-development-ns 300s

# ── 2. PostgreSQL ────────────────────────────────────────────────
info "==> [2/10] PostgreSQL"
if [[ "$MODE" == "helm" ]]; then
  helm_install cluster bitnami/postgresql 18.0.11 "$SCRIPT_DIR/postgres/values-full.yaml" bnctl-postgres-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/postgres/manifests/" -n bnctl-postgres-development-ns
fi
wait_sts cluster-postgresql-primary bnctl-postgres-development-ns 300s

# DWH-critical DB/roles — inject passwords từ collect_secrets() nếu có
info "Creating DWH databases (airflow, hive_metastore, etl_control, cob_control)..."
_airflow_pass="${AIRFLOW_DB_PASS:-airflow_secure_password}"
_hive_pass="${HIVE_DB_PASS:-hive_password}"
sed -e "s|airflow_secure_password|${_airflow_pass}|g" \
    -e "s|hive_password|${_hive_pass}|g" \
    "$SCRIPT_DIR/postgres/init-dwh-databases.sql" | \
  kubectl exec -i -n bnctl-postgres-development-ns cluster-postgresql-primary-0 -c postgresql -- \
    sh -c 'PGPASSWORD=$(cat $POSTGRES_PASSWORD_FILE) psql -U postgres -v ON_ERROR_STOP=1' || \
  die "Failed to create DWH databases — Hive/Airflow sẽ không khởi động được"

# ── 3. Redis ─────────────────────────────────────────────────────
info "==> [3/10] Redis"
if [[ "$MODE" == "helm" ]]; then
  helm_install redis-ha bitnami/redis 23.1.3 "$SCRIPT_DIR/redis/values-full.yaml" bnctl-redis-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/redis/manifests/" -n bnctl-redis-development-ns
fi
wait_sts redis-ha-node bnctl-redis-development-ns 180s

# ── 4. Hive Metastore ────────────────────────────────────────────
info "==> [4/10] Hive Metastore"
kubectl apply -f "$SCRIPT_DIR/hive/manifests/"
wait_deploy hive-metastore bnctl-hive-development-ns 180s
patch_hive_minio_config

# ── 5. Dremio ────────────────────────────────────────────────────
info "==> [5/10] Dremio"
if [[ "$MODE" == "helm" ]]; then
  helm_install cluster bitnami/dremio 3.0.13 "$SCRIPT_DIR/dremio/values-full.yaml" bnctl-dremio-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/dremio/manifests/" -n bnctl-dremio-development-ns
fi
wait_sts cluster-dremio-master-coordinator bnctl-dremio-development-ns 300s

# ── 6. Kafka (Strimzi) ───────────────────────────────────────────
info "==> [6/10] Kafka (Strimzi operator)"
if [[ "$MODE" == "helm" ]]; then
  helm_install strimzi-operator strimzi/strimzi-kafka-operator 0.45.0 "$SCRIPT_DIR/kafka/values-full.yaml" bnctl-kafka-development-ns
  info "Waiting for Strimzi operator..."
  kubectl wait deployment/strimzi-cluster-operator \
    -n bnctl-kafka-development-ns --for=condition=available --timeout=120s
  info "Applying Kafka CR (KRaft)..."
  kubectl apply -f "$SCRIPT_DIR/kafka/kafka-cr.yaml"
  info "Deploying MSSQL + Kafka UI..."
  kubectl apply -f "$SCRIPT_DIR/kafka/mssql-kafkaui.yaml" -n bnctl-kafka-development-ns
else
  info "Installing Strimzi CRDs..."
  kubectl apply -f "$SCRIPT_DIR/kafka/crds.yaml"
  kubectl apply -f "$SCRIPT_DIR/kafka/manifests/" -n bnctl-kafka-development-ns
  info "Waiting for Strimzi operator..."
  kubectl wait deployment/strimzi-cluster-operator \
    -n bnctl-kafka-development-ns --for=condition=available --timeout=120s
  info "Applying Kafka CR (KRaft)..."
  kubectl apply -f "$SCRIPT_DIR/kafka/kafka-cr.yaml"
fi

info "Waiting for MSSQL ready..."
wait_deploy mssql bnctl-kafka-development-ns 180s

info "Waiting for Kafka brokers ready..."
kubectl wait pod -l strimzi.io/name=cdc-kafka \
  -n bnctl-kafka-development-ns --for=condition=ready --timeout=300s || \
  warn "Kafka brokers not ready in 5m — Debezium may fail to connect"

info "Applying Debezium Connect + MSSQL Connector CR..."
kubectl apply -f "$SCRIPT_DIR/kafka/kafka-connect-cr.yaml"

# ── 7. Spark Operator ────────────────────────────────────────────
info "==> [7/10] Spark Operator"
if [[ "$MODE" == "helm" ]]; then
  helm_install spark-operator spark-operator/spark-operator 2.5.0 "$SCRIPT_DIR/spark-operator/values-full.yaml" spark-operator
else
  info "Installing Spark Operator CRDs..."
  kubectl apply -f "$SCRIPT_DIR/spark-operator/crds.yaml"
  kubectl apply -f "$SCRIPT_DIR/spark-operator/manifests/" -n spark-operator
fi
wait_deploy spark-operator-controller spark-operator 120s

# ── 8. Airflow ───────────────────────────────────────────────────
info "==> [8/10] Airflow"
if [[ "$MODE" == "helm" ]]; then
  helm_install airflow apache-airflow/airflow 1.18.0 "$SCRIPT_DIR/airflow/values-full.yaml" bnctl-airflow-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/airflow/manifests/" -n bnctl-airflow-development-ns
fi
wait_deploy airflow-scheduler bnctl-airflow-development-ns 600s

info "Patching Airflow image (CI/CD override)..."
AIRFLOW_IMAGE="haiptjits/dwh-test:airflow-build-185"
kubectl set image deployment/airflow-scheduler \
  scheduler="$AIRFLOW_IMAGE" scheduler-log-groomer="$AIRFLOW_IMAGE" \
  -n bnctl-airflow-development-ns
kubectl set image deployment/airflow-dag-processor \
  dag-processor="$AIRFLOW_IMAGE" dag-processor-log-groomer="$AIRFLOW_IMAGE" \
  -n bnctl-airflow-development-ns
kubectl set image deployment/airflow-api-server \
  api-server="$AIRFLOW_IMAGE" \
  -n bnctl-airflow-development-ns
kubectl set image statefulset/airflow-triggerer \
  triggerer="$AIRFLOW_IMAGE" triggerer-log-groomer="$AIRFLOW_IMAGE" \
  -n bnctl-airflow-development-ns

info "Applying DAG ConfigMap patches..."
DAG_DIR="$SCRIPT_DIR/../dags"
SPARK_DIR="$SCRIPT_DIR/../spark-app"
kubectl create configmap pull-bulk-dag-patch \
  --from-file=pull_bulk_dag.py="$DAG_DIR/pull_bulk_dag.py" \
  --from-file=t24_bcp_extract.yaml="$SPARK_DIR/extract/t24_bcp_extract.yaml" \
  --from-file=t24_bulk_parse.yaml="$SPARK_DIR/parser/t24_bulk_parse.yaml" \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap pull-bulk-fresh-dag-patch \
  --from-file=pull_bulk_fresh_dag.py="$DAG_DIR/pull_bulk_fresh_dag.py" \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap pull-bulk-fresh-parse-yaml \
  --from-file=t24_bulk_fresh_parse.yaml="$SPARK_DIR/parser/t24_bulk_fresh_parse.yaml" \
  -n bnctl-airflow-development-ns --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment/airflow-scheduler deployment/airflow-dag-processor \
  -n bnctl-airflow-development-ns

# ── 9. Jenkins ───────────────────────────────────────────────────
info "==> [9/10] Jenkins"
if [[ "$MODE" == "helm" ]]; then
  helm_install jenkins jenkins/jenkins 5.8.93 "$SCRIPT_DIR/jenkins/values-full.yaml" bnctl-jenkins-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/jenkins/manifests/" -n bnctl-jenkins-development-ns
fi
wait_sts jenkins bnctl-jenkins-development-ns 300s

# ── 10. ELK ──────────────────────────────────────────────────────
info "==> [10/10] ELK"
kubectl get secret minio-credentials -n bnctl-elk-development-ns &>/dev/null || \
  die "Secret 'minio-credentials' missing in bnctl-elk-development-ns — tạo lại bằng cách chạy collect_secrets hoặc xem README Bước 3.8"

if [[ "$MODE" == "helm" ]]; then
  helm_install elasticsearch elastic/elasticsearch 7.17.3 "$SCRIPT_DIR/elk/elasticsearch-values-full.yaml" bnctl-elk-development-ns
  info "Waiting for Elasticsearch ready..."
  kubectl wait pod -l app=elasticsearch-master \
    -n bnctl-elk-development-ns --for=condition=ready --timeout=300s || \
    warn "Elasticsearch not ready in 5m — Kibana/Logstash may fail"
  helm_install kibana   elastic/kibana   7.17.3 "$SCRIPT_DIR/elk/kibana-values-full.yaml"   bnctl-elk-development-ns
  helm_install logstash elastic/logstash 7.17.3 "$SCRIPT_DIR/elk/logstash-values-full.yaml" bnctl-elk-development-ns
else
  kubectl apply -f "$SCRIPT_DIR/elk/manifests/" -n bnctl-elk-development-ns
  info "Waiting for Elasticsearch ready..."
  kubectl wait pod -l app=elasticsearch-master \
    -n bnctl-elk-development-ns --for=condition=ready --timeout=300s || \
    warn "Elasticsearch not ready in 5m — Kibana/Logstash may fail"
fi

# ── Done ─────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✓ All services deployed successfully                        ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Verify:"
echo "  kubectl get pods -A | grep -v Completed | grep -v Running | grep -v kube-system"
echo ""
echo "Post-deploy steps bắt buộc:"
echo "  1. Dremio → Hive: vào Dremio UI → Add Source → Hive → thrift://hive-metastore.bnctl-hive-development-ns:9083"
echo "  2. Debezium: kubectl get kafkaconnector -n bnctl-kafka-development-ns"
echo "  3. Jenkins: http://jenkins-dwh.bnctl.develop → Manage Jenkins → Clouds → Test Connection"
echo ""
