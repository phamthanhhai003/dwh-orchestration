#!/usr/bin/env python3
"""Regenerate BNCTL_DWH_Deployment_Guide.pdf — run from any directory."""

import os
import subprocess
import pathlib
import base64

from weasyprint import HTML, CSS

OUTPUT   = pathlib.Path(__file__).parent / "BNCTL_DWH_Deployment_Guide.pdf"
HERE     = pathlib.Path(__file__).parent
MMDC     = "/home/dmin/.nvm/versions/node/v20.20.0/bin/mmdc"
MMD_FILE = HERE / "arch.mmd"
PNG_FILE = HERE / "arch.png"


def render_mermaid() -> str:
    r = subprocess.run(
        [MMDC, "-i", str(MMD_FILE), "-o", str(PNG_FILE),
         "-b", "#ffffff", "--width", "2000", "--scale", "2"],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        raise RuntimeError(f"mmdc failed:\n{r.stderr}")
    data = base64.b64encode(PNG_FILE.read_bytes()).decode()
    return f'<img src="data:image/png;base64,{data}" style="width:100%;border-radius:6px;"/>'


ARCH_IMG = render_mermaid()

HTML_CONTENT = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8"/>
<title>BNCTL DWH Deployment Guide</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  :root {{
    --bg:        #ffffff;
    --bg2:       #f8fafc;
    --bg3:       #e2e8f0;
    --navy:      #1e3a5f;
    --navy-mid:  #2563eb;
    --accent:    #1d4ed8;
    --accent-lt: #dbeafe;
    --text:      #1e293b;
    --text-dim:  #64748b;
    --green:     #16a34a;
    --green-lt:  #dcfce7;
    --blue:      #2563eb;
    --blue-lt:   #dbeafe;
    --red:       #dc2626;
    --red-lt:    #fee2e2;
    --amber:     #d97706;
    --amber-lt:  #fef3c7;
    --purple:    #7c3aed;
    --purple-lt: #ede9fe;
    --border:    #cbd5e1;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'Inter', 'Segoe UI', sans-serif;
    font-size: 10pt;
    line-height: 1.65;
  }}

  /* ── Cover Page ─────────────────────────────────────────────── */
  .cover {{
    page: cover;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: flex-start;
    min-height: 100vh;
    padding: 80px 70px;
    background: linear-gradient(150deg, #0f2044 0%, #1e3a5f 60%, #1e40af 100%);
    position: relative;
  }}
  .cover::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 5px;
    background: linear-gradient(90deg, #2563eb, #60a5fa, #2563eb);
  }}
  .cover::after {{
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 3px;
    background: #2563eb;
  }}
  .cover-badge {{
    background: rgba(96,165,250,0.15);
    color: #93c5fd;
    font-size: 8pt;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    padding: 5px 14px;
    border-radius: 20px;
    border: 1px solid #3b82f6;
    display: inline-block;
    margin-bottom: 32px;
  }}
  .cover-title {{
    font-size: 36pt;
    font-weight: 700;
    color: #ffffff;
    line-height: 1.15;
    margin-bottom: 14px;
    letter-spacing: -1px;
  }}
  .cover-title span {{ color: #60a5fa; }}
  .cover-sub {{
    font-size: 12pt;
    color: #93c5fd;
    margin-bottom: 52px;
    font-weight: 300;
  }}
  .cover-meta {{
    display: flex;
    gap: 40px;
    border-top: 1px solid rgba(148,163,184,0.3);
    padding-top: 32px;
    width: 100%;
  }}
  .cover-meta-item label {{
    display: block;
    font-size: 7pt;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-bottom: 4px;
  }}
  .cover-meta-item value {{
    font-size: 10pt;
    color: #e2e8f0;
    font-weight: 500;
  }}
  .cover-logo {{
    position: absolute;
    top: 48px; right: 60px;
    font-size: 28pt;
    font-weight: 800;
    color: #60a5fa;
    letter-spacing: -2px;
  }}

  /* ── Page layout ────────────────────────────────────────────── */
  @page {{
    size: A4;
    margin: 18mm 18mm 22mm 18mm;
    background: #ffffff;
    @bottom-right {{
      content: counter(page);
      font-family: 'Inter', sans-serif;
      font-size: 8pt;
      color: #94a3b8;
    }}
    @bottom-left {{
      content: "BNCTL Data Warehouse — Deployment Guide";
      font-family: 'Inter', sans-serif;
      font-size: 8pt;
      color: #94a3b8;
    }}
  }}
  @page cover {{
    margin: 0;
    background: #0f2044;
    @bottom-right {{ content: none; }}
    @bottom-left  {{ content: none; }}
  }}

  /* ── Typography ─────────────────────────────────────────────── */
  h1 {{
    font-size: 18pt;
    font-weight: 700;
    color: var(--navy);
    border-bottom: 2px solid var(--navy-mid);
    padding-bottom: 8px;
    margin: 28px 0 16px;
    page-break-after: avoid;
  }}
  h2 {{
    font-size: 13pt;
    font-weight: 600;
    color: var(--navy);
    margin: 20px 0 10px;
    display: flex;
    align-items: center;
    gap: 10px;
    page-break-after: avoid;
  }}
  h2::before {{
    content: '';
    display: inline-block;
    width: 4px;
    height: 16px;
    background: var(--accent);
    border-radius: 2px;
    flex-shrink: 0;
  }}
  h3 {{
    font-size: 11pt;
    font-weight: 600;
    color: var(--accent);
    margin: 14px 0 8px;
    page-break-after: avoid;
  }}
  p {{ margin-bottom: 10px; color: var(--text); }}

  /* ── Code blocks ────────────────────────────────────────────── */
  .cmd-block {{
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    margin: 12px 0;
    overflow: hidden;
    page-break-inside: avoid;
  }}
  .cmd-block-header {{
    background: #0f172a;
    padding: 5px 14px;
    font-size: 7.5pt;
    color: #94a3b8;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    letter-spacing: 0.5px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .cmd-block-header::before {{
    content: '';
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #3b82f6;
  }}
  .cmd-block pre {{
    padding: 14px 16px;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 8.5pt;
    color: #e2e8f0;
    white-space: pre-wrap;
    word-break: break-all;
    line-height: 1.7;
    margin: 0;
  }}
  .cmd-block pre .comment {{ color: #64748b; }}
  .cmd-block pre .flag    {{ color: #fbbf24; }}
  .cmd-block pre .kw      {{ color: #a78bfa; }}

  code {{
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    background: #f1f5f9;
    color: #1e40af;
    padding: 1px 5px;
    border-radius: 3px;
    font-size: 8.5pt;
    border: 1px solid #e2e8f0;
  }}

  /* ── Callout boxes ──────────────────────────────────────────── */
  .callout {{
    border-radius: 8px;
    padding: 12px 16px;
    margin: 14px 0;
    border-left: 4px solid;
    page-break-inside: avoid;
  }}
  .callout-info    {{ background: var(--blue-lt);   border-color: var(--blue);   color: #1e3a8a; }}
  .callout-warn    {{ background: var(--amber-lt);  border-color: var(--amber);  color: #78350f; }}
  .callout-danger  {{ background: var(--red-lt);    border-color: var(--red);    color: #7f1d1d; }}
  .callout-success {{ background: var(--green-lt);  border-color: var(--green);  color: #14532d; }}
  .callout-title   {{ font-weight: 700; font-size: 9pt; margin-bottom: 5px; display: block; }}
  .callout ul      {{ margin: 6px 0 0 16px; }}
  .callout li      {{ margin-bottom: 4px; }}

  /* ── Tables ─────────────────────────────────────────────────── */
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 14px 0;
    font-size: 9pt;
    page-break-inside: avoid;
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
  }}
  th {{
    background: var(--navy);
    color: #ffffff;
    padding: 9px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 8.5pt;
    letter-spacing: 0.3px;
  }}
  td {{
    padding: 8px 12px;
    border-bottom: 1px solid var(--bg3);
    vertical-align: top;
    color: var(--text);
  }}
  tr:nth-child(even) td {{ background: var(--bg2); }}

  /* ── Step badge ─────────────────────────────────────────────── */
  .step-badge {{
    display: inline-block;
    background: var(--navy);
    color: #ffffff;
    border-radius: 12px;
    padding: 2px 10px;
    font-size: 8pt;
    font-weight: 700;
    margin-right: 6px;
    font-family: 'JetBrains Mono', monospace;
  }}
  .step-badge-blue   {{ background: var(--blue);   color:#fff; }}
  .step-badge-green  {{ background: var(--green);  color:#fff; }}
  .step-badge-purple {{ background: var(--purple); color:#fff; }}
  .step-badge-red    {{ background: var(--red);    color:#fff; }}

  /* ── Secret group cards ─────────────────────────────────────── */
  .secret-group {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 16px;
    margin: 14px 0;
    page-break-inside: avoid;
  }}
  .secret-group-title {{
    font-weight: 700;
    color: var(--navy);
    font-size: 10pt;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .propagate-note {{
    background: var(--green-lt);
    border: 1px solid #86efac;
    border-radius: 6px;
    padding: 8px 12px;
    margin-top: 10px;
    font-size: 8.5pt;
    color: #14532d;
  }}
  .propagate-note strong {{ color: var(--green); }}

  /* ── Architecture diagram ───────────────────────────────────── */
  .svg-wrap {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    margin: 16px 0;
    text-align: center;
  }}

  /* ── Misc ───────────────────────────────────────────────────── */
  .section {{ page-break-before: always; }}
  .section:first-of-type {{ page-break-before: avoid; }}
  ul {{ margin: 8px 0 8px 20px; }}
  li {{ margin-bottom: 5px; color: var(--text); }}
  .text-dim {{ color: var(--text-dim); font-size: 9pt; }}
  .tag {{
    display: inline-block;
    padding: 1px 8px;
    border-radius: 10px;
    font-size: 7.5pt;
    font-weight: 600;
    margin-left: 6px;
  }}
  .tag-helm     {{ background: var(--blue-lt);   color: var(--blue);   border: 1px solid #93c5fd; }}
  .tag-manifest {{ background: var(--purple-lt); color: var(--purple); border: 1px solid #c4b5fd; }}
  .tag-auto     {{ background: var(--green-lt);  color: var(--green);  border: 1px solid #86efac; }}
  hr {{ border: none; border-top: 1px solid var(--border); margin: 20px 0; }}
</style>
</head>
<body>

<!-- ╔══════════════════════════════╗ -->
<!-- ║         COVER PAGE           ║ -->
<!-- ╚══════════════════════════════╝ -->
<div class="cover">
  <div class="cover-logo">BNCTL</div>
  <div class="cover-badge">Tài liệu kỹ thuật nội bộ</div>
  <div class="cover-title">Data Warehouse<br/><span>Deployment Guide</span></div>
  <div class="cover-sub">Hướng dẫn triển khai hạ tầng từ đầu — Apache Airflow · dbt · Spark · Kafka · MinIO · ELK</div>
  <div class="cover-meta">
    <div class="cover-meta-item">
      <label>Phiên bản</label>
      <value>v2.0</value>
    </div>
    <div class="cover-meta-item">
      <label>Airflow</label>
      <value>3.x · KubernetesExecutor</value>
    </div>
    <div class="cover-meta-item">
      <label>Môi trường</label>
      <value>Kubernetes (development)</value>
    </div>
    <div class="cover-meta-item">
      <label>Phân loại</label>
      <value>Internal · Restricted</value>
    </div>
  </div>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 1 — ARCHITECTURE                                       -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>1. Kiến trúc hệ thống</h1>

<h2>1.1 Sơ đồ tổng quan</h2>
<div class="svg-wrap">
  {ARCH_IMG}
</div>

<h2>1.2 Mô tả luồng dữ liệu</h2>
<table>
  <tr><th>Layer</th><th>Công nghệ</th><th>Mô tả</th></tr>
  <tr>
    <td><strong>Source</strong></td>
    <td>SQL Server 2022</td>
    <td>39 bảng T24 core banking — nguồn dữ liệu gốc</td>
  </tr>
  <tr>
    <td><strong>CDC</strong></td>
    <td>Kafka KRaft · Debezium</td>
    <td>Capture thay đổi real-time từ MSSQL → Kafka topics → MinIO</td>
  </tr>
  <tr>
    <td><strong>Orchestration</strong></td>
    <td>Airflow 3.x · Jenkins</td>
    <td>Airflow trigger Spark jobs qua K8s CRD; Jenkins CI/CD build image và push deploy key</td>
  </tr>
  <tr>
    <td><strong>Processing</strong></td>
    <td>Spark 3.5.1 · dbt</td>
    <td>Spark parse raw files → Hive Iceberg; dbt transform → Dremio gold tables</td>
  </tr>
  <tr>
    <td><strong>Storage</strong></td>
    <td>MinIO · Hive · PostgreSQL · Redis · Dremio</td>
    <td>MinIO = S3-compatible raw store; Hive = Iceberg catalog; PG = ETL state; Redis = Celery; Dremio = SQL engine</td>
  </tr>
  <tr>
    <td><strong>Observability</strong></td>
    <td>ELK Stack</td>
    <td>Elasticsearch + Kibana + Logstash thu thập logs Spark jobs từ MinIO</td>
  </tr>
  <tr>
    <td><strong>Reporting</strong></td>
    <td>Power BI PBIRS</td>
    <td>5 domain báo cáo: Accounting, Credit, AML, Operational, Treasury</td>
  </tr>
</table>

<h2>1.3 Services &amp; Images</h2>
<table>
  <tr><th>Service</th><th>Namespace</th><th>Helm Chart / Version</th><th>Image</th></tr>
  <tr>
    <td>Airflow</td>
    <td><code>bnctl-airflow-development-ns</code></td>
    <td><code>apache-airflow/airflow 1.18.0</code></td>
    <td><code>haiptjits/dwh-test:airflow-build-185</code> (CI/CD override)</td>
  </tr>
  <tr>
    <td>MinIO</td>
    <td><code>bnctl-minio-development-ns</code></td>
    <td><code>minio/minio 5.1.0</code></td>
    <td><code>quay.io/minio/minio:RELEASE.2024-03-03T17-50-39Z</code></td>
  </tr>
  <tr>
    <td>PostgreSQL</td>
    <td><code>bnctl-postgres-development-ns</code></td>
    <td><code>bitnami/postgresql 18.0.11</code></td>
    <td><code>bitnami/postgresql:latest</code></td>
  </tr>
  <tr>
    <td>Redis</td>
    <td><code>bnctl-redis-development-ns</code></td>
    <td><code>bitnami/redis 23.1.3</code></td>
    <td><code>bitnami/redis:latest</code></td>
  </tr>
  <tr>
    <td>Hive Metastore</td>
    <td><code>bnctl-hive-development-ns</code></td>
    <td>kubectl apply (raw manifest)</td>
    <td><code>tranconggg/hive-metastore:v2</code></td>
  </tr>
  <tr>
    <td>Dremio</td>
    <td><code>bnctl-dremio-development-ns</code></td>
    <td><code>bitnami/dremio 3.0.13</code></td>
    <td><code>bitnamilegacy/dremio:26.0.0-debian-12-r5</code></td>
  </tr>
  <tr>
    <td>Kafka (Strimzi)</td>
    <td><code>bnctl-kafka-development-ns</code></td>
    <td><code>strimzi/strimzi-kafka-operator 0.45.0</code></td>
    <td><code>quay.io/strimzi/operator:0.45.0</code></td>
  </tr>
  <tr>
    <td>MSSQL</td>
    <td><code>bnctl-kafka-development-ns</code></td>
    <td>raw manifest</td>
    <td><code>mcr.microsoft.com/mssql/server:2022-latest</code></td>
  </tr>
  <tr>
    <td>Spark Operator</td>
    <td><code>spark-operator</code></td>
    <td><code>spark-operator/spark-operator 2.5.0</code></td>
    <td><code>kubeflow/spark-operator/controller:2.5.0</code></td>
  </tr>
  <tr>
    <td>Jenkins</td>
    <td><code>bnctl-jenkins-development-ns</code></td>
    <td><code>jenkins/jenkins 5.8.93</code></td>
    <td><code>jenkins/jenkins:2.516.3-jdk21</code></td>
  </tr>
  <tr>
    <td>Elasticsearch</td>
    <td><code>bnctl-elk-development-ns</code></td>
    <td><code>elastic/elasticsearch 7.17.3</code></td>
    <td>—</td>
  </tr>
  <tr>
    <td>Kibana</td>
    <td><code>bnctl-elk-development-ns</code></td>
    <td><code>elastic/kibana 7.17.3</code></td>
    <td>—</td>
  </tr>
  <tr>
    <td>Logstash</td>
    <td><code>bnctl-elk-development-ns</code></td>
    <td><code>elastic/logstash 7.17.3</code></td>
    <td>—</td>
  </tr>
</table>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 2 — PREREQUISITES                                      -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>2. Yêu cầu trước khi deploy</h1>

<h2>2.1 Công cụ cần cài đặt</h2>
<table>
  <tr><th>Tool</th><th>Version tối thiểu</th><th>Kiểm tra</th></tr>
  <tr><td><code>kubectl</code></td><td>1.28+</td><td><code>kubectl version --client</code></td></tr>
  <tr><td><code>helm</code></td><td>3.12+</td><td><code>helm version</code></td></tr>
  <tr><td><code>python3</code></td><td>3.9+</td><td><code>python3 --version</code></td></tr>
  <tr><td><code>openssl</code></td><td>bất kỳ</td><td><code>openssl version</code></td></tr>
  <tr><td><code>base64</code></td><td>GNU coreutils</td><td><code>base64 --version</code></td></tr>
</table>

<h2>2.2 StorageClass — Longhorn</h2>
<p>Toàn bộ PVC dùng StorageClass <strong>longhorn</strong> (default). Kiểm tra trước khi deploy:</p>
<div class="cmd-block">
  <div class="cmd-block-header">bash — kiểm tra</div>
  <pre><span class="comment"># Phải thấy longhorn với cột DEFAULT = true</span>
kubectl get storageclass
<span class="comment"># NAME                 PROVISIONER          RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION</span>
<span class="comment"># longhorn (default)   driver.longhorn.io   Delete          Immediate           true</span></pre>
</div>

<div class="callout callout-warn">
  <span class="callout-title">Cluster mới / khác</span>
  Nếu <code>kubectl get storageclass</code> không thấy longhorn → cài trước khi chạy <code>deploy.sh</code>.
</div>

<h3>Cài Longhorn trên cluster mới</h3>
<p>Longhorn yêu cầu <code>open-iscsi</code> trên tất cả worker nodes:</p>
<div class="cmd-block">
  <div class="cmd-block-header">bash — prerequisites (chạy trên từng node)</div>
  <pre><span class="comment"># Ubuntu/Debian</span>
apt-get install -y open-iscsi nfs-common

<span class="comment"># RHEL/CentOS</span>
yum install -y iscsi-initiator-utils nfs-utils
systemctl enable --now iscsid</pre>
</div>

<div class="cmd-block">
  <div class="cmd-block-header">bash — cài Longhorn v1.7.0</div>
  <pre><span class="comment"># Cài bằng manifest</span>
kubectl apply -f https://raw.githubusercontent.com/longhorn/longhorn/v1.7.0/deploy/longhorn.yaml

<span class="comment"># Chờ tất cả pods ready (~3-5 phút)</span>
kubectl wait pod --all -n longhorn-system \
  --for=condition=ready --timeout=300s

<span class="comment"># Set làm default StorageClass</span>
kubectl patch storageclass longhorn \
  -p '{{"metadata":{{"annotations":{{"storageclass.kubernetes.io/is-default-class":"true"}}}}}}'

<span class="comment"># Verify</span>
kubectl get storageclass</pre>
</div>

<div class="callout callout-info">
  <span class="callout-title">Longhorn UI</span>
  Sau khi cài, truy cập Longhorn UI để kiểm tra disk và replica health:<br/>
  <code>kubectl port-forward svc/longhorn-frontend 8080:80 -n longhorn-system</code><br/>
  → <code>http://localhost:8080</code>
</div>

<div class="callout callout-info">
  <span class="callout-title">Python dependency</span>
  Script tự động generate Fernet key bằng thư viện <code>cryptography</code>.
  Nếu không có → fallback về <code>openssl rand -base64 32</code> (vẫn hợp lệ nhưng không phải Fernet format chuẩn — hãy generate bằng <code>python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"</code> trước).
</div>

<h2>2.3 Kết nối cluster</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — kiểm tra cluster</div>
  <pre><span class="comment"># Verify kubectl trỏ đúng context</span>
kubectl config current-context
kubectl cluster-info

<span class="comment"># Cần quyền create namespace, secret, deploy helm chart</span>
kubectl auth can-i create namespace
kubectl auth can-i create secret --all-namespaces</pre>
</div>

<h2>2.4 Files cần chuẩn bị</h2>
<table>
  <tr><th>File</th><th>Mô tả</th><th>Dùng ở bước</th></tr>
  <tr>
    <td><code>~/.ssh/airflow_deploy_key</code></td>
    <td>SSH deploy key để git-sync pull DAGs từ GitLab</td>
    <td>[4/7] Airflow — SSH key</td>
  </tr>
  <tr>
    <td><code>~/.docker/config.json</code></td>
    <td>Docker Hub credentials (pull private images)</td>
    <td>[4/7] Airflow — DockerHub</td>
  </tr>
  <tr>
    <td><code>deployment/postgres/init-dwh-databases.sql</code></td>
    <td>SQL tạo databases, roles, và permissions cho DWH</td>
    <td>[2/10] PostgreSQL init</td>
  </tr>
</table>

<div class="callout callout-warn">
  <span class="callout-title">SSH key phải được add vào GitLab trước</span>
  GitLab → Settings → Deploy Keys → Add key. Airflow git-sync sẽ fail nếu chưa có.
</div>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 3 — DEPLOY SCRIPT                                      -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>3. Script deploy.sh</h1>

<h2>3.1 Cách chạy</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — syntax cơ bản</div>
  <pre><span class="comment"># Clone/cd vào thư mục deployment</span>
cd /path/to/airflow_orchestration/deployment

<span class="comment"># Fresh cluster — nhập secrets, deploy bằng Helm</span>
bash deploy.sh helm

<span class="comment"># Redeploy — secrets đã tồn tại, chỉ deploy lại workloads</span>
bash deploy.sh helm <span class="flag">--skip-secrets</span>

<span class="comment"># Restore workloads bằng raw manifests (cluster đã có RBAC)</span>
bash deploy.sh manifest <span class="flag">--skip-secrets</span></pre>
</div>

<h2>3.2 Hai chế độ deploy</h2>
<table>
  <tr><th>Mode</th><th>Dùng khi nào</th><th>Lưu ý</th></tr>
  <tr>
    <td><span class="step-badge step-badge-blue">helm</span></td>
    <td>Fresh cluster, lần đầu deploy</td>
    <td>Tạo đầy đủ CRD, RBAC, ServiceAccount, Helm hooks (db-migrate, create-user)</td>
  </tr>
  <tr>
    <td><span class="step-badge step-badge-purple">manifest</span></td>
    <td>Restore workloads trên cluster đã có RBAC</td>
    <td>Chỉ apply Deployment/StatefulSet/Service/ConfigMap — không có SA/RBAC/Helm hooks → Airflow sẽ fail nếu dùng cho fresh cluster</td>
  </tr>
</table>

<div class="callout callout-danger">
  <span class="callout-title">Manifest mode KHÔNG phải fresh install</span>
  <code>manifest</code> mode thiếu ServiceAccounts, ClusterRole/RoleBinding (RBAC), và Airflow init Jobs (db-migrate, create-user).
  Pod sẽ fail do thiếu serviceAccount. Chỉ dùng để restore workload trên cluster đã setup đầy đủ RBAC.
</div>

<div class="callout callout-warn">
  <span class="callout-title">--skip-secrets và Hive ConfigMap</span>
  Khi dùng <code>--skip-secrets</code>, script bỏ qua bước <code>patch_hive_minio_config()</code>.
  Nếu MinIO credentials thay đổi, hãy patch thủ công ConfigMap <code>metastore-cfg</code>
  trong namespace <code>bnctl-hive-development-ns</code> và restart Hive Metastore.
</div>

<h2>3.3 Kiểm tra prerequisites</h2>
<p>Script tự động kiểm tra trước khi bắt đầu:</p>
<ul>
  <li><code>kubectl</code> — phải có trong PATH</li>
  <li><code>helm</code> — chỉ khi mode là <code>helm</code></li>
  <li>Kết nối cluster — <code>kubectl cluster-info</code> phải thành công</li>
  <li>Namespaces — tự tạo 11 namespaces (idempotent, an toàn nếu đã tồn tại)</li>
</ul>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 4 — SECRET COLLECTION                                  -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>4. Thu thập secrets (Interactive)</h1>

<h2>4.1 Tổng quan cơ chế</h2>
<p>Script hỏi credentials <strong>trực tiếp trong terminal</strong> trước khi deploy. Tất cả passwords được nhập ẩn (không hiển thị), file paths được nhập văn bản thường.</p>

<div class="callout callout-info">
  <span class="callout-title">Tips nhập liệu</span>
  <ul>
    <li><strong>Passwords/secrets:</strong> ẩn hoàn toàn (dùng <code>read -rsp</code>)</li>
    <li><strong>Usernames/hosts/paths:</strong> hiển thị khi gõ (dùng <code>read -rp</code>)</li>
    <li><strong>Enter để bỏ qua:</strong> secret đó phải đã tồn tại trên cluster</li>
    <li><strong>Fernet key / API secret / JWT secret:</strong> Enter → tự động generate an toàn</li>
    <li><strong>MinIO credentials:</strong> nhập 1 lần → tự propagate cho Airflow + Spark + ELK</li>
    <li><strong>PostgreSQL password:</strong> nhập 1 lần → dùng default cho Airflow DB và Hive DB</li>
  </ul>
</div>

<h2>4.2 7 nhóm credentials</h2>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">1/7</span> MinIO
    <span class="tag tag-auto">auto-propagate</span>
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Namespace đích</th><th>Secret name</th></tr>
    <tr>
      <td><code>rootUser</code></td>
      <td>Text (default: <code>minioadmin</code>)</td>
      <td><code>bnctl-minio-development-ns</code></td>
      <td><code>minio</code></td>
    </tr>
    <tr>
      <td><code>rootPassword</code></td>
      <td>Password (ẩn)</td>
      <td><code>bnctl-minio-development-ns</code></td>
      <td><code>minio</code></td>
    </tr>
  </table>
  <div class="propagate-note">
    <strong>Auto-propagate từ rootUser + rootPassword:</strong><br/>
    → <code>airflow-minio-conn</code> (bnctl-airflow-development-ns) — AWS connection string<br/>
    → <code>minio-credentials</code> (bnctl-spark2-development-ns) — keys: <code>AWS_ACCESS_KEY_ID</code> / <code>AWS_SECRET_ACCESS_KEY</code><br/>
    → <code>minio-credentials</code> (bnctl-elk-development-ns) — keys: <code>access_key</code> / <code>secret_key</code><br/>
    → <code>metastore-cfg</code> ConfigMap (bnctl-hive-development-ns) — patch <code>fs.s3a.access.key</code> / <code>fs.s3a.secret.key</code>
  </div>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">2/7</span> PostgreSQL
    <span class="tag tag-auto">auto-propagate</span>
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>postgres-password (superuser)</td>
      <td>Password (ẩn)</td>
      <td><code>cluster-postgresql</code> → bnctl-postgres-development-ns</td>
    </tr>
    <tr>
      <td>Airflow DB password (role <code>airflow</code>)</td>
      <td>Password — Enter = dùng postgres-password</td>
      <td><code>airflow-metadata</code> → bnctl-airflow-development-ns (connection string)</td>
    </tr>
    <tr>
      <td>PostgreSQL host</td>
      <td>Text (default: <code>cluster-postgresql-primary.bnctl-postgres-development-ns</code>)</td>
      <td>Dùng trong connection string Airflow metadata</td>
    </tr>
    <tr>
      <td>hive_user DB password (role <code>hive_user</code>)</td>
      <td>Password — Enter = dùng postgres-password</td>
      <td><code>hive-metastore-secret</code> → bnctl-hive-development-ns (JDBC URL)</td>
    </tr>
  </table>
  <div class="propagate-note">
    <strong>Auto-propagate:</strong><br/>
    → <code>hive-metastore-secret</code> (bnctl-hive-development-ns) — <code>metastore-db-url</code> / <code>metastore-db-user</code> / <code>metastore-db-password</code>
  </div>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">3/7</span> Redis
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>redis-password</td>
      <td>Password (ẩn)</td>
      <td><code>redis-ha</code> → bnctl-redis-development-ns</td>
    </tr>
  </table>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">4/7</span> Airflow — keys &amp; files
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>fernet-key</td>
      <td>Password — Enter = <strong>auto-generate</strong></td>
      <td><code>airflow-fernet-key</code> → bnctl-airflow-development-ns</td>
    </tr>
    <tr>
      <td>api-secret-key</td>
      <td>Password — Enter = <strong>auto-generate</strong></td>
      <td><code>airflow-api-secret-key</code> → bnctl-airflow-development-ns</td>
    </tr>
    <tr>
      <td>jwt-secret</td>
      <td>Password — Enter = <strong>auto-generate</strong></td>
      <td><code>airflow-jwt-secret</code> → bnctl-airflow-development-ns</td>
    </tr>
    <tr>
      <td>Path SSH deploy key</td>
      <td>Text (path tới file <code>~/.ssh/airflow_deploy_key</code>)</td>
      <td><code>airflow-ssh-secret</code> → bnctl-airflow-development-ns</td>
    </tr>
    <tr>
      <td>Path Docker Hub config.json</td>
      <td>Text (path tới <code>~/.docker/config.json</code>)</td>
      <td><code>dockerhub-credentials</code> → bnctl-airflow-development-ns</td>
    </tr>
  </table>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">5/7</span> Kafka / MSSQL
    <span class="tag tag-auto">auto-propagate</span>
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>MSSQL SA password</td>
      <td>Password (ẩn) — min 8 ký tự, chữ hoa+thường+số+ký tự đặc biệt</td>
      <td><code>mssql-sa-credentials</code> → bnctl-kafka-development-ns</td>
    </tr>
    <tr>
      <td>Debezium connector password</td>
      <td>Password — Enter = dùng SA password</td>
      <td><code>mssql-credentials</code> → bnctl-kafka-development-ns</td>
    </tr>
  </table>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">6/7</span> Dremio
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>dremio-admin-password</td>
      <td>Password (ẩn)</td>
      <td><code>cluster-dremio</code> → bnctl-dremio-development-ns</td>
    </tr>
  </table>
</div>

<div class="secret-group">
  <div class="secret-group-title">
    <span class="step-badge">7/7</span> Jenkins
  </div>
  <table>
    <tr><th>Field</th><th>Type</th><th>Secret tạo ra</th></tr>
    <tr>
      <td>GitLab Personal Access Token</td>
      <td>Password (ẩn)</td>
      <td><code>jenkins-casc-secrets</code> → bnctl-jenkins-development-ns (key: GITLAB_API_TOKEN)</td>
    </tr>
    <tr>
      <td>Path SSH deploy key (Jenkins → Airflow)</td>
      <td>Text (đường dẫn tới file)</td>
      <td><code>jenkins-casc-secrets</code> (key: AIRFLOW_SSH_KEY) — nội dung file được nhúng</td>
    </tr>
    <tr>
      <td>KUBECONFIG (auto)</td>
      <td><em>Tự động lấy từ <code>kubectl config view --raw | base64</code></em></td>
      <td><code>jenkins-casc-secrets</code> (key: KUBECONFIG_B64)</td>
    </tr>
  </table>
</div>

<h2>4.3 Cơ chế idempotent (apply_secret)</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — apply_secret() helper</div>
  <pre>apply_secret() {{
  <span class="kw">local</span> ns=$1 name=$2; <span class="kw">shift</span> 2
  kubectl create secret generic "$name" "$@" \
    -n "$ns" <span class="flag">--dry-run=client -o yaml</span> 2>/dev/null | kubectl apply -f - >/dev/null 2>&1 \
    && info "  ✓ secret/$name → $ns" \
    || warn "  ✗ secret/$name FAILED"
}}</pre>
</div>
<p>Dùng <code>--dry-run=client -o yaml | kubectl apply</code> thay vì <code>kubectl create</code> trực tiếp → an toàn khi chạy lại (update nếu đã tồn tại, không fail).</p>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 5 — DEPLOYMENT ORDER                                   -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>5. Thứ tự triển khai</h1>

<h2>5.1 10 bước deploy</h2>

<table>
  <tr>
    <th style="width:60px">Bước</th>
    <th>Service</th>
    <th>Namespace</th>
    <th>Helm chart / version</th>
    <th>Chờ</th>
  </tr>
  <tr>
    <td><span class="step-badge">1/10</span></td>
    <td>MinIO</td>
    <td><code>bnctl-minio-development-ns</code></td>
    <td><code>minio/minio 5.1.0</code></td>
    <td>StatefulSet <code>minio</code> · 300s</td>
  </tr>
  <tr>
    <td><span class="step-badge">2/10</span></td>
    <td>PostgreSQL + DWH init</td>
    <td><code>bnctl-postgres-development-ns</code></td>
    <td><code>bitnami/postgresql 18.0.11</code></td>
    <td>StatefulSet <code>cluster-postgresql-primary</code> · 300s; sau đó chạy init SQL</td>
  </tr>
  <tr>
    <td><span class="step-badge">3/10</span></td>
    <td>Redis</td>
    <td><code>bnctl-redis-development-ns</code></td>
    <td><code>bitnami/redis 23.1.3</code></td>
    <td>StatefulSet <code>redis-ha-node</code> · 180s</td>
  </tr>
  <tr>
    <td><span class="step-badge">4/10</span></td>
    <td>Hive Metastore + patch MinIO config</td>
    <td><code>bnctl-hive-development-ns</code></td>
    <td>kubectl apply (manifests)</td>
    <td>Deployment <code>hive-metastore</code> · 180s → <strong>patch_hive_minio_config</strong> → restart</td>
  </tr>
  <tr>
    <td><span class="step-badge">5/10</span></td>
    <td>Dremio</td>
    <td><code>bnctl-dremio-development-ns</code></td>
    <td><code>bitnami/dremio 3.0.13</code></td>
    <td>StatefulSet <code>cluster-dremio-master-coordinator</code> · 300s</td>
  </tr>
  <tr>
    <td><span class="step-badge">6/10</span></td>
    <td>Kafka (Strimzi) + MSSQL + Debezium</td>
    <td><code>bnctl-kafka-development-ns</code></td>
    <td><code>strimzi/strimzi-kafka-operator 0.45.0</code></td>
    <td>Operator deploy → Kafka CR → MSSQL 180s → Kafka brokers 300s → Debezium Connect CR</td>
  </tr>
  <tr>
    <td><span class="step-badge">7/10</span></td>
    <td>Spark Operator</td>
    <td><code>spark-operator</code></td>
    <td><code>spark-operator/spark-operator 2.5.0</code></td>
    <td>Deployment <code>spark-operator-controller</code> · 120s</td>
  </tr>
  <tr>
    <td><span class="step-badge">8/10</span></td>
    <td>Airflow + image patch + ConfigMaps</td>
    <td><code>bnctl-airflow-development-ns</code></td>
    <td><code>apache-airflow/airflow 1.18.0</code></td>
    <td>Deployment <code>airflow-scheduler</code> · 600s; sau đó patch image + apply ConfigMaps + restart</td>
  </tr>
  <tr>
    <td><span class="step-badge">9/10</span></td>
    <td>Jenkins</td>
    <td><code>bnctl-jenkins-development-ns</code></td>
    <td><code>jenkins/jenkins 5.8.93</code></td>
    <td>StatefulSet <code>jenkins</code> · 300s</td>
  </tr>
  <tr>
    <td><span class="step-badge">10/10</span></td>
    <td>ELK (Elasticsearch + Kibana + Logstash)</td>
    <td><code>bnctl-elk-development-ns</code></td>
    <td><code>elastic/* 7.17.3</code></td>
    <td>Elasticsearch pods ready · 300s; sau đó Kibana + Logstash</td>
  </tr>
</table>

<h2>5.2 Hive MinIO auto-patch (bước 4)</h2>
<p>Sau khi Hive Metastore ready, script tự động patch <code>fs.s3a.access.key</code> và <code>fs.s3a.secret.key</code> trong ConfigMap <code>metastore-cfg</code>:</p>

<div class="cmd-block">
  <div class="cmd-block-header">bash — patch_hive_minio_config() — logic</div>
  <pre><span class="comment"># 1. Get ConfigMap JSON từ cluster</span>
kubectl get configmap metastore-cfg \
  -n bnctl-hive-development-ns -o json

<span class="comment"># 2. Python regex replace fs.s3a.* values trong core-site.xml</span>
<span class="comment"># 3. kubectl apply để update</span>
<span class="comment"># 4. Restart hive-metastore deployment</span>
kubectl rollout restart deployment/hive-metastore \
  -n bnctl-hive-development-ns</pre>
</div>

<div class="callout callout-success">
  <span class="callout-title">Auto-propagation tóm tắt</span>
  <ul>
    <li><strong>MinIO creds →</strong> Airflow (S3 connection) + Spark (AWS keys) + ELK (Logstash S3 input) + Hive ConfigMap (core-site.xml)</li>
    <li><strong>PG password →</strong> Airflow metadata connection + Hive JDBC URL (nếu để trống = dùng PG password)</li>
    <li><strong>MSSQL SA password →</strong> Debezium connector password (nếu để trống = dùng SA password)</li>
  </ul>
</div>

<h2>5.3 Airflow image patch (bước 8)</h2>
<p>Sau khi Helm install Airflow, script patch custom image <code>haiptjits/dwh-test:airflow-build-185</code> lên tất cả components:</p>
<div class="cmd-block">
  <div class="cmd-block-header">bash — set image</div>
  <pre>AIRFLOW_IMAGE="haiptjits/dwh-test:airflow-build-185"

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
  -n bnctl-airflow-development-ns</pre>
</div>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 6 — CROSS-SERVICE CONNECTIONS                          -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>6. Cross-Service Connection Map</h1>
<p>Toàn bộ kết nối giữa các service — endpoint đã verify trên cluster:</p>

<table>
  <tr><th>Từ</th><th>Đến</th><th>Endpoint</th><th>Ghi chú</th></tr>
  <tr>
    <td>Hive Metastore</td>
    <td>PostgreSQL</td>
    <td><code>cluster-postgresql-primary.bnctl-postgres-development-ns:5432/hive_metastore</code></td>
    <td>user: <code>hive_user</code></td>
  </tr>
  <tr>
    <td>Hive Metastore</td>
    <td>MinIO</td>
    <td><code>http://minio.bnctl-minio-development-ns:9000</code></td>
    <td>bucket: <code>hive-warehouse</code></td>
  </tr>
  <tr>
    <td>Dremio</td>
    <td>MinIO</td>
    <td><code>http://minio.bnctl-minio-development-ns:9000</code></td>
    <td>access key: <code>minioadmin</code></td>
  </tr>
  <tr>
    <td>Dremio</td>
    <td>Hive</td>
    <td><code>thrift://hive-metastore.bnctl-hive-development-ns:9083</code></td>
    <td>cấu hình thủ công qua Dremio UI (xem mục 7.2)</td>
  </tr>
  <tr>
    <td>Airflow</td>
    <td>PostgreSQL</td>
    <td><code>cluster-postgresql-primary.bnctl-postgres-development-ns:5432/airflow</code></td>
    <td>metadata DB — secret <code>airflow-metadata</code></td>
  </tr>
  <tr>
    <td>Airflow</td>
    <td>MinIO</td>
    <td><code>http://minio-svc.bnctl-minio-development-ns:9000</code></td>
    <td>conn id: <code>minio_conn</code> — secret <code>airflow-minio-conn</code></td>
  </tr>
  <tr>
    <td>Airflow</td>
    <td>Redis</td>
    <td><code>redis-ha-master.bnctl-redis-development-ns:6379</code></td>
    <td>Celery broker — secret <code>redis-ha</code></td>
  </tr>
  <tr>
    <td>Spark jobs</td>
    <td>Hive</td>
    <td><code>thrift://hive-metastore.bnctl-hive-development-ns:9083</code></td>
    <td>Iceberg catalog</td>
  </tr>
  <tr>
    <td>Spark jobs</td>
    <td>MinIO</td>
    <td><code>http://minio.bnctl-minio-development-ns:9000</code></td>
    <td>S3A filesystem — secret <code>minio-credentials</code> (spark2 ns)</td>
  </tr>
  <tr>
    <td>Debezium</td>
    <td>MSSQL</td>
    <td><code>mssql.bnctl-kafka-development-ns:1433/testdb</code></td>
    <td>CDC source — secret <code>mssql-credentials</code></td>
  </tr>
  <tr>
    <td>Debezium</td>
    <td>Kafka</td>
    <td><code>cdc-kafka-bootstrap:9092</code></td>
    <td>schema history topic: <code>schema-changes.t24</code></td>
  </tr>
  <tr>
    <td>Logstash</td>
    <td>MinIO</td>
    <td><code>http://minio-svc.bnctl-minio-development-ns:9000</code></td>
    <td>bucket: <code>airflow-logs</code> — secret <code>minio-credentials</code> (elk ns, keys: <code>access_key</code>/<code>secret_key</code>)</td>
  </tr>
  <tr>
    <td>Logstash</td>
    <td>Elasticsearch</td>
    <td><code>http://elasticsearch-master:9200</code></td>
    <td>index: <code>airflow-logs-*</code> (same namespace)</td>
  </tr>
  <tr>
    <td>Kibana</td>
    <td>Elasticsearch</td>
    <td><code>http://elasticsearch-master:9200</code></td>
    <td>same namespace</td>
  </tr>
  <tr>
    <td>Jenkins</td>
    <td>Kubernetes</td>
    <td><code>https://kubernetes.default</code> (in-cluster)</td>
    <td>agent namespace: <code>bnctl-jenkins-development-ns</code>, image: <code>btthanhk4/inbound-agent:1.0.1</code></td>
  </tr>
  <tr>
    <td>Jenkins</td>
    <td>GitLab</td>
    <td>via <code>GITLAB_API_TOKEN</code></td>
    <td>secret <code>jenkins-casc-secrets</code></td>
  </tr>
</table>

<div class="callout callout-info">
  <span class="callout-title">Lưu ý PVC</span>
  Tất cả PVC dùng StorageClass <strong>longhorn</strong>. Data không được backup trong repo — storage phải có sẵn trên cluster mới trước khi deploy.
</div>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 7 — POST-DEPLOY                                        -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>7. Post-Deploy — Các bước bắt buộc</h1>

<h2>7.1 Kiểm tra nhanh — tất cả pods</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — verify pods</div>
  <pre><span class="comment"># Xem pods bị lỗi (không phải Running/Completed)</span>
kubectl get pods -A | grep -v "Completed" | grep -v "Running" | grep -v "kube-system"

<span class="comment"># Events lỗi của pod cụ thể</span>
kubectl describe pod &lt;pod-name&gt; -n &lt;namespace&gt; | tail -30
kubectl logs &lt;pod-name&gt; -n &lt;namespace&gt; --tail=50</pre>
</div>

<h2>7.2 Verify — Port-forward các UI</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — port-forward</div>
  <pre><span class="comment"># Airflow UI → http://localhost:8080</span>
kubectl port-forward svc/airflow-api-server 8080:8080 \
  -n bnctl-airflow-development-ns

<span class="comment"># MinIO UI → http://localhost:9001</span>
kubectl port-forward svc/minio-console 9001:9001 \
  -n bnctl-minio-development-ns

<span class="comment"># Dremio UI → http://localhost:9047</span>
kubectl port-forward svc/cluster-dremio 9047:9047 \
  -n bnctl-dremio-development-ns

<span class="comment"># Kafka UI → http://localhost:8080</span>
kubectl port-forward svc/kafka-ui 8080:80 \
  -n bnctl-kafka-development-ns

<span class="comment"># Kibana → http://localhost:5601</span>
kubectl port-forward svc/kibana-kibana 5601:5601 \
  -n bnctl-elk-development-ns

<span class="comment"># Jenkins → http://localhost:8090</span>
kubectl port-forward svc/jenkins 8090:8080 \
  -n bnctl-jenkins-development-ns</pre>
</div>

<div class="cmd-block">
  <div class="cmd-block-header">bash — verify DAGs đã load</div>
  <pre>POD=$(kubectl get pods -n bnctl-airflow-development-ns \
  -l component=dag-processor -o jsonpath="{{.items[0].metadata.name}}")
kubectl exec -n bnctl-airflow-development-ns $POD \
  -c dag-processor -- ls /opt/airflow/dags/repo/dags/</pre>
</div>

<h2>7.3 Cấu hình Dremio → Hive Source</h2>
<div class="callout callout-warn">
  <span class="callout-title">Bắt buộc — không tự động</span>
  Dremio không đọc Hive config từ <code>dremio.conf</code> — phải add source thủ công qua UI.
</div>
<ol style="margin:10px 0 10px 20px">
  <li>Vào Dremio UI: <code>http://dremio-dwh.bnctl.develop</code> (hoặc port-forward <code>9047</code>)</li>
  <li><strong>Add Source</strong> → chọn <strong>Hive 2.x / 3.x</strong></li>
  <li>Điền:
    <ul>
      <li><strong>Hive Metastore Host:</strong> <code>hive-metastore.bnctl-hive-development-ns.svc.cluster.local</code></li>
      <li><strong>Port:</strong> <code>9083</code></li>
    </ul>
  </li>
  <li>Tab <strong>Storage</strong> → bật <strong>Enable S3-compatible storage</strong> → điền:
    <ul>
      <li><strong>Endpoint:</strong> <code>http://minio.bnctl-minio-development-ns.svc.cluster.local:9000</code></li>
      <li><strong>Access Key:</strong> <code>minioadmin</code></li>
      <li><strong>Secret Key:</strong> (MinIO password đã set lúc deploy)</li>
      <li><strong>Path-style access:</strong> ✅ bật</li>
    </ul>
  </li>
  <li>Save → Dremio tự sync schemas từ Hive catalog</li>
</ol>

<h2>7.4 Verify Debezium Connector</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — kiểm tra Debezium</div>
  <pre><span class="comment"># Xem trạng thái KafkaConnector CRD — expected: RUNNING</span>
kubectl get kafkaconnector -n bnctl-kafka-development-ns
<span class="comment"># mssql-source-connector   RUNNING</span>

<span class="comment"># Log Kafka Connect nếu lỗi</span>
kubectl logs -n bnctl-kafka-development-ns \
  -l strimzi.io/cluster=debezium-connect --tail=50</pre>
</div>

<div class="callout callout-info">
  <span class="callout-title">snapshot.mode: initial</span>
  Lần đầu chạy Debezium sẽ snapshot toàn bộ <strong>39 bảng T24</strong> trong <code>testdb</code>.
  Nếu data lớn, quá trình có thể mất vài chục phút. Kiểm tra progress qua Kafka UI → topic <code>schema-changes.t24</code>.
</div>

<h2>7.5 Jenkins — Verify và lấy admin password</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — Jenkins admin password</div>
  <pre><span class="comment"># Lấy admin password từ secret</span>
kubectl exec -n bnctl-jenkins-development-ns statefulset/jenkins \
  -c jenkins -- cat /run/secrets/additional/chart-admin-password</pre>
</div>

<p>Sau khi login:</p>
<ol style="margin:10px 0 10px 20px">
  <li>Vào <strong>Manage Jenkins</strong> → <strong>Clouds</strong></li>
  <li>Chọn Kubernetes cloud → <strong>Test Connection</strong></li>
  <li>Nếu fail → kiểm tra secret <code>jenkins-casc-secrets</code> key <code>KUBECONFIG_B64</code> còn valid</li>
</ol>

<div class="callout callout-info">
  <span class="callout-title">Jenkins JCasC + SCM Triggers</span>
  Jenkins dùng JCasC (Configuration as Code) — tự load config từ ConfigMap <code>jenkins-jenkins-jcasc-config</code>.
  Các credentials load từ secret <code>jenkins-casc-secrets</code>: <code>GITLAB_API_TOKEN</code>, <code>AIRFLOW_SSH_KEY</code>, <code>KUBECONFIG_B64</code>.
  SCM polling interval và webhook triggers cấu hình qua Jenkins UI → job → Configure → Build Triggers (không phải Jenkinsfile).
</div>

<h2>7.6 Lấy admin password Airflow</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash</div>
  <pre><span class="comment"># Password trong Helm values (field: webserverSecretKey)</span>
helm get values airflow -n bnctl-airflow-development-ns | grep -i password

<span class="comment"># Hoặc xem secret trực tiếp</span>
kubectl get secret airflow-webserver-secret-key \
  -n bnctl-airflow-development-ns \
  -o jsonpath="{{.data.webserver-secret-key}}" | base64 -d</pre>
</div>

<h2>7.7 Trigger DAG thử nghiệm</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — trigger DAG với date range</div>
  <pre><span class="comment"># Tất cả DAGs dùng schedule=None — phải trigger thủ công</span>
airflow dags trigger accounting_pipeline_parser \
  --conf '{{"START_DATE":"2026-04-01","END_DATE":"2026-04-01"}}'

airflow dags trigger accounting_pipeline_model \
  --conf '{{"START_DATE":"2026-04-01","END_DATE":"2026-04-01"}}'</pre>
</div>
</div>


<!-- ══════════════════════════════════════════════════════════════ -->
<!-- SECTION 8 — TROUBLESHOOTING                                    -->
<!-- ══════════════════════════════════════════════════════════════ -->
<div class="section">
<h1>8. Troubleshooting</h1>

<table>
  <tr>
    <th>Triệu chứng</th>
    <th>Nguyên nhân phổ biến</th>
    <th>Hướng xử lý</th>
  </tr>
  <tr>
    <td>Airflow pod <code>CrashLoopBackOff</code></td>
    <td>DB migration chưa xong, secret thiếu, image pull lỗi</td>
    <td><code>kubectl logs airflow-scheduler-* -n bnctl-airflow-development-ns --previous</code> → xem error; kiểm tra secret <code>airflow-metadata</code> có đúng connection string không</td>
  </tr>
  <tr>
    <td>Hive Metastore không start</td>
    <td>DB <code>hive_metastore</code> chưa tạo, secret <code>hive-metastore-secret</code> sai JDBC URL</td>
    <td>Verify <code>init-dwh-databases.sql</code> đã chạy thành công; <code>kubectl describe secret hive-metastore-secret -n bnctl-hive-development-ns</code></td>
  </tr>
  <tr>
    <td>Spark jobs fail <code>S3AException</code></td>
    <td>MinIO credentials sai trong secret <code>minio-credentials</code> (Spark ns) hoặc Hive core-site.xml chưa patch</td>
    <td>Verify: <code>kubectl get secret minio-credentials -n bnctl-spark2-development-ns -o yaml</code>; chạy lại <code>patch_hive_minio_config</code> thủ công nếu cần</td>
  </tr>
  <tr>
    <td>Debezium connector <code>FAILED</code></td>
    <td>MSSQL chưa enable CDC, Kafka broker chưa ready, password sai</td>
    <td><code>kubectl get kafkaconnector -n bnctl-kafka-development-ns</code> → xem <code>status.conditions</code>; check MSSQL: <code>SELECT is_cdc_enabled FROM sys.databases</code></td>
  </tr>
  <tr>
    <td>ELK pod fail <code>secret not found</code></td>
    <td>Secret <code>minio-credentials</code> trong ELK namespace thiếu (chạy với <code>--skip-secrets</code>)</td>
    <td>Chạy <code>collect_secrets()</code> độc lập hoặc tạo thủ công: <code>kubectl create secret generic minio-credentials --from-literal=access_key=... --from-literal=secret_key=... -n bnctl-elk-development-ns</code></td>
  </tr>
  <tr>
    <td>Jenkins không spawn pod agents</td>
    <td>KUBECONFIG_B64 trong <code>jenkins-casc-secrets</code> hết hạn hoặc sai cluster</td>
    <td>Regenerate: <code>kubectl config view --raw | base64 -w0</code> → update secret; Test Connection trong Jenkins Clouds</td>
  </tr>
  <tr>
    <td><code>helm: command not found</code></td>
    <td>Helm chưa cài hoặc không trong PATH</td>
    <td><code>curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash</code></td>
  </tr>
  <tr>
    <td>PostgreSQL init SQL fail</td>
    <td>Pod chưa fully ready khi chạy init, PGPASSWORD sai</td>
    <td>Script dùng <code>PGPASSWORD=$(cat $POSTGRES_PASSWORD_FILE)</code> — verify: <code>kubectl exec -it cluster-postgresql-primary-0 -n bnctl-postgres-development-ns -- psql -U postgres -l</code></td>
  </tr>
</table>

<h2>8.1 Chạy lại một service cụ thể</h2>
<div class="cmd-block">
  <div class="cmd-block-header">bash — upgrade Helm release đơn lẻ</div>
  <pre><span class="comment"># Ví dụ: upgrade lại Airflow</span>
helm upgrade airflow apache-airflow/airflow \
  --version 1.18.0 \
  -f deployment/airflow/values-full.yaml \
  -n bnctl-airflow-development-ns

<span class="comment"># Restart deployment sau khi thay đổi secret</span>
kubectl rollout restart deployment/airflow-scheduler \
  -n bnctl-airflow-development-ns</pre>
</div>

<h2>8.2 Reset và deploy lại toàn bộ</h2>
<div class="callout callout-danger">
  <span class="callout-title">DESTRUCTIVE — xóa toàn bộ data</span>
  Chỉ dùng trong môi trường development khi muốn reset sạch. Không dùng trên production.
</div>
<div class="cmd-block">
  <div class="cmd-block-header">bash — uninstall tất cả Helm releases</div>
  <pre><span class="comment"># Uninstall từng release</span>
helm uninstall airflow  -n bnctl-airflow-development-ns
helm uninstall minio    -n bnctl-minio-development-ns
helm uninstall cluster  -n bnctl-postgres-development-ns
helm uninstall redis-ha -n bnctl-redis-development-ns
<span class="comment"># ... tương tự cho các service khác</span>

<span class="comment"># Xóa namespace (xóa hết resources bên trong)</span>
kubectl delete namespace bnctl-airflow-development-ns
<span class="comment"># ... tương tự</span>

<span class="comment"># Deploy lại từ đầu</span>
bash deploy.sh helm</pre>
</div>
</div>

</body>
</html>
"""

if __name__ == "__main__":
    print(f"Rendering Mermaid architecture diagram...")
    print(f"Generating PDF: {OUTPUT}")
    HTML(string=HTML_CONTENT).write_pdf(
        str(OUTPUT),
        stylesheets=[CSS(string="@page { size: A4; margin: 18mm 18mm 22mm 18mm; background: #0f172a; }")]
    )
    size_kb = OUTPUT.stat().st_size // 1024
    print(f"Done — {OUTPUT.name} ({size_kb} KB)")
