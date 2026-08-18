pipeline {
    agent { label 'built-in' }

    options {
        gitLabConnection('dev-env')
        disableConcurrentBuilds()
    }

    environment {
        NAMESPACE        = "bnctl-airflow-development-ns"
        SPARK_NS         = "bnctl-spark2-development-ns"
        KAFKA_NS         = "bnctl-kafka-development-ns"
        IMAGE_REPO       = "haiptjits/dwh-test"
        IMAGE_TAG        = "airflow-build-${BUILD_NUMBER}"
        BUILD_JOB        = "build-airflow-image-${BUILD_NUMBER}"
        SPARK_IMAGE_REPO = "congtvjits/spark-t24"
        SPARK_IMAGE_TAG  = "latest"
        SPARK_BUILD_JOB  = "build-spark-t24-${BUILD_NUMBER}"
        CDC_ENABLE_JOB   = "cdc-enable-${BUILD_NUMBER}"
        CDC_SQL_CM       = "cdc-enable-sql-${BUILD_NUMBER}"
        KUBECONFIG       = "/tmp/kube-${BUILD_NUMBER}.yaml"
        PATH             = "${env.WORKSPACE}:${env.PATH}"
    }

    stages {

        stage('Prepare') {
            steps {
                script { env.BRANCH = env.GIT_BRANCH?.replace('origin/', '') ?: 'develop' }
                withCredentials([string(credentialsId: 'k8s-dev-kubeconfig', variable: 'KUBECONFIG_CONTENT')]) {
                    sh '''
                    mkdir -p ~/.ssh && ssh-keyscan gitlab.com >> ~/.ssh/known_hosts || true
                    printf '%s' "$KUBECONFIG_CONTENT" > ${KUBECONFIG} && chmod 600 ${KUBECONFIG}
                    command -v kubectl || {
                        curl -Lo "$WORKSPACE/kubectl" "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
                        chmod +x "$WORKSPACE/kubectl"
                    }
                    export PATH=/var/jenkins_home/miniconda/bin:$PATH
                    pip install --quiet --upgrade boto3
                    '''
                }
            }
        }

        stage('dbt Lint') {
            steps {
                sh '''
                set -e
                export PATH=/var/jenkins_home/miniconda/bin:$PATH
                export DREMIO_HOST=localhost DREMIO_USER=dummy DREMIO_PASSWORD=dummy
                for proj in T24_SILVER T24_ACCOUNTING T24_CREDIT T24_OPERATIONAL T24_AML T24_TREASURY COB_TEST; do
                    cd ${WORKSPACE}/${proj}
                    # T24_AML dùng dbt_utils.generate_surrogate_key -> phải deps trước khi parse
                    if [ -f packages.yml ]; then dbt deps --profiles-dir .; fi
                    dbt parse --profiles-dir . && echo "✅ ${proj}"
                    cd ${WORKSPACE}
                done
                '''
            }
        }

        stage('Build & Push Image') {
            steps {
                gitlabCommitStatus(name: 'build-image') {
                    sh '''
                    set -e
                    kubectl delete job ${BUILD_JOB} -n ${NAMESPACE} --ignore-not-found=true
                    cat <<YAML | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${BUILD_JOB}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app.kubernetes.io/name: jenkins
              topologyKey: kubernetes.io/hostname
      initContainers:
        - name: git-clone
          image: alpine/git:v2.52.0
          command: ["/bin/sh","-c"]
          args:
            - |
              set -ex
              mkdir -p /root/.ssh
              cp /ssh-secret/gitSshKey /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa
              ssh-keyscan gitlab.com >> /root/.ssh/known_hosts
              git clone --depth 1 -b ${BRANCH} git@gitlab.com:neonstudio/bnctl-data-warehouse/airflow_orchestration.git /workspace
          volumeMounts:
            - { name: workspace, mountPath: /workspace }
            - { name: ssh, mountPath: /ssh-secret, readOnly: true }
      containers:
        - name: kaniko
          image: gcr.io/kaniko-project/executor:latest
          args:
            - --context=dir:///workspace
            - --destination=${IMAGE_REPO}:${IMAGE_TAG}
            - --cache=true
            - --cache-repo=${IMAGE_REPO}-cache
          volumeMounts:
            - { name: workspace, mountPath: /workspace }
            - { name: dockerhub-creds, mountPath: /kaniko/.docker, readOnly: true }
      volumes:
        - { name: workspace, emptyDir: {} }
        - { name: ssh, secret: { secretName: airflow-ssh-secret } }
        - name: dockerhub-creds
          secret:
            secretName: dockerhub-credentials
            items: [{ key: config.json, path: config.json }]
YAML
                    until kubectl get pods -n ${NAMESPACE} -l job-name=${BUILD_JOB} -o jsonpath="{.items[0].metadata.name}" 2>/dev/null | grep -q .; do sleep 2; done
                    kubectl wait --for=condition=complete job/${BUILD_JOB} -n ${NAMESPACE} --timeout=600s
                    '''
                }
            }
        }

        stage('Deploy Image') {
            steps {
                gitlabCommitStatus(name: 'deploy') {
                    sh '''
                    set -e

                    for deploy in airflow-scheduler airflow-dag-processor airflow-api-server; do
                        kubectl patch deployment $deploy -n ${NAMESPACE} --type=json \
                            -p='[{"op":"replace","path":"/spec/strategy/rollingUpdate/maxSurge","value":0},{"op":"replace","path":"/spec/strategy/rollingUpdate/maxUnavailable","value":1}]' 2>/dev/null || true
                    done

                    # Sync ConfigMap overrides from repo
                    kubectl get configmap pull-bulk-dag-patch -n ${NAMESPACE} >/dev/null 2>&1 && \
                        kubectl create configmap pull-bulk-dag-patch \
                            --from-file=pull_bulk_dag.py=${WORKSPACE}/dags/pull_bulk_dag.py \
                            --from-file=t24_bcp_extract.yaml=${WORKSPACE}/spark-app/extract/t24_bcp_extract.yaml \
                            --from-file=t24_bulk_parse.yaml=${WORKSPACE}/spark-app/parser/t24_bulk_parse.yaml \
                            -n ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

                    kubectl get configmap pull-bulk-fresh-dag-patch -n ${NAMESPACE} >/dev/null 2>&1 && \
                        kubectl create configmap pull-bulk-fresh-dag-patch \
                            --from-file=pull_bulk_fresh_dag.py=${WORKSPACE}/dags/pull_bulk_fresh_dag.py \
                            -n ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

                    kubectl get configmap pull-bulk-fresh-parse-yaml -n ${NAMESPACE} >/dev/null 2>&1 && \
                        kubectl create configmap pull-bulk-fresh-parse-yaml \
                            --from-file=t24_bulk_fresh_parse.yaml=${WORKSPACE}/spark-app/parser/t24_bulk_fresh_parse.yaml \
                            -n ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

                    kubectl get configmap sftp-hold-dag-patch -n ${NAMESPACE} >/dev/null 2>&1 && \
                        kubectl create configmap sftp-hold-dag-patch \
                            --from-file=hold_accounting_parser_spark.yaml=${WORKSPACE}/spark-app/parser/hold_accounting_parser_spark.yaml \
                            --from-file=hold_crb_parser_spark.yaml=${WORKSPACE}/spark-app/parser/hold_crb_parser_spark.yaml \
                            --from-file=sftp_sync_spark.yaml=${WORKSPACE}/spark-app/sync/sftp_sync_spark.yaml \
                            -n ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

                    kubectl get configmap t24-bcp-extract-script -n ${SPARK_NS} >/dev/null 2>&1 && \
                        kubectl create configmap t24-bcp-extract-script \
                            --from-file=t24_bcp_extract.py=${WORKSPACE}/jobs/t24_bcp_extract.py \
                            -n ${SPARK_NS} --dry-run=client -o yaml | kubectl apply -f -

                    # Update pod template (image + mounts from repo)
                    sed "s|IMAGE_PLACEHOLDER|${IMAGE_REPO}:${IMAGE_TAG}|g" jenkins/pod_template_file.yaml > /tmp/pod_template_new.yaml
                    printf 'data:\n  pod_template_file.yaml: |\n' > /tmp/cm_patch.yaml
                    sed 's/^/    /' /tmp/pod_template_new.yaml >> /tmp/cm_patch.yaml
                    kubectl patch configmap airflow-config -n ${NAMESPACE} --type=merge --patch-file /tmp/cm_patch.yaml

                    # Update worker_container_repository/tag in airflow.cfg
                    AIRFLOW_CFG=$(kubectl get configmap airflow-config -n ${NAMESPACE} -o jsonpath='{.data.airflow\\.cfg}')
                    AIRFLOW_CFG=$(echo "$AIRFLOW_CFG" | sed "s|worker_container_repository = .*|worker_container_repository = ${IMAGE_REPO}|g")
                    AIRFLOW_CFG=$(echo "$AIRFLOW_CFG" | sed "s|worker_container_tag = .*|worker_container_tag = ${IMAGE_TAG}|g")
                    printf 'data:\n  airflow.cfg: |\n' > /tmp/cfg_patch.yaml
                    echo "$AIRFLOW_CFG" | sed 's/^/    /' >> /tmp/cfg_patch.yaml
                    kubectl patch configmap airflow-config -n ${NAMESPACE} --type=merge --patch-file /tmp/cfg_patch.yaml

                    # Roll out new image
                    kubectl set image deployment/airflow-scheduler \
                        scheduler=${IMAGE_REPO}:${IMAGE_TAG} scheduler-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} -n ${NAMESPACE}
                    kubectl set image deployment/airflow-dag-processor \
                        dag-processor=${IMAGE_REPO}:${IMAGE_TAG} dag-processor-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} -n ${NAMESPACE}
                    kubectl set image deployment/airflow-api-server \
                        api-server=${IMAGE_REPO}:${IMAGE_TAG} -n ${NAMESPACE}
                    kubectl set image statefulset/airflow-triggerer \
                        triggerer=${IMAGE_REPO}:${IMAGE_TAG} triggerer-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} -n ${NAMESPACE}

                    for res in deployment/airflow-scheduler deployment/airflow-dag-processor deployment/airflow-api-server statefulset/airflow-triggerer; do
                        kubectl rollout resume $res -n ${NAMESPACE} 2>/dev/null || true
                    done

                    kubectl rollout status deployment/airflow-scheduler   -n ${NAMESPACE} --timeout=600s &
                    kubectl rollout status deployment/airflow-dag-processor -n ${NAMESPACE} --timeout=600s &
                    kubectl rollout status deployment/airflow-api-server   -n ${NAMESPACE} --timeout=600s &
                    kubectl rollout status statefulset/airflow-triggerer   -n ${NAMESPACE} --timeout=600s &
                    wait
                    '''
                }
            }
        }

        stage('Build Spark Image') {
            when { changeset "spark/Dockerfile.t24" }
            steps {
                gitlabCommitStatus(name: 'build-spark-image') {
                    sh '''
                    set -e
                    kubectl delete job ${SPARK_BUILD_JOB} -n ${NAMESPACE} --ignore-not-found=true
                    cat <<YAML | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${SPARK_BUILD_JOB}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app.kubernetes.io/name: jenkins
              topologyKey: kubernetes.io/hostname
      initContainers:
        - name: git-clone
          image: alpine/git:v2.52.0
          command: ["/bin/sh","-c"]
          args:
            - |
              set -ex
              mkdir -p /root/.ssh
              cp /ssh-secret/gitSshKey /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa
              ssh-keyscan gitlab.com >> /root/.ssh/known_hosts
              git clone --depth 1 -b ${BRANCH} git@gitlab.com:neonstudio/bnctl-data-warehouse/airflow_orchestration.git /workspace
          volumeMounts:
            - { name: workspace, mountPath: /workspace }
            - { name: ssh, mountPath: /ssh-secret, readOnly: true }
      containers:
        - name: kaniko
          image: gcr.io/kaniko-project/executor:latest
          args:
            - --context=dir:///workspace
            - --dockerfile=spark/Dockerfile.t24
            - --destination=${SPARK_IMAGE_REPO}:${SPARK_IMAGE_TAG}
            - --cache=true
            - --cache-repo=${SPARK_IMAGE_REPO}-cache
          volumeMounts:
            - { name: workspace, mountPath: /workspace }
            - { name: dockerhub-creds, mountPath: /kaniko/.docker, readOnly: true }
      volumes:
        - { name: workspace, emptyDir: {} }
        - { name: ssh, secret: { secretName: airflow-ssh-secret } }
        - name: dockerhub-creds
          secret:
            secretName: dockerhub-congtvjits-credentials
            items: [{ key: config.json, path: config.json }]
YAML
                    until kubectl get pods -n ${NAMESPACE} -l job-name=${SPARK_BUILD_JOB} -o jsonpath="{.items[0].metadata.name}" 2>/dev/null | grep -q .; do sleep 2; done
                    kubectl wait --for=condition=complete job/${SPARK_BUILD_JOB} -n ${NAMESPACE} --timeout=600s
                    '''
                }
            }
        }

        stage('Verify DAGs') {
            steps {
                sh '''
                POD=$(kubectl get pods -n ${NAMESPACE} -l component=dag-processor --field-selector=status.phase=Running -o jsonpath="{.items[0].metadata.name}")
                kubectl exec -n ${NAMESPACE} $POD -c dag-processor -- ls /opt/airflow/dags/repo/dags/
                '''
            }
        }
    }

    post {
        always {
            sh '''
            kubectl delete job ${BUILD_JOB} ${SPARK_BUILD_JOB} ${CDC_ENABLE_JOB} -n ${NAMESPACE} --ignore-not-found=true 2>/dev/null || true
            kubectl delete configmap ${CDC_SQL_CM} -n ${KAFKA_NS} --ignore-not-found=true 2>/dev/null || true
            kubectl delete pods -n ${NAMESPACE} --field-selector=status.phase=Failed || true
            rm -f ${KUBECONFIG}
            '''
        }
        success { echo "✅ ${IMAGE_REPO}:${IMAGE_TAG} deployed" }
        failure { echo "❌ Build/deploy failed" }
    }
}
