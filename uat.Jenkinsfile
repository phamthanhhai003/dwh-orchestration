pipeline {
    agent any

    options {
        gitLabConnection('uat-env')
        disableConcurrentBuilds()
    }

    environment {
        NAMESPACE       = "airflow-uat-ns"
        IMAGE_REPO      = "haiptjits/dwh-uat"
        IMAGE_TAG       = "airflow--uat-build-${BUILD_NUMBER}"
        BUILD_JOB       = "build-airflow-image-${BUILD_NUMBER}"
        BRANCH          = "${env.GIT_BRANCH?.replaceAll('origin/', '') ?: 'uat'}"
        PATH            = "${env.WORKSPACE}:${env.PATH}"
        GIT_SSH_COMMAND = 'ssh -o AddressFamily=inet'
    }

    stages {

        stage('Prepare SSH') {
            steps {
                sh '''
                mkdir -p ~/.ssh
                ssh-keyscan -p 443 altssh.gitlab.com >> ~/.ssh/known_hosts || true

                if ! command -v kubectl >/dev/null 2>&1; then
                    K8S_VER=$(curl -Ls https://dl.k8s.io/release/stable.txt)
                    curl -Lo "$WORKSPACE/kubectl" "https://dl.k8s.io/release/${K8S_VER}/bin/linux/amd64/kubectl"
                    chmod +x "$WORKSPACE/kubectl"
                fi
                '''
            }
        }

        stage('Build & Push Image') {
            steps {
                gitlabCommitStatus(name: 'build-image') {
                    withKubeConfig([credentialsId: 'k8s-uat-kubeconfig']) {
                        sh '''
                        set -e

                        echo "=== Delete old build job ==="
                        kubectl delete job ${BUILD_JOB} -n ${NAMESPACE} --ignore-not-found=true

                        echo "=== Submit Kaniko build job ==="
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
          command:
            - /bin/sh
            - -c
            - |
              set -ex
              mkdir -p /root/.ssh
              cp /ssh-secret/gitSshKey /root/.ssh/id_rsa
              chmod 600 /root/.ssh/id_rsa
              echo "[altssh.gitlab.com]:443 ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBFSMqzJeV9rUzU4kWitGjeR4PWSa29SPqJ1fVkhtj3Hw9xjLVXVYrU9QlYWrOLXBpQ6KWjbjTDTdDkoohFzgbEY=" >> /root/.ssh/known_hosts
              echo "[altssh.gitlab.com]:443 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAfuCHKVTjquxvt6CM6tdG4SLp1Btn/nOeHHE5UOzRdf" >> /root/.ssh/known_hosts
              git clone --depth 1 -b ${BRANCH} ssh://git@altssh.gitlab.com:443/neonstudio/data-warehouse/airflow_orchestration.git /workspace
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ssh
              mountPath: /ssh-secret
              readOnly: true
      containers:
        - name: kaniko
          image: gcr.io/kaniko-project/executor:latest
          args:
            - --context=dir:///workspace
            - --destination=${IMAGE_REPO}:${IMAGE_TAG}
            - --cache=true
            - --cache-repo=haiptjits/dwh-test
            - --snapshot-mode=redo
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: dockerhub-creds
              mountPath: /kaniko/.docker
              readOnly: true
      volumes:
        - name: workspace
          emptyDir:
            medium: Memory
        - name: ssh
          secret:
            secretName: airflow-ssh-secret
        - name: dockerhub-creds
          secret:
            secretName: dockerhub-credentials
            items:
              - key: config.json
                path: config.json
YAML

                        echo "=== Wait for build ==="
                        until kubectl get pods -n ${NAMESPACE} -l job-name=${BUILD_JOB} -o jsonpath="{.items[0].metadata.name}" 2>/dev/null | grep -q .; do sleep 2; done
                        kubectl wait --for=condition=complete job/${BUILD_JOB} -n ${NAMESPACE} --timeout=900s
                        '''
                    }
                }
            }
        }

        stage('Build Logs') {
            steps {
                withKubeConfig([credentialsId: 'k8s-uat-kubeconfig']) {
                    sh '''
                    POD=$(kubectl get pods -n ${NAMESPACE} -l job-name=${BUILD_JOB} -o jsonpath="{.items[0].metadata.name}")
                    echo "=== Kaniko logs ==="
                    kubectl logs -n ${NAMESPACE} $POD -c kaniko || true
                    '''
                }
            }
        }

        stage('Deploy Image') {
            steps {
                gitlabCommitStatus(name: 'deploy') {
                    withKubeConfig([credentialsId: 'k8s-uat-kubeconfig']) {
                        sh '''
                        set -e

                        echo "=== Set maxSurge=0 to prevent OOM pod flood ==="
                        for deploy in airflow-scheduler airflow-dag-processor airflow-api-server; do
                          kubectl patch deployment $deploy -n ${NAMESPACE} --type=json \
                            -p='[{"op":"replace","path":"/spec/strategy/rollingUpdate/maxSurge","value":0},{"op":"replace","path":"/spec/strategy/rollingUpdate/maxUnavailable","value":1}]' 2>/dev/null || true
                        done

                        echo "=== Update image ==="
                        kubectl set image deployment/airflow-scheduler \
                          scheduler=${IMAGE_REPO}:${IMAGE_TAG} \
                          scheduler-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} \
                          -n ${NAMESPACE}

                        kubectl set image deployment/airflow-dag-processor \
                          dag-processor=${IMAGE_REPO}:${IMAGE_TAG} \
                          dag-processor-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} \
                          -n ${NAMESPACE}

                        kubectl set image deployment/airflow-api-server \
                          api-server=${IMAGE_REPO}:${IMAGE_TAG} \
                          -n ${NAMESPACE}

                        kubectl set image statefulset/airflow-triggerer \
                          triggerer=${IMAGE_REPO}:${IMAGE_TAG} \
                          triggerer-log-groomer=${IMAGE_REPO}:${IMAGE_TAG} \
                          -n ${NAMESPACE}

                        echo "=== Patch worker pod template: image ==="
                        sed "s|IMAGE_PLACEHOLDER|${IMAGE_REPO}:${IMAGE_TAG}|g" jenkins/pod_template_file.yaml > /tmp/pod_template_new.yaml
                        echo 'data:' > /tmp/cm_patch.yaml
                        echo '  pod_template_file.yaml: |' >> /tmp/cm_patch.yaml
                        sed 's/^/    /' /tmp/pod_template_new.yaml >> /tmp/cm_patch.yaml
                        kubectl patch configmap airflow-config -n ${NAMESPACE} --type=merge --patch-file /tmp/cm_patch.yaml

                        echo "=== Patch airflow.cfg worker_container image ==="
                        AIRFLOW_CFG=$(kubectl get configmap airflow-config -n ${NAMESPACE} -o jsonpath='{.data.airflow\\.cfg}')
                        AIRFLOW_CFG=$(echo "$AIRFLOW_CFG" | sed "s|worker_container_repository = .*|worker_container_repository = ${IMAGE_REPO}|g")
                        AIRFLOW_CFG=$(echo "$AIRFLOW_CFG" | sed "s|worker_container_tag = .*|worker_container_tag = ${IMAGE_TAG}|g")
                        echo 'data:' > /tmp/cfg_patch.yaml
                        echo '  airflow.cfg: |' >> /tmp/cfg_patch.yaml
                        echo "$AIRFLOW_CFG" | sed 's/^/    /' >> /tmp/cfg_patch.yaml
                        kubectl patch configmap airflow-config -n ${NAMESPACE} --type=merge --patch-file /tmp/cfg_patch.yaml

                        echo "=== Resume any paused rollouts ==="
                        for deploy in airflow-scheduler airflow-dag-processor airflow-api-server; do
                          kubectl rollout resume deployment/$deploy -n ${NAMESPACE} 2>/dev/null || true
                        done
                        kubectl rollout resume statefulset/airflow-triggerer -n ${NAMESPACE} 2>/dev/null || true

                        echo "=== Run DB migrate to unblock init containers ==="
                        TRIGGERER_POD=$(kubectl get pods -n ${NAMESPACE} -l component=triggerer \
                          --field-selector=status.phase=Running \
                          -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || echo "")
                        if [ -n "$TRIGGERER_POD" ]; then
                          kubectl exec -n ${NAMESPACE} $TRIGGERER_POD -c triggerer -- airflow db migrate || true
                        fi

                        echo "=== Wait rollout ==="
                        kubectl rollout status deployment/airflow-scheduler -n ${NAMESPACE} --timeout=900s
                        kubectl rollout status deployment/airflow-dag-processor -n ${NAMESPACE} --timeout=900s
                        kubectl rollout status deployment/airflow-api-server -n ${NAMESPACE} --timeout=900s
                        kubectl rollout status statefulset/airflow-triggerer -n ${NAMESPACE} --timeout=900s
                        '''
                    }
                }
            }
        }

        stage('Verify DAG in Airflow') {
            steps {
                withKubeConfig([credentialsId: 'k8s-uat-kubeconfig']) {
                    sh '''
                    echo "=== Check DAG files in dag-processor ==="
                    POD=$(kubectl get pods -n ${NAMESPACE} -l component=dag-processor --field-selector=status.phase=Running -o jsonpath="{.items[0].metadata.name}")
                    kubectl exec -n ${NAMESPACE} $POD -c dag-processor -- ls /opt/airflow/dags/repo/dags/
                    '''
                }
            }
        }
    }

    post {
        always {
            script {
                try {
                    withKubeConfig([credentialsId: 'k8s-uat-kubeconfig']) {
                        sh '''
                        kubectl delete job ${BUILD_JOB} -n ${NAMESPACE} --ignore-not-found=true || true
                        kubectl delete pods -n ${NAMESPACE} --field-selector=status.phase=Failed || true
                        '''
                    }
                } catch (err) {
                    echo "Post-cleanup skipped: ${err.getMessage()}"
                }
            }
        }
        success {
            echo "✅ Image ${IMAGE_REPO}:${IMAGE_TAG} deployed successfully!"
        }
        failure {
            echo "❌ Build/deploy failed!"
        }
    }
}
