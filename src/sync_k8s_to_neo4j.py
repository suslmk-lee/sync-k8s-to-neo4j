from kubernetes import client, config, watch
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging
import time
from threading import Thread
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 환경 확인 (Kubernetes 또는 로컬)
def is_running_in_kubernetes():
    return os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount')

# PROFILE 환경 변수 설정
PROFILE = os.getenv("PROFILE", "dev")
logger.info(f"현재 PROFILE: {PROFILE}")

# Kubernetes 환경에서는 ConfigMap과 Secret에서 환경 변수를 이미 로드했으므로 추가 로드 불필요
if not is_running_in_kubernetes():
    # 로컬 개발 환경에서만 .env 파일 로드
    logger.info("로컬 개발 환경에서 실행 중입니다. .env 파일을 로드합니다.")
    # 현재 스크립트의 디렉토리 경로 가져오기
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 프로젝트 루트 디렉토리 (src의 상위 디렉토리)
    root_dir = os.path.dirname(current_dir)
    env_file = os.path.join(root_dir, f".env.{PROFILE}")
    
    logger.info(f"환경 변수 파일 경로: {env_file}")
    
    if os.path.exists(env_file):
        logger.info(f"환경 변수 파일 로드 중: {env_file}")
        load_dotenv(dotenv_path=env_file, override=True)
        logger.info(f"환경 변수 파일 로드 완료: {env_file}")
    else:
        logger.warning(f"환경 변수 파일을 찾을 수 없습니다: {env_file}, 기본 .env 파일 또는 환경 변수를 사용합니다.")
        default_env = os.path.join(root_dir, ".env")
        if os.path.exists(default_env):
            logger.info(f"기본 환경 변수 파일 로드 중: {default_env}")
            load_dotenv(dotenv_path=default_env, override=True)
        else:
            logger.info("환경 변수 파일이 없습니다. 시스템 환경 변수를 사용합니다.")
else:
    logger.info("Kubernetes 환경에서 실행 중입니다. ConfigMap과 Secret에서 환경 변수를 사용합니다.")
    # Kubernetes 환경에서는 환경 변수가 이미 설정되어 있으므로 추가 작업 불필요
    logger.info(f"NEO4J_URI 환경 변수: {os.getenv('NEO4J_URI')}")
    logger.info(f"SYNC_DELAY_SECONDS 환경 변수: {os.getenv('SYNC_DELAY_SECONDS')}")

class KubernetesClient:
    def __init__(self, kubeconfig_path=None):
        self.kubeconfig_path = kubeconfig_path or os.getenv("KUBECONFIG_PATH")
        try:
            if self.kubeconfig_path:
                config.load_kube_config(config_file=self.kubeconfig_path)
                logger.info(f"Loaded kubeconfig from {self.kubeconfig_path}")
            else:
                config.load_kube_config()
                logger.info("Loaded default kubeconfig")
        except Exception:
            try:
                config.load_incluster_config()
                logger.info("Loaded in-cluster config")
            except Exception as e:
                raise Exception(f"Failed to load Kubernetes configuration: {str(e)}")
        
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_objects = client.CustomObjectsApi()

class Neo4jSync:
    def __init__(self):
        self.k8s_client = KubernetesClient()
        
        # 환경에 따른 기본값 설정
        default_neo4j_uri = "bolt://neo4j.default.svc.cluster.local:7687" if is_running_in_kubernetes() else "bolt://180.210.82.103:32500"
        
        # 환경 변수 로드 (Kubernetes 환경에서는 ConfigMap 또는 Secret에서 자동으로 로드됨)
        self.neo4j_uri = os.getenv("NEO4J_URI", default_neo4j_uri)
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        self.sync_delay_seconds = float(os.getenv("SYNC_DELAY_SECONDS", 0))
        
        # 최종 설정값 로깅
        logger.info(f"사용할 Neo4j URI: {self.neo4j_uri}")
        logger.info(f"사용할 Sync Delay: {self.sync_delay_seconds}초")
        try:
            self.driver = GraphDatabase.driver(self.neo4j_uri, auth=("neo4j", self.neo4j_password))
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info(f"Successfully connected to Neo4j at {self.neo4j_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j at {self.neo4j_uri}: {str(e)}")
            raise

    def update_namespace(self, tx, ns):
        tx.run("MERGE (n:Namespace {name: $name}) SET n.status = $status",
               name=ns.metadata.name, status=ns.status.phase)

    def update_pod(self, tx, pod):
        tx.run("""
            MERGE (p:Pod {name: $name, namespace: $namespace})
            SET p.status = $status
            MERGE (n:Namespace {name: $namespace})
            MERGE (p)-[:BELONGS_TO]->(n)
        """, name=pod.metadata.name, namespace=pod.metadata.namespace, status=pod.status.phase)

    def update_deployment(self, tx, dep):
        tx.run("""
            MERGE (d:Deployment {name: $name, namespace: $namespace})
            SET d.replicas = $replicas
            MERGE (n:Namespace {name: $namespace})
            MERGE (d)-[:BELONGS_TO]->(n)
        """, name=dep.metadata.name, namespace=dep.metadata.namespace, replicas=dep.spec.replicas)

    def watch_resource(self, resource_type, list_func, update_func):
        w = watch.Watch()
        last_version = {}
        logger.info(f"Starting watch for {resource_type}...")
        for event in w.stream(list_func):
            obj = event['object']
            name = obj.metadata.name
            version = obj.metadata.resource_version
            if name in last_version and last_version[name] == version:
                logger.debug(f"Skipping {resource_type} {name} - unchanged version {version}")
                continue
            logger.info(f"{resource_type} {name} - {event['type']} - ResourceVersion: {version}")
            with self.driver.session() as session:
                session.write_transaction(update_func, obj)
            last_version[name] = version
            if self.sync_delay_seconds > 0:
                time.sleep(self.sync_delay_seconds)

    def run(self):
        logger.info("Starting Kubernetes to Neo4j real-time sync...")
        threads = [
            Thread(target=self.watch_resource, args=("Namespace", self.k8s_client.core_v1.list_namespace, self.update_namespace)),
            Thread(target=self.watch_resource, args=("Pod", self.k8s_client.core_v1.list_pod_for_all_namespaces, self.update_pod)),
            Thread(target=self.watch_resource, args=("Deployment", self.k8s_client.apps_v1.list_deployment_for_all_namespaces, self.update_deployment)),
        ]
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()

def main():
    sync = Neo4jSync()
    try:
        sync.run()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        sync.driver.close()

if __name__ == "__main__":
    main()