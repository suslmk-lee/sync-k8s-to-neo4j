# sync-k8s-to-neo4j

## 프로젝트 개요

Kubernetes AI Agent Addon Module로, Kubernetes 리소스 및 이벤트 정보를 Neo4j Graph DB에 실시간으로 동기화하는 모듈입니다. 이 모듈은 Kubernetes 클러스터의 상태를 그래프 데이터베이스에 저장하여 시각화, 분석 및 AI 기반 운영 지원을 가능하게 합니다.

## 주요 기능

- Kubernetes 리소스(Namespace, Pod, Deployment) 실시간 모니터링
- 리소스 변경 사항을 Neo4j Graph DB에 자동 동기화
- 리소스 간 관계 모델링 (예: Pod와 Namespace 간의 관계)
- 개발 및 운영 환경에서 유연하게 동작하도록 설계

## 기술적 구성

### 아키텍처

이 프로젝트는 다음과 같은 구성 요소로 이루어져 있습니다:

1. **Kubernetes Client**: Kubernetes API 서버에 연결하여 리소스 변경 사항을 감시합니다.
2. **Neo4j Client**: 그래프 데이터베이스에 연결하여 데이터를 저장합니다.
3. **동기화 엔진**: Kubernetes 이벤트를 Neo4j 그래프 모델로 변환합니다.

### 기술 스택

- **언어**: Python 3
- **Kubernetes 연동**: kubernetes-client/python
- **그래프 DB**: Neo4j
- **환경 설정**: dotenv (로컬 개발), Kubernetes ConfigMap 및 Secret (운영 환경)
- **컨테이너화**: Docker/Podman
- **배포**: Kubernetes Deployment

## 환경 설정

### 로컬 개발 환경

로컬 개발 환경에서는 `.env.dev` 파일을 통해 환경 변수를 설정합니다:

```
NEO4J_URI=bolt://180.210.82.103:32500
NEO4J_PASSWORD=password
SYNC_DELAY_SECONDS=5
KUBECONFIG_PATH=/path/to/your/kubeconfig
```

### Kubernetes 운영 환경

Kubernetes 환경에서는 ConfigMap과 Secret을 사용하여 환경 변수를 설정합니다:

```yaml
# ConfigMap 예시
apiVersion: v1
kind: ConfigMap
metadata:
  name: neo4j-sync-config
data:
  SYNC_DELAY_SECONDS: "2"
  PROFILE: "prod"

# Secret 예시
apiVersion: v1
kind: Secret
metadata:
  name: neo4j-sync-secret
type: Opaque
data:
  NEO4J_PASSWORD: cGFzc3dvcmQK  # "password"의 Base64 인코딩 값
```

## 빌드 및 배포

### 컨테이너 이미지 빌드 및 푸시

```bash
# 레지스트리 로그인
sudo podman login 44ce789b-kr1-registry.container.nhncloud.com

# 이미지 빌드
sudo podman build -t 44ce789b-kr1-registry.container.nhncloud.com/container-platform-registry/sync-k8s-to-neo4j .

# 이미지 푸시
sudo podman push 44ce789b-kr1-registry.container.nhncloud.com/container-platform-registry/sync-k8s-to-neo4j
```

### Kubernetes 배포

```bash
# RBAC 설정 적용
kubectl apply -f k8s/sync-rbac.yaml

# 애플리케이션 배포
kubectl apply -f k8s/sync-deployment.yaml
```

## 실행 방법

### 로컬 개발 환경

```bash
# 개발 환경 설정으로 실행
PROFILE=dev python src/sync_k8s_to_neo4j.py

# 운영 환경 설정으로 실행
PROFILE=prod python src/sync_k8s_to_neo4j.py
```

## Neo4j 그래프 모델

이 프로젝트는 다음과 같은 노드 유형과 관계를 사용합니다:

- **노드 유형**:
  - Namespace: Kubernetes 네임스페이스
  - Pod: Kubernetes 파드
  - Deployment: Kubernetes 디플로이먼트

- **관계 유형**:
  - BELONGS_TO: 리소스가 특정 네임스페이스에 속함을 나타냄

## 라이선스

이 프로젝트는 내부 사용 목적으로 개발되었습니다.
