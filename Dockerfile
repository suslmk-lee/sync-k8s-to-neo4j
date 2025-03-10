FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/sync_k8s_to_neo4j.py .
COPY .env.dev .env.prod ./
CMD ["python", "sync_k8s_to_neo4j.py"]