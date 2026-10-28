FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt && useradd --uid 10001 --create-home worker
COPY extraction.py db.py worker.py ./
USER 10001
ENTRYPOINT ["python", "worker.py"]
CMD ["work"]
