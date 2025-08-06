FROM python:3.11-slim
WORKDIR /app
COPY scheduler.py .
RUN pip install kubernetes
CMD ["python", "scheduler.py"]
