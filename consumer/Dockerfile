FROM python:3.11-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/ .
COPY storage/ /app/storage/

ENV PYTHONUNBUFFERED=1

CMD ["python", "consumer.py"]
