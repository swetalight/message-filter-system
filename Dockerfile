FROM python:3.7-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip

# Устанавливаем зависимости в правильном порядке
RUN pip install --no-cache-dir kafka-python==1.4.7
RUN pip install --no-cache-dir aiokafka==0.5.2
RUN pip install --no-cache-dir mode==2.0.0
RUN pip install --no-cache-dir faust==1.10.4
RUN pip install --no-cache-dir python-dotenv==0.20.0

COPY patch_nullcontext.py .
COPY app.py .

RUN mkdir -p /data

ENV PYTHONPATH=/app

# Применяем патч и запускаем Faust worker
CMD ["sh", "-c", "sleep 5 && python patch_nullcontext.py && faust -A app worker -l info"]