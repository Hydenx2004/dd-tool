FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching — only re-runs if requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config.py .
COPY datadog_client.py .
COPY grok_builder.py .
COPY main.py .

ENTRYPOINT ["python", "main.py"]
