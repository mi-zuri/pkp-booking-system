FROM python:3.11-slim

WORKDIR /app

# Build deps for cassandra-driver's optional C extensions; slims down after.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Single worker per instance: the point of the project is horizontal scaling
# across container instances (app-1/2/3), not threads inside one process.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
