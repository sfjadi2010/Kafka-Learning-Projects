FROM python:3.11-slim

WORKDIR /app

# Create data directory for SQLite
RUN mkdir -p /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
