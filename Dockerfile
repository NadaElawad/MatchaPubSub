FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy waiter code
COPY waiter.py .

# Run with python -u (unbuffered) so logs stream in real time to kubectl logs
ENTRYPOINT ["python", "-u", "waiter.py"]
CMD ["--bootstrap-server", "kafka:9092"]
