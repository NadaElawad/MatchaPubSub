FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application, database, and static web code
COPY waiter.py client.py db.py dashboard.py api.py dishwasher.py stream_analytics.py ./
COPY static ./static

# Default to running the waiter service
CMD ["python", "-u", "waiter.py"]
