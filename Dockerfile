# Use an explicit, lightweight Python runtime base image
FROM python:3.11-slim

# Set system environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files to disk
# PYTHONUNBUFFERED: Ensures application logs are flushed straight to the console stream
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5710

# Set the operational working directory inside the container
WORKDIR /app

# Install system-level dependencies if required (e.g., libpq for PostgreSQL compatibility)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only the requirements file first to maximize Docker layer caching efficiency
COPY requirements.txt .

# Install pinned Python production dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the core application workspace layers
COPY ./app ./app
COPY ./docs ./docs

# Expose the microservice web entry port
EXPOSE ${PORT}

# Fire up the production ASGI production worker server
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]