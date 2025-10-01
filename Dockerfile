# Use official Python slim image
FROM python:3.10-slim

# Prevent Python from writing .pyc files & force stdout flushing
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy app and src folders, and run.py
COPY app/ ./app/
COPY src/ ./src/
COPY app.py .