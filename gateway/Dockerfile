# Use a lightweight, official Python runtime
FROM python:3.11-slim

# Prevent Python from writing .pyc files to disk and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the API script into the container
COPY gateway.py .

# Optional: If you have a static cluster topology, uncomment the line below to copy it in
# COPY cluster_topology.json .

# Expose the port the FastAPI server will run on
EXPOSE 8000

# Command to run the FastAPI application using Uvicorn
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8000"]
