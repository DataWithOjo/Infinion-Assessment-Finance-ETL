FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Prevent Python from writing .pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Ensure output is flushed directly to terminal
ENV PYTHONUNBUFFERED=1

# Copy requirements first (to leverage Docker cache)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Default command to run the pipeline
CMD ["python", "main.py"]