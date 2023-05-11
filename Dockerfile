# Base image
FROM python:3.10-slim


# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Start the application
ENTRYPOINT ["python","/app/src/main.py"]