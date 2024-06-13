FROM python:3.9-slim

# Set working directory to /app
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Expose port 5000 for Flask app
EXPOSE 5000

# Start RQ workers
CMD ["rq", "worker", "-w", "4", "groq", "post"] && \
    gunicorn -w 4 app:app
