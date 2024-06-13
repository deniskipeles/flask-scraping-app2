# Use the official Python image from Docker Hub
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file first
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip install -r requirements.txt

# Copy all files from the current directory into the container's /app directory
COPY . .

# Expose port 5000 to the outside world
EXPOSE 5000

# Set the command to run when the container starts
CMD ["./start.sh"]

