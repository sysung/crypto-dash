# Start with a lightweight, official Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual python streaming scripts
COPY main.py .
COPY producers/ producers/

# Tell the container what command to run when it boots up
CMD ["python", "main.py"]