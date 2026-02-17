# playwright image with Python 3.12 and Ubuntu Jammy (22.04)
FROM mcr.microsoft.com/playwright/python:v1.56.0-jammy

# cleaner logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# working directory inside the container
WORKDIR /app

# copy and install dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of the code
COPY . .

# sleep infinity because it's meant to run only occasionally and we want to keep the container alive for debugging
CMD ["sleep", "infinity"]
