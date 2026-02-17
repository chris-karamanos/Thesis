FROM python:3.11-slim

WORKDIR /app

# preventing Python from writing .pyc files to disc and buffering stdout and stderr 
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# build deps for some Python wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
 && rm -rf /var/lib/apt/lists/*

# install dependencies  
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY . /app

EXPOSE 8008

CMD ["uvicorn", "rank_service:app", "--host", "0.0.0.0", "--port", "8008"]
