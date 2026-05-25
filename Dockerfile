FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Librairies systeme utiles pour drivers DB (Oracle/PostgreSQL/MySQL)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Par defaut, garde le conteneur idle pour permettre l'execution manuelle
CMD ["sleep", "infinity"]
