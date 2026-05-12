FROM python:3.11-slim

# Java para Spark
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Variables Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Carpeta trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar proyecto
COPY . .

CMD ["tail", "-f", "/dev/null"]