FROM python:3-slim

# Set working directory
WORKDIR /app

# Install system dependencies for build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsqlite3-dev \
    zlib1g-dev \
    g++ \
    git \
    ca-certificates \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Install tippecanoe
RUN git clone https://github.com/felt/tippecanoe.git \
    && cd tippecanoe \
    && make -j \
    && make install \
    && cd .. \
    && rm -rf tippecanoe

# Clean up build dependencies to reduce image size
RUN apt-get purge -y --auto-remove build-essential g++ git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app/

# Set environment variables - these can be overridden at runtime
# ENV FUNDERMAPS_LOG_LEVEL=INFO \
#     FUNDERMAPS_MAX_WORKERS=3 \
#     FUNDERMAPS_DB_HOST=localhost \
#     FUNDERMAPS_DB_NAME=postgres \
#     FUNDERMAPS_DB_USER=postgres \
#     FUNDERMAPS_DB_PORT=25060 \
#     FUNDERMAPS_S3_SERVICE_URI=https://ams3.digitaloceanspaces.com

# Run the script when the container launches
ENTRYPOINT ["python"]
