FROM python:3-slim AS builder

# Install build dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    git \
    ca-certificates \
    zlib1g-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Build tippecanoe
RUN git clone --depth 1 https://github.com/felt/tippecanoe.git \
    && cd tippecanoe \
    && make -j 2 \
    && make install

# Final stage
FROM python:3-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies only (what tippecanoe needs to run, not build)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    gdal-bin \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy tippecanoe from builder stage
COPY --from=builder /usr/local/bin/tippecanoe* /usr/local/bin/

# Copy requirements file
COPY requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app/

# Run the script when the container launches (this should be set in the command)
ENTRYPOINT ["python3"]
