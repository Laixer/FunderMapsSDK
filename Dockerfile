FROM debian:bookworm-slim AS builder

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

#-----------------------------------------------------------------------------

# Final stage
FROM ghcr.io/astral-sh/uv:debian-slim

# Define arguments for user/group IDs for better permission handling
ARG UID=1001
ARG GID=1001

# Install runtime dependencies only (what tippecanoe needs to run, not build)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy tippecanoe from builder stage
COPY --from=builder /usr/local/bin/tippecanoe* /usr/local/bin/

# Create a non-root user 'eve' to run the application
RUN groupadd -g ${GID} eve && \
    useradd -u ${UID} -g eve -s /bin/sh -m eve

# Switch to the non-root user
USER eve

# Set working directory and fix permissions
WORKDIR /app
COPY . .

# Configure UV and the environment PATH
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PATH="/app/.venv/bin:$PATH"

# Install the project and its dependencies using the lockfile.
RUN --mount=type=cache,target=/home/eve/.cache/uv,uid=${UID},gid=${GID} \
    uv sync --locked --no-dev

ENTRYPOINT ["uv", "run"]
