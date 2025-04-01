#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting FunderMaps SDK deployment...${NC}"

# Build Docker container
echo -e "${YELLOW}Building Docker container...${NC}"
docker build -t fundermaps-sdk .

if [ $? -ne 0 ]; then
    echo "Docker build failed!"
    exit 1
fi
echo -e "${GREEN}Docker container built successfully!${NC}"

# Check if contrib directory exists
if [ ! -d "contrib" ]; then
    echo "contrib directory not found!"
    exit 1
fi

# Install systemd unit files
echo -e "${YELLOW}Installing systemd unit files...${NC}"
for unit_file in contrib/*.service contrib/*.timer; do
    if [ -f "$unit_file" ]; then
        echo "Installing $unit_file..."
        sudo cp "$unit_file" /etc/systemd/system/
    fi
done

# Reload systemd to recognize new unit files
echo -e "${YELLOW}Reloading systemd...${NC}"
sudo systemctl daemon-reload

# Enable and start all timer units
echo -e "${YELLOW}Enabling and starting timer units...${NC}"
for timer in contrib/*.timer; do
    if [ -f "$timer" ]; then
        timer_name=$(basename "$timer")
        echo "Enabling and starting $timer_name..."
        sudo systemctl enable "$timer_name"
        sudo systemctl start "$timer_name"
    fi
done

echo -e "${GREEN}Deployment completed successfully!${NC}"
