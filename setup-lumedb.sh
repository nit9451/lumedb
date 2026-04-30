#!/bin/bash

# LumeDB Quick Setup Script
# This script downloads and starts the LumeDB Docker container.

set -e

echo "=========================================="
echo "🚀 Welcome to the LumeDB Setup Installer!"
echo "=========================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed on this machine."
    echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop/"
    echo "and run this script again."
    exit 1
fi

echo "✅ Docker is installed."
echo "📦 Pulling the latest LumeDB image from Docker Hub..."
docker pull niti45/lumedb:latest

echo ""
echo "⚙️  Starting the LumeDB Engine on port 7070..."

# Check if port 7070 is already in use by another container
if docker ps | grep -q 7070; then
    echo "⚠️  Port 7070 is currently in use. Attempting to stop conflicting LumeDB container..."
    CONTAINER_ID=$(docker ps -q --filter "ancestor=niti45/lumedb:latest")
    if [ ! -z "$CONTAINER_ID" ]; then
        docker stop $CONTAINER_ID
        docker rm $CONTAINER_ID
    else
        echo "❌ Port 7070 is being used by another application. Please free port 7070."
        exit 1
    fi
fi

# Run the container with a persistent volume
docker run -d \
  --name lumedb-engine \
  -p 7070:7070 \
  -v lumedb_data:/var/lib/lumedb \
  --restart unless-stopped \
  niti45/lumedb:latest

echo ""
echo "=========================================="
echo "🎉 SUCCESS! LumeDB is now running locally."
echo "=========================================="
echo "• The database is listening for TCP connections on: 127.0.0.1:7070"
echo "• Data is persistently stored in the 'lumedb_data' Docker volume."
echo ""
echo "👉 Next Steps:"
echo "1. Read the LEARNING_LUMEDB.md file to learn the query syntax."
echo "2. Open LumeDB Studio in your browser at https://lumedb-studio.vercel.app"
echo "3. Connect the Studio to 127.0.0.1 : 7070 to explore your database visually!"
echo ""
echo "To view database logs, run: docker logs -f lumedb-engine"
echo "To stop the database, run: docker stop lumedb-engine"
