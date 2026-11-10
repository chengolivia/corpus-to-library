
#!/bin/bash
set -euo pipefail

# Update system
sudo apt update
sudo apt upgrade -y

# Install system dependencies
sudo apt install -y \
    curl \
    git \
    python3.12 \
    python3.12-venv

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"

uv --version

# Clone repository
git clone https://github.com/chengolivia/corpus-to-library.git
cd corpus-to-library

# Install dependencies from committed lockfile
uv sync --locked

echo "Setup complete!"
