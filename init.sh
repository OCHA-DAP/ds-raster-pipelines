#!/bin/bash
set -m

# Remove all package lists
sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean
sudo apt-get update

# Install core dependencies first
sudo apt-get install -V build-essential cmake libnetcdf-dev --assume-yes --fix-missing

# Try jpeg dependency specifically since that was erroring
sudo apt-get install -V libopenjpeg-dev --assume-yes --fix-missing

# Then try eccodes
sudo apt-get install -V libeccodes-dev --assume-yes --fix-missing
