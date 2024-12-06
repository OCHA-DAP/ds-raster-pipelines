#!/bin/bash
set -m

# Remove all package lists
sudo rm -rf /var/lib/apt/lists/*

# Get fresh package lists and update
sudo apt-get clean
sudo apt-get update

# Try installing with verbose output for better debugging
sudo apt-get install -V libeccodes-dev --assume-yes --fix-missing
