#!/bin/bash
set -m

echo 'Hello world!'
# Remove all package lists
sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean
sudo apt-get update

echo 'Done!
