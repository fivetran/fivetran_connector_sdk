#!/bin/bash

# Install Chrome and ChromeDriver on Linux (Debian/Ubuntu based)

# Update package lists
sudo apt-get update

# Install dependencies
sudo apt-get install -y wget unzip libxss1 libappindicator3-1 libasound2

# Download and install Google Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f -y # Fix any dependency issues

# Get the Chrome version
chrome_version=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1)

# Get the latest ChromeDriver version compatible with Chrome
chromedriver_version=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$chrome_version")

# Download ChromeDriver
wget "https://chromedriver.storage.googleapis.com/$chromedriver_version/chromedriver_linux64.zip"

# Unzip ChromeDriver
unzip chromedriver_linux64.zip

# Move ChromeDriver to /usr/local/bin (or another directory in your PATH) and make it executable
sudo mv chromedriver /usr/local/bin/chromedriver
sudo chmod +x /usr/local/bin/chromedriver

# Clean up downloaded files
rm google-chrome-stable_current_amd64.deb chromedriver_linux64.zip

echo "Google Chrome and ChromeDriver installed successfully."
echo "ChromeDriver version: $chromedriver_version"
echo "Chrome version: $chrome_version"