#!/bin/bash
# Update and install are required because runners start fresh each time
sudo apt-get update
sudo apt-get install cowsay -y

# Use the full path to the executable
/usr/games/cowsay -f dragon "run for cover, I am a DRAGON" >> dragon.txt

# Verification steps
cat dragon.txt
ls -ltra
