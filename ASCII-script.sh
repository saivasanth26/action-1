#!/bin/bash

# 1. Update package list (required for apt-get to find the package)
sudo apt-get update

# 2. Install cowsay
sudo apt-get install cowsay -y

# 3. Use the full path for cowsay (Ubuntu/Debian installs it to /usr/games/)
/usr/games/cowsay -f dragon "Run for cover, I am a DRAGON...RAAR" > dragon.txt

# 4. Display results
cat dragon.txt
ls -ltra
