#!/bin/bash

cd /home/ubuntu/dhan_system

git fetch origin

LOCAL=$(git rev-parse main)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" != "$REMOTE" ]; then
    git checkout main
    git pull origin main
    sudo systemctl restart dhanbot
fi
