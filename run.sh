#!/bin/bash
# Force execution on Apple Silicon architecture to match installed packages
source venv/bin/activate
arch -arm64 python main.py
