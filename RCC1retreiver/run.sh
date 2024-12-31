#!/bin/sh
Xvfb -ac -nolisten inet6 :99 &
python3 /opt/RCC1Retreiver.py