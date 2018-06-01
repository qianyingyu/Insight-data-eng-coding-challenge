#!/usr/bin/env bash

# The three external python library I used are sys, csv and numpy
# If these packages are missing, please use pip to install them
# pip install sys
# pip install csv
# pip install numpy

# Make sure the python file is executable
chmod a+x ./src/sessionization.py

# Execute the following command from the root directory
python src/sessionization.py ./input/log.csv ./input/inactivity_period.txt ./output/sessionization.txt
