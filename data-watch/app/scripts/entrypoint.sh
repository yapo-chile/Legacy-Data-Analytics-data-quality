#!/bin/sh

python /app/populate_data.py $@
python -W ignore /app/datawatch.py $@