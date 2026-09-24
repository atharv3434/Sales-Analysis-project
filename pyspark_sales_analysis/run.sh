#!/bin/bash

set -e

echo "Starting PySpark Sales Analysis..."
spark-submit src/main.py
echo "PySpark job completed."
