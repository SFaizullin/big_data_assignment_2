#!/bin/bash
QUERY="$*"
if [ -z "$QUERY" ]; then
    echo "Usage: ./search.sh <query>"
    exit 1
fi

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    --archives .venv.tar.gz#environment \
    query.py "$QUERY"