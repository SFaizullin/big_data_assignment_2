from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName('data preparation') \
    .getOrCreate()

if not os.path.exists("data"):
    os.makedirs("data")

print("Uploading local data/ directory to HDFS...")
os.system("hdfs dfs -mkdir -p /data")
os.system("hdfs dfs -put -f data/*.txt /data/")

sc = spark.sparkContext
rdd = sc.wholeTextFiles("/data/*.txt")

def parse_doc(file_tuple):
    filepath, text = file_tuple
    filename = filepath.split('/')[-1]
    name_no_ext = filename.rsplit('.', 1)[0]
    
    parts = name_no_ext.split('_', 1)
    doc_id = parts[0]
    doc_title = parts[1] if len(parts) > 1 else "Unknown"
    
    safe_text = text.replace('\t', ' ').replace('\n', ' ').strip()
    return f"{doc_id}\t{doc_title}\t{safe_text}"

formatted_rdd = rdd.map(parse_doc)

print("Formatting data and saving to /input/data in HDFS as single partition...")
os.system("hdfs dfs -rm -r -f /input/data")
formatted_rdd.coalesce(1).saveAsTextFile("/input/data")

spark.stop()
print("Data preparation complete.")