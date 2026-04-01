import os
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

print("Uploading local a.parquet to HDFS...")
os.system("hdfs dfs -put -f /app/a.parquet /a.parquet")

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Reading /a.parquet from HDFS...")
df = spark.read.parquet("hdfs:///a.parquet")

n = 1000
print(f"Sampling {n} documents...")
df = df.select(['id', 'title', 'text']).limit(n)

if not os.path.exists("/app/data"):
    os.makedirs("/app/data")

print("Writing .txt files locally...")
for row in df.collect():
    filename = "/app/data/" + sanitize_filename(str(row['id']) + "_" + str(row['title'])).replace(" ", "_") + ".txt"
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(row['text'])
    except Exception as e:
        print(f"Failed {filename}: {e}")

print("Uploading .txt files to HDFS /data...")
os.system("hdfs dfs -mkdir -p /data")
os.system("hdfs dfs -put -f /app/data/*.txt /data/")

print("Transforming to RDD and saving to /input/data...")
sc = spark.sparkContext
rdd = sc.wholeTextFiles("hdfs:///data/*.txt")

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

os.system("hdfs dfs -rm -r -f /input/data")
formatted_rdd.coalesce(1).saveAsTextFile("hdfs:///input/data")

spark.stop()
print("Data preparation complete")
