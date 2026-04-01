import sys
import math
import re
import argparse
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

def bm25_score(idf, tf, dl, avgdl, k1=1.0, b=0.75):
    return idf * ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl))))

parser = argparse.ArgumentParser()
parser.add_argument('query_args', nargs='*', help='Optional query string arguments')
args = parser.parse_args()

if args.query_args:
    query = " ".join(args.query_args).strip()
else:
    query = sys.stdin.read().strip()
    
if not query:
    print("Empty query.")
    sys.exit(0)

spark = SparkSession.builder.appName("BM25_Ranker").getOrCreate()
sc = spark.sparkContext

cluster = Cluster(['cassandra-server']) 
session = cluster.connect('search_engine')

global_stats = session.execute("SELECT total_docs, total_len FROM global_stats WHERE id=1").one()
if not global_stats:
    print("Index not found")
    sys.exit(1)
    
total_docs = global_stats.total_docs
avgdl = global_stats.total_len / total_docs

terms = re.findall(r'[a-zA-Z]+', query.lower())

score_contributions = []

for term in terms:
    vocab = session.execute("SELECT df FROM vocabulary WHERE term = %s", (term,)).one()
    if not vocab:
        continue
        
    df = vocab.df
    idf = math.log(total_docs / df)
    
    index_entries = session.execute("SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,))
    
    for row in index_entries:
        score_contributions.append((row.doc_id, (idf, row.tf)))
        
if not score_contributions:
    print("----- Top 10 Relevant Documents -----")
    print("No matches found")
    spark.stop()
    sys.exit(0)
    
doc_ids = list(set([item[0] for item in score_contributions]))

doc_meta_dict = {}
for doc_id in doc_ids:
    doc_meta = session.execute("SELECT length, title FROM documents WHERE doc_id = %s", (doc_id,)).one()
    if doc_meta:
        doc_meta_dict[doc_id] = (doc_meta.length, doc_meta.title)
        
doc_meta_broadcast = sc.broadcast(doc_meta_dict)

rdd = sc.parallelize(score_contributions)
grouped_rdd = rdd.groupByKey().mapValues(list)

def calculate_final_score(record):
    doc_id, tf_idfs = record
    meta = doc_meta_broadcast.value.get(doc_id)
    if not meta:
        return (doc_id, "Unknown", 0.0)
        
    dl, title = meta
    total_score = 0.0
    for idf, tf in tf_idfs:
        total_score += bm25_score(idf, tf, dl, avgdl)
        
    return (doc_id, title, total_score)
    
scores_rdd = grouped_rdd.map(calculate_final_score)
top_10 = scores_rdd.sortBy(lambda x: x[2], ascending=False).take(10)

print("----- Top 10 Relevant Documents -----")
for doc_id, title, score in top_10:
    print(f"Doc ID: {doc_id} | Title: {title} | BM25 Score: {score}")

spark.stop()