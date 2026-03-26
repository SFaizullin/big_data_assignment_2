from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import math
import re

def bm25_score(idf, tf, dl, avgdl, k1=1.0, b=0.75):
    return idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl))))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        query = sys.stdin.read().strip()
    else:
        query = sys.argv[1]

    spark = SparkSession.builder.appName("BM25_Ranker").getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster(['cassandra-server']) 
    session = cluster.connect('search_engine')

    global_stats = session.execute("SELECT total_docs, total_len FROM global_stats WHERE id=1").one()
    if not global_stats:
        print("Index not found.")
        sys.exit(1)
        
    total_docs = global_stats.total_docs
    avgdl = global_stats.total_len / total_docs

    terms = re.findall(r'[a-zA-Z]+', query.lower())
    
    doc_scores = {}
    
    for term in terms:
        vocab = session.execute("SELECT df FROM vocabulary WHERE term = %s", (term,)).one()
        if not vocab:
            continue
            
        df = vocab.df
        idf = math.log(total_docs / df)
        
        index_entries = session.execute("SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,))
        
        records = [(row.doc_id, row.tf) for row in index_entries]
        
        if records:
            rdd = sc.parallelize(records)
            doc_meta_rdd = rdd.map(lambda r: (r[0], idf, r[1])) # doc_id, idf, tf
            
            stats = doc_meta_rdd.collect()
            
            for doc_id, t_idf, tf in stats:
                doc_meta = session.execute("SELECT length FROM documents WHERE doc_id = %s", (doc_id,)).one()
                if doc_meta:
                    length = doc_meta.length
                    score = bm25_score(t_idf, tf, length, avgdl)
                    doc_scores[doc_id] = doc_scores.get(doc_id, 0) + score

    top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print("----- Top 10 Relevant Documents -----")
    for doc_id, score in top_docs:
        doc_meta = session.execute("SELECT title FROM documents WHERE doc_id = %s", (doc_id,)).one()
        title = doc_meta.title if doc_meta else "Unknown"
        print(f"Doc ID: {doc_id} | Title: {title} | BM25 Score: {score}")

    spark.stop()