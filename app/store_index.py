from cassandra.cluster import Cluster
import sys
import subprocess

def get_hdfs_files(path):
    proc = subprocess.Popen(["hdfs", "dfs", "-cat", f"{path}/part-*"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = proc.communicate()
    return stdout.decode('utf-8').split('\n')

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS search_engine WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS global_stats (
        id int PRIMARY KEY,
        total_docs int,
        total_len int
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        doc_id text PRIMARY KEY,
        title text,
        length int
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        tf int,
        PRIMARY KEY (term, doc_id)
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        df int
    );
""")

lines = get_hdfs_files("/indexer/index")

for line in lines:
    line = line.strip()
    if not line:
        continue
        
    parts = line.split('\t')
    record_type = parts[0]
    
    if record_type == 'DOC_ID':
        doc_id, title, length = parts[1], parts[2], int(parts[3])
        session.execute("INSERT INTO documents (doc_id, title, length) VALUES (%s, %s, %s)", (doc_id, title, length))
    
    elif record_type == 'GLOBAL_STATS':
        stat_name, stat_val = parts[1], int(parts[2])
        if stat_name == 'TOTAL_DOCS':
            session.execute("UPDATE global_stats SET total_docs = %s WHERE id = 1", (stat_val,))
        elif stat_name == 'TOTAL_LEN':
            session.execute("UPDATE global_stats SET total_len = %s WHERE id = 1", (stat_val,))

    elif record_type == 'INDEX':
        term, df, docs_str = parts[1], int(parts[2]), parts[3]
        session.execute("INSERT INTO vocabulary (term, df) VALUES (%s, %s)", (term, df))
        
        for doc_tf in docs_str.split(','):
            doc_id, tf = doc_tf.split(':')
            session.execute("INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)", (term, doc_id, int(tf)))

print("Index stored in Cassandra successfully")