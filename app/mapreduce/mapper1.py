import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    parts = line.split('\t')
    if len(parts) < 3:
        continue
        
    doc_id = parts[0]
    doc_title = parts[1]
    doc_text = parts[2]
    
    WORDS = re.findall(r'[a-zA-Z]+', doc_text.lower())
    
    print(f"!META#{doc_id}\t{len(WORDS)}|{doc_title}")
    
    for word in WORDS:
        if len(word) > 1:
            print(f"{word}#{doc_id}\t1")