#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    parts = line.split('\t')
    if len(parts) != 2:
        continue
        
    key, val = parts[0], parts[1]
    
    if key == '!META':
        doc_id, length_title = val.split(':', 1)
        length, title = length_title.split('|', 1)
        print(f"!META_DOCS\t{doc_id}|{title}|{length}")
        print(f"!META_TOTAL_DOCS\t1")
        print(f"!META_TOTAL_LEN\t{length}")
    else:
        # Pass word and tf to Reducer 2 to compute df
        print(f"{key}\t{val}")