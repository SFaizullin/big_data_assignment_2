import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    parts = line.split('\t')
    if len(parts) != 2:
        continue
        
    key, val = parts[0], parts[1]
    
    if key.startswith('!META'):
        _, doc_id = key.split('#')
        print(f"!META\t{doc_id}:{val}")
    else:
        if current_key == key:
            current_count += int(val)
        else:
            if current_key:
                word, doc_id = current_key.split('#')
                print(f"{word}\t{doc_id}:{current_count}")
            current_key = key
            current_count = int(val)

if current_key and not current_key.startswith('!META'):
    word, doc_id = current_key.split('#')
    print(f"{word}\t{doc_id}:{current_count}")