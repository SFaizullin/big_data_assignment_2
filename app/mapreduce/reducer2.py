import sys

current_word = None
word_docs = []
current_df = 0

total_docs = 0
total_len = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    parts = line.split('\t')
    if len(parts) != 2:
        continue
        
    key, val = parts[0], parts[1]
    
    if key == '!META_DOCS':
        doc_id, title, length = val.split('|')
        print(f"DOC_ID\t{doc_id}\t{title}\t{length}")
    elif key == '!META_TOTAL_DOCS':
        total_docs += int(val)
    elif key == '!META_TOTAL_LEN':
        total_len += int(val)
    else:
        if current_word == key:
            word_docs.append(val)
            current_df += 1
        else:
            if current_word:
                docs_str = ",".join(word_docs)
                print(f"INDEX\t{current_word}\t{current_df}\t{docs_str}")
            current_word = key
            word_docs = [val]
            current_df = 1

if current_word:
    docs_str = ",".join(word_docs)
    print(f"INDEX\t{current_word}\t{current_df}\t{docs_str}")

if total_docs > 0:
    print(f"GLOBAL_STATS\tTOTAL_DOCS\t{total_docs}")
    print(f"GLOBAL_STATS\tTOTAL_LEN\t{total_len}")