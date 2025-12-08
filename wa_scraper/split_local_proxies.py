from pathlib import Path

input_file = "local_proxies.txt"
num_parts = 5

# Read and clean lines
lines = [line.strip() for line in Path(input_file).read_text().splitlines() if line.strip()]
total = len(lines)

# Compute chunk sizes
chunk_size = total // num_parts
remainder = total % num_parts

start = 0
for i in range(1, num_parts + 1):
    # Distribute the remainder (one extra line per file until used up)
    end = start + chunk_size + (1 if i <= remainder else 0)
    part_lines = lines[start:end]
    
    out_file = f"./local_proxies/proxies_part{i}.txt"
    Path(out_file).write_text("\n".join(part_lines))
    print(f"Wrote {len(part_lines)} lines → {out_file}")
    
    start = end
