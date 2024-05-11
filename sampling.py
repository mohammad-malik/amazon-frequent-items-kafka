import json
import random
from tqdm import tqdm


def sample_json(
    input_file,
    output_file,
    target_size_gb,
    filter_key="also_buy",
    batch_size=10000
                ):
    target_size_bytes = target_size_gb * 1024**3
    current_size_bytes = 0

    with open(input_file, "r", encoding="utf-8") as infile, open(
        output_file, "w", encoding="utf-8"
    ) as outfile:
        batch = []
        for line in tqdm(infile, desc=f"Sampling {target_size_gb} \
                                            GB from {input_file}"):

            batch.append(line)
            if len(batch) >= batch_size:
                random.shuffle(batch)
                for line in batch:
                    if current_size_bytes >= target_size_bytes:
                        return
                    record = json.loads(line)
                    if record.get(filter_key):
                        outfile.write(line)
                        current_size_bytes += len(line.encode("utf-8"))
                batch = []

        # Process the last batch
        if batch:
            random.shuffle(batch)
            for line in batch:
                if current_size_bytes >= target_size_bytes:
                    return
                record = json.loads(line)
                if record.get(filter_key):
                    outfile.write(line)
                    current_size_bytes += len(line.encode("utf-8"))

    print("Finished sampling. Output size: " +
          f"{current_size_bytes / 1024**3:.2f} GB")


sample_json("All_Amazon_Meta.json", "Sampled_Amazon_Meta.json", 15)
