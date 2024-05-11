import orjson
import re
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count


def clean_feature(feature):
    """Cleaning individual features by removing unwanted entries,
    normalizing the rest."""

    unwanted_patterns = [
        r"<[^>]*>",
        r"https?:\/\/\S+",
        r"P\.when\(.*?\);",
        r"span class\w+",
        r"^$",
        "unknown",
    ]

    for pattern in unwanted_patterns:
        if re.search(pattern, feature, re.IGNORECASE):
            return None

    feature = re.sub(r"\\", "", feature)
    feature = re.sub(r"\s+", " ", feature).strip()
    feature = re.sub(r"[^\w\s]", "", feature)

    return feature if feature else None


# Compile the regular expression
alt_text_regex = re.compile(r'alt="([^"]+)"')


def preprocess_record(record):
    """Process each record to clean and normalize data fields."""

    preprocessed = {
        "asin": record.get("asin", "").strip(),
        "category": [
            cat.strip() for cat in record.get("category", []) if cat.strip()
        ],
        "price": record.get("price", 0.0),
        "also_buy": list(record.get("also_buy", [])),
        "title": record.get("title", "").strip(),
    }
    return preprocessed


def process_batch(records):
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        preprocessed_records = list(executor.map(preprocess_record, records))
    return preprocessed_records


def main():
    input_file_path = "Sampled_Amazon_Meta.json"
    output_file_path = "preprocessed-Sampled_Amazon_Meta.json"

    batch_size = 100000

    with open(input_file_path, "r") as infile, \
         open(output_file_path, "wb") as outfile:
        batch = []
        counter = 0
        for line in infile:
            if counter >= 5:
                break
            original_record = orjson.loads(line)
            batch.append(original_record)
            if len(batch) == batch_size:
                preprocessed_records = process_batch(batch)
                outfile.write(
                    orjson.dumps(
                        preprocessed_records,
                        option=orjson.OPT_INDENT_2
                    )
                )
                batch = []

        if batch:
            preprocessed_records = process_batch(batch)
            outfile.write(
                orjson.dumps(preprocessed_records, option=orjson.OPT_INDENT_2)
            )

    counter += 1


if __name__ == "__main__":
    main()
