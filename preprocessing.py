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

    # For the main category, extracting alt-text and removing junk.
    main_cat_html = record.get("main_cat", "")
    main_cat_match = alt_text_regex.search(main_cat_html)
    main_cat = (
        main_cat_match.group(1)
        if main_cat_match
        else record.get("main_cat", "").strip()
    )
    if main_cat.lower() in ["unknown category", "unknown", ""]:
        main_cat = None

    # Using the clean_feature function to clean the features.
    features = record.get("feature", [])
    cleaned_features = filter(None, (clean_feature(feature) for feature in features))

    preprocessed = {
        "asin": record.get("asin", "").strip(),
        "brand": record.get("brand", "").strip() or None,
        "category": [cat.strip() for cat in record.get("category", []) if cat.strip()],
        "main_cat": main_cat,
        "features": list(cleaned_features),
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
    output_file_path = "test.json"

    batch_size = 100000

    with open(input_file_path, "r") as infile, open(output_file_path, "wb") as outfile:
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
                    orjson.dumps(preprocessed_records, option=orjson.OPT_INDENT_2)
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