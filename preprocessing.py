import json
import re


def clean_feature(feature):
    """Cleaning individual features by dropping entries containing specific unwanted patterns and normalizing the rest."""
    unwanted_patterns = [
        r"<[^>]*>",
        r"https?:\/\/\S+",
        r"P\.when\(.*?\);",
        r"span class\w+",
    ]

    if any(re.search(pattern, feature) for pattern in unwanted_patterns):
        return ""

    feature = re.sub(r"\\", "", feature)
    feature = re.sub(r"\s+", " ", feature).strip()
    feature = re.sub(r"[^\w\s]", "", feature)

    return feature


def preprocess_record(record):
    main_cat_html = record.get("main_cat", "")
    main_cat_match = re.search(r'alt="([^"]+)"', main_cat_html)
    main_cat = main_cat_match.group(1) if main_cat_match else "Unknown Category"

    features = record.get("feature", [])
    cleaned_features = [
        clean_feature(feature) for feature in features if feature.strip()
    ]

    preprocessed = {
        "asin": record["asin"],
        "brand": record.get("brand", "unknown"),
        "category": record.get("category"),
        "main_cat": main_cat,
        "features": cleaned_features,
    }

    related_products = set(record.get("also_buy", []))
    related_products.update(record.get("also_viewed", []))
    if related_products:
        preprocessed["related"] = list(related_products)

    return preprocessed


input_file_path = "Sampled_Amazon_Meta.json"
output_file_path = "preprocessed_for_itemsets.json"

records = []
with open(input_file_path, "r") as infile, open(output_file_path, "w") as outfile:
    for line in infile:
        original_record = json.loads(line)
        preprocessed_record = preprocess_record(original_record)
        records.append(preprocessed_record)
        if len(records) == 100:
            break
    json.dump(records, outfile, indent=4)
