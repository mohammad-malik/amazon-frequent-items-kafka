import orjson
import re
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count
from azure.storage.blob import BlobServiceClient


def clean_feature(feature):
    """Cleaning individual features by removing unwanted entries, normalizing the rest."""
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


# Compile the regular expression for alt text extraction
alt_text_regex = re.compile(r'alt="([^"]+)"')


def preprocess_record(record):
    """Process each record to clean and normalize data fields."""
    main_cat_html = record.get("main_cat", "")
    main_cat_match = alt_text_regex.search(main_cat_html)
    main_cat = (
        main_cat_match.group(1)
        if main_cat_match
        else record.get("main_cat", "").strip()
    )
    if main_cat.lower() in ["unknown category", "unknown", ""]:
        main_cat = None

    features = record.get("feature", [])
    cleaned_features = filter(
        None,
        (clean_feature(feature) for feature in features))

    preprocessed = {
        "asin": record.get("asin", "").strip(),
        "brand": record.get("brand", "").strip() or None,
        "category": [
            cat.strip() for cat in record.get("category", []) if cat.strip()
        ],
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
    # Reading connection string, container name, blob name from file
    with open("azure_blob_data.txt", "r") as file:
        connect_str = file.readline().replace("\n", "")
        container_name = file.readline().replace("\n", "")
        blob_name = file.readline().replace("\n", "")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    batch_size = 100000
    batch = []
    buffer = ""
    processed_blob_client = container_client.get_blob_client(
        "processed-" + blob_name
    )

    stream = blob_client.download_blob().chunks()
    for chunk in stream:
        data = buffer + chunk.decode("utf-8")
        records = data.split("\n")
        buffer = records.pop()  # Save incomplete record to buffer

        for record in records:
            if record.strip():  # Avoid processing empty lines
                original_record = orjson.loads(record)
                batch.append(original_record)
                if len(batch) == batch_size:
                    preprocessed_records = process_batch(batch)
                    processed_blob_client.upload_blob(
                        b"\n".join(
                            [orjson.dumps(record)
                             for record in preprocessed_records]
                        ),
                        blob_type="AppendBlob",
                        overwrite=True,
                    )
                    batch = []

    if batch:
        preprocessed_records = process_batch(batch)
        processed_blob_client.upload_blob(
            b"\n".join(
                [orjson.dumps(record)
                 for record in preprocessed_records]
            ),
            blob_type="AppendBlob",
            overwrite=True,
        )


if __name__ == "__main__":
    main()
