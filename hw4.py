# %%
import datetime
import json
from pathlib import Path

import boto3
import requests

# %%
DATE_PARAM = "2024-11-18"
date = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")
url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date.strftime('%Y/%m/%d')}"
print(f"Requesting REST API URL: {url}")

wiki_server_response = requests.get(url, headers={"User-Agent": "curl/7.68.0"})
wiki_response_status = wiki_server_response.status_code
wiki_response_body = wiki_server_response.text

print(f"Wikipedia REST API Response body: {wiki_response_body}")
print(f"Wikipedia REST API Response Code: {wiki_response_status}")

if wiki_response_status != 200:
    print(
        f"❌ Received non-OK status code from Wiki Server: {wiki_response_status}. Response body: {wiki_response_body}"
    )
else:
    print(f"✅ Successfully retrieved Wikipedia data, content-length: {len(wiki_response_body)}")

# %%
current_directory = Path(__file__).parent
RAW_LOCATION_BASE = current_directory / "data" / "raw-views"
RAW_LOCATION_BASE.mkdir(exist_ok=True, parents=True)
print(f"Created directory {RAW_LOCATION_BASE}")
raw_views_file = f"{RAW_LOCATION_BASE}/raw-views-{date.year}-{date.month}-{date.day:02d}.txt"  # Placeholder, feel free to remove this
raw_views_file = Path(raw_views_file)
with open(raw_views_file, "w") as f:
    f.write(wiki_response_body)

# %%
S3_WIKI_BUCKET = "michel-cc-wikidata"
s3 = boto3.client("s3", region_name="eu-west-1")

default_region = "eu-west-1"
try:
    bucket_configuration = {"LocationConstraint": default_region}
    response = s3.create_bucket(Bucket=S3_WIKI_BUCKET, CreateBucketConfiguration=bucket_configuration)

    print("\n📦 AWS Response:")
    print(response)

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"\n✅ Success! Bucket {S3_WIKI_BUCKET} created in {default_region}")
except Exception as e:
    print(f"❌ Error creating bucket: {str(e)}")

# Verify bucket access
try:
    s3.head_bucket(Bucket=S3_WIKI_BUCKET)
    print("✅ Bucket is accessible")
except Exception as e:
    print(f"❌ Cannot access bucket: {str(e)}")
    raise

# %%
print(f"⬆️  Uploading file to bucket: {S3_WIKI_BUCKET}")
file_name = f"raw-views-{date.year}-{date.month}-{date.day:02d}.txt"
try:
    s3.upload_file(raw_views_file, S3_WIKI_BUCKET, f"datalake/raw/{file_name}")
    print("✅ Upload successful!")
    print(f"📍 File location: s3://{S3_WIKI_BUCKET}/datalake/raw/{file_name}")
    print(f"ℹ️ Note: The https URL https://{S3_WIKI_BUCKET}.s3.eu-west-1.amazonaws.com/{file_name}")
    print("   won't work directly because S3 objects are private by default!")
    print("   We'll generate a pre-signed URL later to access this file via HTTPS.")

    # Verify the upload by listing objects in the bucket
    objects = s3.list_objects_v2(Bucket=S3_WIKI_BUCKET)
    print("\n📦 Current bucket contents:")
    for obj in objects.get("Contents", []):
        print(f"- {obj['Key']} ({obj['Size']} bytes)")
except Exception as e:
    print(f"❌ Error uploading file: {str(e)}")
s3_key = f"datalake/raw/{file_name}"  # Placeholder, feel free to remove this

# ==================================================


print(f"Uploaded raw edits to s3://{S3_WIKI_BUCKET}/{s3_key}")

# %%
wiki_response_parsed = wiki_server_response.json()
articles = wiki_response_parsed["items"][0]["articles"]
current_time = datetime.datetime.now(datetime.timezone.utc)  # Always use UTC!!
json_lines = ""
for article in articles:
    record = {
        "article": article["article"],
        "views": article["views"],
        "rank": article["rank"],
        "date": date.strftime("%Y-%m-%d"),
        "retrieved_at": current_time.replace(
            tzinfo=None
        ).isoformat(),  # We need to remove tzinfo as Athena cannot work with offsets
    }
    json_lines += json.dumps(record) + "\n"

JSON_LOCATION_DIR = current_directory / "data" / "views"
JSON_LOCATION_DIR.mkdir(exist_ok=True, parents=True)
print(f"Created directory {JSON_LOCATION_DIR}")
print(f"JSON lines:\n{json_lines}")

json_lines_filename = f"views-{date.strftime('%Y-%m-%d')}.json"
json_lines_file = JSON_LOCATION_DIR / json_lines_filename

with json_lines_file.open("w") as file:
    file.write(json_lines)

s3.upload_file(json_lines_file, S3_WIKI_BUCKET, f"datalake/views/{json_lines_filename}")
print(f"✅ Uploaded processed data to s3://{S3_WIKI_BUCKET}/datalake/views/{json_lines_filename}")
# %%
