import json
import boto3
from urllib.parse import urlparse

s3 = boto3.client("s3")

def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))
    try:
        s3_uri = event["s3Uri"]
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        print(f"Reading from S3 -> Bucket: {bucket}, Key: {key}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")
        print("Raw file contents:", data[:500])  # print first 500 chars

        content = json.loads(data)
        if "files" not in content:
            raise ValueError("Missing 'files' key in JSON")

        print("Parsed content successfully.")
        return content

    except Exception as e:
        import traceback
        traceback_str = traceback.format_exc()
        print("ERROR:", str(e))
        print("TRACE:", traceback_str)
        raise e
