import json
import os
import boto3
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, timezone
from extract_text import run_textract , get_random_textract_client



S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
MONGO_URI = os.getenv("PROD_MONGO_URI")
MONGO_DB = os.getenv("MONGO_DATABASE")

mongo_client = MongoClient(MONGO_URI)
db =  mongo_client[MONGO_DB]
col_files = db["tb_file_details"]




def format_table(table):
    """Format a 2D table neatly with aligned columns."""
    if not table or not any(table):
        return ""
    
    # Normalize rows (ensure same length)
    max_cols = max(len(row) for row in table)
    normalized = [row + [""] * (max_cols - len(row)) for row in table]
    
    # Compute max width per column
    max_col_widths = [
        max(len(str(row[i])) for row in normalized)
        for i in range(max_cols)
    ]
    
    # Build formatted lines
    lines = []
    for row in normalized:
        line = " | ".join(str(cell).ljust(max_col_widths[i]) for i, cell in enumerate(row))
        lines.append(line)
    
    return "\n".join(lines)


def structure_textract_output(data):
    """Combine extracted Textract-like tables into a readable structure."""
    structured = []
    structured.append("📊 **TABLES (In Order)**")
    
    for idx, table in enumerate(data, 1):
        structured.append(f"\nTable {idx}:\n{format_table(table)}")
    
    return "\n".join(structured)

def lambda_handler(event, context):

    fileId = event["fileId"]
    userId = event["userId"]
    clusterId = event["clusterId"]
    creditId = event["creditId"]

    job_id = ObjectId()

    file_oid = ObjectId(fileId)
    user_oid = ObjectId(userId)
    cluster_oid = ObjectId(clusterId)
    credit_oid = ObjectId(creditId)

    # Fetch mongo document
    file_doc = col_files.find_one(
        {"_id": file_oid, "clusterId": cluster_oid, "userId": user_oid},
        {"originalS3File": 1}
    )

    if not file_doc:
        return {"error": "File not found in MongoDB"}

    originalS3File = file_doc.get("originalS3File")
    if not originalS3File:
        return {"error": "Missing originalS3File in DB"}

    local_path = f"/tmp/{originalS3File}"

    # cleanup old file
    if os.path.exists(local_path):
        os.remove(local_path)

    # S3 download path
    s3_key = f"{userId}/{clusterId}/raw/{originalS3File}"

    textract_client, region, temp_bucket = get_random_textract_client()

    # Run Textract
    extraction_result = run_textract(
        S3_BUCKET_NAME, s3_key, file_oid, textract_client, temp_bucket, region
    )

    pages = extraction_result.get("page_count", 0)
    raw_tables = extraction_result.get("normalized_data", {}).get("tables", [])
    raw_lines = extraction_result.get("normalized_data", {}).get("lines", [])

    tables = structure_textract_output(raw_tables)
    text_content = f"{tables}\n\nRAW_LINES\n" + "\n".join(raw_lines)

    return {
        "pages": pages,
        "text_content": text_content,
        "fileId": fileId,
        "userId": userId,
        "clusterId": clusterId,
        "creditId": creditId
    }
