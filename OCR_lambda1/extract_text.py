import os
import time
from typing import Tuple, List, Union
import boto3
from mongo import (
    try_claim_processing,
    fetch_job_record,
    set_job_started,
    set_job_succeeded,
    set_job_failed,
    get_textract_job_collection
)
from datetime import datetime, timezone
from trp import Document
from botocore.exceptions import ClientError

import time
import traceback

from uuid import uuid4
from typing import List, Dict, Any, Optional, Tuple

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")

AWS_REGION = "ap-south-1"

s3 = boto3.client("s3", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)


# -----------------------------
TEMP_BUCKET_PREFIX = "yellow-temp-"
S3_SOURCE_REGION = "ap-south-1"

# ============================================================
# S3 / Textract Helpers
# ============================================================

def get_random_textract_client():
    """Select a random AWS region for Textract and return client + temp bucket name."""
    region = S3_SOURCE_REGION
    print(f"🌍 Using Textract in region: {region}")
    textract = boto3.client("textract", region_name=region)
    temp_bucket = f"{TEMP_BUCKET_PREFIX}{region}"
    return textract, region, temp_bucket


def copy_to_temp_bucket(source_bucket: str, source_key: str, temp_bucket: str, region: str) -> Optional[str]:
    """Copy file to temporary bucket in the same region."""
    s3_dest = boto3.client("s3", region_name=region)
    try:
        temp_key = f"{uuid4().hex}_{source_key.split('/')[-1]}"
        print(f"📤 Copying file to temp bucket: s3://{temp_bucket}/{temp_key}")
        s3_dest.copy_object(
            Bucket=temp_bucket,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Key=temp_key
        )
        return temp_key
    except ClientError as e:
        print(f"⚠️ S3 ClientError: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Unexpected S3 copy error: {e}")
        return None


def cleanup_temp_bucket(temp_bucket: str, temp_key: Optional[str], region: str):
    """Delete temporary object after Textract completes."""
    if not temp_key:
        return
    try:
        print(f"🗑️ Cleaning up temp file: s3://{temp_bucket}/{temp_key}")
        boto3.client("s3", region_name=region).delete_object(Bucket=temp_bucket, Key=temp_key)
    except Exception as e:
        print(f"⚠️ Cleanup failed: {e}")


def poll_existing_job(textract_client, job_id: str, wait_time: int = 5, max_wait_minutes: int = 20):
    """Poll an existing Textract job until completion or timeout. Used when another worker has already started the same job."""
    print(f"🔁 Polling Textract job {job_id} (max {max_wait_minutes} min)")
    elapsed = 0
    job_output = None
    status = "IN_PROGRESS"
    try:
        while elapsed < max_wait_minutes * 60:
            resp = textract_client.get_document_analysis(JobId=job_id)
            status = resp.get("JobStatus", status)
            print(f"... Job {job_id} → {status}")
            if status in ["SUCCEEDED", "FAILED"]:
                job_output = resp
                break
            time.sleep(wait_time)
            elapsed += wait_time

        if status != "SUCCEEDED":
            raise Exception(f"Job {job_id} did not succeed (status: {status})")

        # Collect all pages if succeeded
        results = job_output.get("Blocks", [])
        next_token = job_output.get("NextToken")
        while next_token:
            next_resp = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
            results.extend(next_resp.get("Blocks", []))
            next_token = next_resp.get("NextToken")

        textract_json = {
            "Blocks": results,
            "DocumentMetadata": job_output.get("DocumentMetadata", {})
        }
        normalized_data = normalize_textract_response(textract_json)
        page_count = textract_json["DocumentMetadata"].get("Pages", 0)
        final_output = {
            "page_count": page_count,
            "normalized_data": normalized_data,
            "raw_textract": textract_json
        }
        print(f"✅ Finished polling existing job {job_id}")
        return final_output

    except Exception as e:
        print(f"❌ Error polling job {job_id}: {e}")
        raise


# ============================================================
# Textract Normalization (TRP Based)
# ============================================================

def normalize_textract_response(textract_output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Textract JSON using TRP → extract only tables and lines.
    Returns:
    {
        "tables": [...],
        "lines": [...]
    }
    """
    

    print("🔄 Normalizing Textract JSON using TRP (tables + lines)...")
    doc = Document(textract_output)
    normalized = {"tables": [], "lines": []}

    # --- Extract tables ---
    for page_no, page in enumerate(doc.pages, start=1):
        for table in page.tables:
            table_data = []
            for row in table.rows:
                cells = [cell.text.strip() if cell.text else "" for cell in row.cells]
                if any(cells):  # skip empty rows
                    table_data.append(cells)
            if table_data:
                normalized["tables"].append(table_data)

        # --- Extract lines ---
        for line in page.lines:
            if line.text and line.text.strip():
                normalized["lines"].append(line.text.strip())

    print(
        f"✅ Normalization complete: {len(normalized['tables'])} tables, "
        f"{len(normalized['lines'])} lines"
    )
    return normalized



# ============================================================
# Main Orchestrator
# ============================================================

def run_textract(bucket: str, key: str, file_id: str, textract_client, temp_bucket: str, region: str) -> Dict[str, Any]:
    """Distributed-safe Textract runner:
    - Uses Mongo locking (_id = fileId)
    - Reuses completed jobs when available
    - Avoids duplicate concurrent processing
    """
    temp_key = None
    owner_id = str(uuid4())[:8]
    col = get_textract_job_collection()  # ✅ initialize collection once

    try:
        # --- Try to claim (acts as distributed lock) ---
        claimed = try_claim_processing(file_id, owner_id)
        if not claimed:
            # Another process already created the record
            existing = fetch_job_record(file_id)
            if existing:
                status = existing.get("status")
                print(f"ℹ️ Existing Textract job found for {file_id}: {status}")
                if status == "SUCCEEDED":
                    print("✅ Using cached Textract result from Mongo")
                    
                    return existing.get("result", {})
                
                elif status in ["IN_PROGRESS", "CLAIMED"]:
                    job_id = existing.get("jobId")
                    if not job_id:
                        print("⏳ Waiting for jobId to be set...")
                        for _ in range(10):
                            time.sleep(3)
                            existing = fetch_job_record(file_id)
                            job_id = existing.get("jobId")
                            if job_id:
                                break
                    if job_id:
                        print(f"⏳ Polling existing JobId: {job_id}")
                        result = poll_existing_job(textract_client, job_id)
                        return result
                    else:
                        raise Exception("Job already claimed but no JobId yet (timeout)")
                    
                elif status == "FAILED":
                    print("⚠️ Retrying after previous failed attempt")

                    # Instead of trying to insert again, update existing record
                    col.update_one(
                        {"_id": file_id},
                        {
                            "$set": {
                                "status": "CLAIMED",
                                "owner": owner_id,
                                "jobId": None,
                                "result": None,
                                "updatedAt": datetime.utcnow()
                            },
                            "$inc": {"attempts": 1}
                        }
                    )
                    print(f"🔁 Re-claimed file {file_id} after failure")

            else:
                raise Exception("Could not find or claim textract_jobs record.")

        # --- This process is the new owner ---
        temp_key = copy_to_temp_bucket(bucket, key, temp_bucket, region)
        if not temp_key:
            raise Exception("Failed to copy to temp bucket")

        print("📄 Starting Textract Document Analysis...")
        start_resp = textract_client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": temp_bucket, "Name": temp_key}},
            FeatureTypes=["TABLES"],
        )
        job_id = start_resp["JobId"]
        print(f"🎯 Job ID: {job_id}")

        # Save job start
        set_job_started(file_id, job_id)

        # Poll for completion
        status = "IN_PROGRESS"
        job_output = None
        while status == "IN_PROGRESS":
            time.sleep(5)
            resp = textract_client.get_document_analysis(JobId=job_id)
            status = resp.get("JobStatus", status)
            print(f"... Status: {status}")
            if status in ["SUCCEEDED", "FAILED"]:
                job_output = resp
                break

        if status != "SUCCEEDED":
            set_job_failed(file_id, f"Textract job failed: {status}")
            raise Exception(f"Textract failed with status {status}")

        # Collect all pages
        results = job_output.get("Blocks", [])
        next_token = job_output.get("NextToken")
        while next_token:
            next_resp = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
            results.extend(next_resp.get("Blocks", []))
            next_token = next_resp.get("NextToken")

        textract_json = {
            "Blocks": results,
            "DocumentMetadata": job_output.get("DocumentMetadata", {})
        }

        normalized_data = normalize_textract_response(textract_json)
        
        page_count = textract_json["DocumentMetadata"].get("Pages", 0)
        final_output = {
            "page_count": page_count,
            "normalized_data": normalized_data,
            "raw_textract": textract_json
        }

        set_job_succeeded(file_id, final_output, page_count)
        print("✅ Textract job succeeded and stored in Mongo")
        return final_output

    except Exception as e:
        print(f"❌ run_textract_with_lock error: {e}")
        traceback.print_exc()
        set_job_failed(file_id, str(e))
        return {}

    finally:
        cleanup_temp_bucket(temp_bucket, temp_key, region)
