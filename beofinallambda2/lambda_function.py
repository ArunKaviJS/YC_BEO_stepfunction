import json
import os
import boto3
from datetime import datetime, timezone
from pymongo import MongoClient
from bson import ObjectId

from itemdescription import itemdescription_function,generate_invoice_number
from update_credits import update_debit_credit, delete_credit_record
from utils import update_job_status

# --- ENV ---
MONGO_URI = os.getenv("PROD_MONGO_URI")
MONGO_DB = os.getenv("MONGO_DATABASE")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
col_files = db["tb_file_details"]


def lambda_handler(event, context):
    print("Incoming Event:", event)

    try:
        fileId = event["fileId"]
        userId = event["userId"]
        clusterId = event["clusterId"]
        creditId = event.get("creditId")
        text_content = event["text_content"]
        pages = event.get("pages", 1)

    except Exception as e:
        return {"error": f"Missing required fields: {e}"}

    file_oid = ObjectId(fileId)
    user_oid = ObjectId(userId)
    cluster_oid = ObjectId(clusterId)
    credit_oid = ObjectId(creditId) if creditId else None

    job_id = ObjectId()

    # --- Update job: started ---
    update_job_status(job_id, "processing", "Starting invoice extraction")

    structured = {}
    summary = {}

    try:
        
        # --- Fetch existing invoiceNo before extraction ---
        existing_invoice_no = None

        existing_doc = col_files.find_one(
            {"_id": file_oid},
            {"extractedValues.invoiceNo": 1}
        )

        if existing_doc:
            existing_invoice_no = existing_doc.get("extractedValues", {}).get("invoiceNo")

        print("[DEBUG] Existing invoiceNo:", existing_invoice_no)

        # --- Extract structured invoice items from text ---
        structured = itemdescription_function(text_content)
        print('*********')
        print("STRUCTURED ITEMS:", structured)
        print('*********')

        if not structured or not structured.get("items"):
            if credit_oid:
                print(f"Deleting credit record due to failure: {credit_oid}")
                delete_credit_record(credit_oid,file_oid)
            return {
                "pagesCount": pages,
                "summary": summary,
                "status": "no-items"
            }

        invoice_doc_update = {
            "extractedText": text_content,
            "extractedValues": structured,
            "updatedExtractedValues": structured,
            "rawStructured": structured,
            "updatedAt": datetime.now(timezone.utc),
        }

        # --- Update MongoDB with extracted invoice data ---
        update_res = col_files.update_one(
            {"_id": file_oid, "clusterId": cluster_oid, "userId": user_oid},
            {"$set": invoice_doc_update},
            upsert=False
        )

        if update_res.modified_count == 0:
            raise Exception("Mongo update failed — file not found OR no changes")

        print("[STRUCTURED] Stored structured_json in Mongo")

        # --- Deduct Credits ---
        credits_to_deduct = pages
        credit_result = update_debit_credit(
            user_oid, cluster_oid, file_oid, credits_to_deduct, job_id, credit_oid
        )
        print(f"[CREDITS] {credit_result}")
        
        # --- Final invoiceNo decision ---
        if existing_invoice_no:
            # Preserve existing invoice number
            structured["invoiceNo"] = existing_invoice_no
            print("[INFO] Reusing existing invoiceNo:", existing_invoice_no)

            col_files.update_one(
                {"_id": file_oid},
                {"$set": {
                    "extractedValues.invoiceNo": existing_invoice_no,
                    "updatedExtractedValues.invoiceNo": existing_invoice_no
                }}
            )

        else:
            # Generate only if NOT existing
            invoice_no = generate_invoice_number(
                db,
                structured.get("invoiceDate"),
                userId)

            structured["invoiceNo"] = invoice_no
            print("[INFO] Generated new invoiceNo:", invoice_no)

            col_files.update_one(
                {"_id": file_oid},
                {"$set": {
                    "extractedValues.invoiceNo": invoice_no,
                    "updatedExtractedValues.invoiceNo": invoice_no
                }}
            )


    except Exception as e:
        print(f"[ERROR] {e}")

        # Rollback credit record if update fails
        if credit_oid:
            print(f"Deleting credit record due to failure: {credit_oid}")
            delete_credit_record(credit_oid,file_oid)

        update_job_status(job_id, "failed", str(e))

        return {
            "status": "failed",
            "pagesCount": pages,
            "summary": summary,
            "error": str(e)
        }

    # --- Success ---
    update_job_status(job_id, "completed", "Invoice processed successfully")

    return {
        "status": "success",
        "pagesCount": pages,
        "summary": structured
    }
