import os
import traceback
from uuid import uuid4
from datetime import datetime, timezone

from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

PROD_MONGO_URI = os.getenv("PROD_MONGO_URI")
if not PROD_MONGO_URI:
    raise RuntimeError("Missing ENV: PROD_MONGO_URI")

mongo_client = MongoClient(PROD_MONGO_URI)
mongo_database = os.getenv("MONGO_DATABASE")
db = mongo_client[mongo_database]

# Direct collection access (no helper function)
tb_credits = db["tb_credits"]

tb_file_details=db["tb_file_details"]


# ---------------------------------------------------------
# 1️⃣ UPDATE CREDIT RECORD (Replace old insert_debit_credit)
# ---------------------------------------------------------

def update_debit_credit(
    user_id, 
    cluster_id, 
    file_id, 
    credits_to_deduct, 
    job_id, 
    credit_id,
    message="Success"     # <-- added optional parameter for successMessage
):
    """
    Updates the credit record ('type' and 'updatedAt') using creditId.
    Also updates file details after successful debit update.
    """

    if not credit_id:
        raise ValueError("creditId is required but missing")

    try:
        # ---------------- OLD LOGIC (unchanged) ----------------
        result = tb_credits.update_one(
            {"_id": ObjectId(credit_id)},
            {
                "$set": {
                    "updatedAt": datetime.now(timezone.utc),
                    "type": "debited"
                }
            }
        )

        if result.matched_count == 0:
            raise LookupError(f"No credit record found for creditId={credit_id}")

        print(f"✅ Updated credit record {credit_id}")

        # ---------------- NEW FILE DETAILS UPDATE ----------------
        if file_id:
            file_update_result = tb_file_details.update_one(
                {"_id": ObjectId(file_id)},
                {
                    "$set": {
                        "processingStatus": "Completed",
                        "successMessage": message,
                        "updatedAt": datetime.utcnow().isoformat() + "Z"
                    }
                }
            )

            print(f"✅ Marked SUCCESS for file {file_id}, modified: {file_update_result.modified_count}")
        else:
            print("⚠️ file_id is missing, skipping file details update.")

        return {
            "status": "success",
            "creditId": credit_id,
            "fileId": file_id
        }

    except Exception as e:
        print(f"❌ Error updating creditId={credit_id}: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------
# 2️⃣ DELETE CREDIT RECORD
# ---------------------------------------------------------

def delete_credit_record(credit_id, file_id=None):
    """
    Deletes a credit record by creditId.
    Also updates file details status to 'Failed' if file_id is provided.
    Raises errors when creditId missing or not found.
    """

    if not credit_id:
        raise ValueError("creditId is required but missing")

    try:
        # ---------- OLD LOGIC (unchanged) ----------
        result = tb_credits.delete_one({"_id": ObjectId(credit_id)})

        if result.deleted_count == 0:
            raise LookupError(f"No credit record found for creditId={credit_id}")

        print(f"🗑️ Deleted credit record {credit_id}")

        # ---------- NEW FILE-DETAILS UPDATE ----------
        if file_id:
            file_update_result = tb_file_details.update_one(
                {"_id": ObjectId(file_id)},
                {
                    "$set": {
                        "processingStatus": "Failed",
                        "updatedAt": datetime.utcnow().isoformat() + "Z"
                    }
                }
            )

            print(f"⚠️ Marked FAILED for file {file_id}, modified: {file_update_result.modified_count}")
        else:
            print("⚠️ file_id not provided, skipping file update.")

        return {
            "status": "success",
            "creditId": credit_id,
            "fileId": file_id
        }

    except Exception as e:
        print(f"❌ Error deleting creditId={credit_id}: {e}")
        traceback.print_exc()
        return {"status": 'error', "message": str(e)}

