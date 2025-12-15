import os
import json
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- MongoDB Connection ---
MONGO_URI = os.getenv("PROD_MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["yc-invoice"]
col_files = db["tb_file_details"]

def lambda_handler(event, context):
    """
    ✅ Mark the Mongo document as SUCCESS.
    Expected event format:
    {
        "fileId": "68e62784866151ede43ab136",
        "message": "Processing completed successfully"
    }
    """
    try:
        file_id = event.get("fileId")
        message = event.get("message", "Processing completed successfully")

        if not file_id:
            raise ValueError("Missing 'fileId' in event")

        # --- Update MongoDB ---
        result = col_files.update_one(
            {"_id": ObjectId(file_id)},
            {
                "$set": {
                    "processingStatus": "Completed",
                    "successMessage": message,
                    "updatedAt": datetime.utcnow().isoformat() + "Z"
                }
            }
        )

        print(f"✅ Marked SUCCESS for file {file_id}, modified: {result.modified_count}")
        return {
            "status": "Completed",
            "fileId": file_id,
            "message": message
        }

    except Exception as e:
        print(f"🔥 Error in mark_success_lambda: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }
