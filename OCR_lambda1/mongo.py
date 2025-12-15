from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Dict, Any
import traceback
import json
import re

from config import MONGO_URI, DB_NAME, FILE_DETAILS_COLLECTION, CREDIT_COLLECTION

# --- Initialize MongoDB Client ---
mongo_client = MongoClient(MONGO_URI)


# ======================================================
# 📄 Textract Job Management
# ======================================================

def get_textract_job_collection():
    db = mongo_client[DB_NAME]
    return db["tb_textract_jobs"]


def try_claim_processing(file_id, owner):
    """Attempt to claim a file for processing (insert new job record)."""
    try:
        col=get_textract_job_collection()
        col.insert_one({
            "_id": file_id,  # _id serves as file_id
            "owner": owner,
            "status": "CLAIMED",
            "jobId": None,
            "result": None,
            "attempts": 0,
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        })
        print(f"✅ Claimed file {file_id}")
        return True
    except DuplicateKeyError:
        print(f"⚠️ File {file_id} already exists, reusing existing job.")
        return False


def fetch_job_record(file_id: str):
    col = get_textract_job_collection()
    return col.find_one({"_id": file_id})


def set_job_started(file_id: str, job_id: str):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": file_id},
        {
            "$set": {
                "jobId": job_id,
                "status": "IN_PROGRESS",
                "updatedAt": datetime.utcnow(),
            },
            "$inc": {"attempts": 1},
        },
        return_document=ReturnDocument.AFTER,
    )


def set_job_succeeded(file_id: str, result: dict, page_count: int):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": file_id},
        {
            "$set": {
                "status": "SUCCEEDED",
                "result": result,
                "page_count": page_count,
                "updatedAt": datetime.utcnow(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


def set_job_failed(file_id: str, error_msg: str):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": file_id},
        {
            "$set": {
                "status": "FAILED",
                "error": error_msg,
                "updatedAt": datetime.utcnow(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


# ======================================================
# 🗃️ Mongo Utilities
# ======================================================

def get_mongo_collection(collection_name):
    """Returns a MongoDB collection handle."""
    db = mongo_client[DB_NAME]
    return db[collection_name]


# ======================================================
# 📋 Requested Fields Handling
# ======================================================

def fetch_requested_fields(user_id, cluster_id):
    """
    Fetch and normalize requested fields for a given user and cluster.

    - For 'fieldType' = 'field', return as-is.
    - For 'fieldType' = 'table', expand each subfield inside 'tableData'
      as individual entries with reference to their table name.
    """
    collection = get_mongo_collection("tb_clusters")
    query = {"userId": ObjectId(user_id), "_id": ObjectId(cluster_id)}
    projection = {"requestedFields": 1, "_id": 0}
    doc = collection.find_one(query, projection)

    if not doc:
        print(f"⚠️ No requested fields found for user {user_id}, cluster {cluster_id}")
        return []

    requested_fields = doc.get("requestedFields", [])
    normalized_fields = []

    for field in requested_fields:
        field_type = field.get("fieldType", "field")
        field_name = field.get("fieldName", "").strip()
        if not field_name:
            continue  # skip invalid fields

        if field_type == "field":
            normalized_fields.append({
                "fieldType": "field",
                "fieldName": field_name,
                "fieldDatatype": field.get("fieldDatatype", "String"),
                "fieldDescription": field.get("fieldDescription", ""),
                "fieldExample": field.get("fieldExample", ""),
            })
        elif field_type == "table":
            table_name = field_name
            table_data = field.get("tableData", [])
            if not table_data:
                print(f"⚠️ Table '{table_name}' has no columns defined.")
                continue

            for col in table_data:
                col_name = col.get("fieldName", "").strip()
                if not col_name:
                    continue
                normalized_fields.append({
                    "fieldType": "table",
                    "tableName": table_name,
                    "fieldName": col_name,
                    "fieldDatatype": col.get("fieldDatatype", "String"),
                    "fieldDescription": col.get("fieldDescription", ""),
                    "fieldExample": col.get("fieldExample", ""),
                })

    return normalized_fields


# ======================================================
# 📄 Extracted Text Retrieval
# ======================================================

def fetch_extracted_text(user_id, cluster_id, file_id):
    collection = get_mongo_collection("tb_file_details")
    query = {
        "_id": ObjectId(file_id),
        "userId": ObjectId(user_id),
        "clusterId": ObjectId(cluster_id),
    }
    projection = {
        "extractedField": 1,
        "originalS3File": 1,
        "pageCount": 1,
        "originalFile": 1,
        "normalized_data": 1,
    }

    doc = collection.find_one(query, projection)
    if not doc:
        return None, None, None, None

    return (
        doc.get("extractedField"),
        doc.get("originalS3File"),
        doc.get("pageCount"),
        doc.get("normalized_data"),
    )


def mark_file_as_failed(doc_id):
    collection = get_mongo_collection(FILE_DETAILS_COLLECTION)
    collection.update_one(
        {"_id": ObjectId(doc_id)},
        {"$set": {
            "processingStatus": "Failed",
            "updatedAt": datetime.now(timezone.utc),
        }}
    )


# ======================================================
# 🧭 Job Status Management
# ======================================================

def update_job_status(job_id: str, status: str, summary: dict = None, message: str = None):
    """Insert or update job status."""
    collection = get_mongo_collection("job_status")
    doc = {
        "job_id": job_id,
        "status": status,
        "updatedAt": datetime.now(timezone.utc),
    }
    if summary:
        doc["summary"] = summary
    if message:
        doc["message"] = message

    collection.update_one({"job_id": job_id}, {"$set": doc}, upsert=True)


def fetch_job_status(job_id: str):
    """Retrieve job status by job_id."""
    collection = get_mongo_collection("job_status")
    return collection.find_one({"job_id": job_id}, {"_id": 0})


# ======================================================
# 🤖 LLM Field Extraction
# ======================================================
def extract_fields_with_llm(full_text: str, requested_fields_raw: list[dict],
                            agent: "AzureLLMAgent", context: any = None) -> list:
    """
    Extract table values row-by-row using AzureLLMAgent.

    ✅ Each row -> [["Number"], ["Count"], ...]
    ✅ Skips category/header rows like "DESSERT & FORTI", "RED WINE (Categ...)".
    ✅ Returns "NA" for missing values.
    ✅ Preserves decimals (e.g., 6.25) and '&' in text.
    ✅ Output: list of rows (each row = list of [value] for each requested field).
    """
    import json, re

    # Build list of field names in order
    field_names = [f.get("fieldName", "").strip() for f in requested_fields_raw if f.get("fieldName")]

    if not field_names:
        return [[["NA"]]]

    # --- Build unified prompt for multi-column extraction ---
    prompt = f"""
You are a data extraction expert. The following OCR text contains tabular data.

=====================
📄 OCR TEXT:
=====================
{full_text}

=====================
📋 TASK:
=====================
Extract the following columns (in this order): {", ".join(field_names)}.

=====================
📘 RULES:
=====================
1. Each line in the OCR text represents one row.
2. Return one array for each valid row.
   - Each row = list of column values in order.
   - Example: [["WDF001"], ["5"]] for one row.
3. Skip rows that are category or header titles, such as:
   - "DESSERT & FORTI | INE (Category: DFW)"
   - "FINE WINE (Cate | FIW)"
   - "RED WINE (Categ | DW)"
4. Preserve "&" (e.g., "DESSERT & FORTI" stays as-is).
5. Preserve decimals (e.g., 6.25 must not split).
6. If any value in a row is missing, replace it with "NA".
7. Keep rows in order.
8. Return only valid JSON — an array of arrays.
   Example Output:
   [
     [["DESSERT & FORTI"], ["NA"]],
     [["WDF001"], ["5"]],
     [["WDF009"], ["2.5"]]
   ]
Return only the JSON, nothing else.
"""

    # --- Run LLM once for all fields ---
    try:
        value = agent.complete(prompt, context=context) if context else agent.complete(prompt)
    except Exception as e:
        print(f"⚠️ LLM extraction failed: {e}")
        return [[["NA"]]]

    # --- Parse JSON safely ---
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            raise ValueError("Not a list")

        clean_rows = []
        for row in parsed:
            if not isinstance(row, list):
                continue

            # Clean each cell
            clean_cells = []
            for cell in row:
                if isinstance(cell, list) and cell:
                    val = str(cell[0]).strip()
                else:
                    val = str(cell).strip()

                # Replace blanks or invalids with NA
                if not val or val.lower() in ("", "none", "null", "na", "n/a"):
                    val = "NA"

                # Skip headers if the entire row is just a header
                if re.search(r"(Category|Categ|Section|WINE|DESSERT|BEVERAGE|FOOD)", val, re.IGNORECASE):
                    val = "NA"

                clean_cells.append([val])

            # Skip rows that are all "NA"
            if all(c[0] == "NA" for c in clean_cells):
                continue

            clean_rows.append(clean_cells)

        return clean_rows or [[["NA"]]]

    except Exception:
        # --- Fallback: simple parsing if LLM output invalid ---
        clean_rows = []
        lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

        for ln in lines:
            # Skip category/header lines
            if re.search(r"(Category|Categ|Section|WINE|DESSERT|BEVERAGE|FOOD)", ln, re.IGNORECASE):
                continue

            # Split by | or 2+ spaces
            parts = [p.strip() for p in re.split(r"\|{1,}|\s{2,}", ln) if p.strip()]

            if not parts:
                continue

            # Fill with NA up to field count
            while len(parts) < len(field_names):
                parts.append("NA")

            row = []
            for val in parts[:len(field_names)]:
                val = re.sub(r"[^\w&\.\-\s]", "", val).strip()
                if not val:
                    val = "NA"
                row.append([val])

            clean_rows.append(row)

        return clean_rows or [[["NA"]]]





# ======================================================
# 🧾 Update Extracted Values to MongoDB
# ======================================================

def update_extracted_values_to_mongo(
    user_id,
    cluster_id,
    doc_id,
    fields,
    extracted_field_list,
    full_text
):
    """
    ✅ Update extracted values to MongoDB.
    Correctly aligns table columns row-by-row.

    - Groups table columns into { "fieldType": "table", "items": [ ... ] } format.
    - Ensures Number[i] matches Count[i].
    - Supports nested list structure like [["BK0167"]] instead of ["BK0167"].
    """
    collection = get_mongo_collection("tb_file_details")
    filter_query = {"_id": ObjectId(doc_id)}

    field_data = {}
    table_columns = {}

    # --- Collect fields ---
    for idx, field in enumerate(fields):
        field_type = field.get("fieldType", "field")
        field_name = field.get("fieldName")
        table_name = field.get("tableName")
        value = extracted_field_list[idx] if idx < len(extracted_field_list) else [["NA"]]

        # Normalize everything to list of lists
        if not isinstance(value, list):
            value = [[value]]
        else:
            # Ensure each item is a list (nested [["v"]])
            value = [[v] if not isinstance(v, list) else v for v in value]

        # --- Normal field ---
        if field_type == "field" or not table_name:
            field_data[field_name] = value

        # --- Table field ---
        else:
            if table_name not in table_columns:
                table_columns[table_name] = {}
            table_columns[table_name][field_name] = value

    # --- Build final Mongo structure ---
    final_update = {}

    # Add normal (non-table) fields
    final_update.update(field_data)

    # --- Process each table group ---
    for table_name, columns in table_columns.items():
        # Get all columns and their value counts
        max_rows = max((len(v) for v in columns.values()), default=0)
        items = []

        for i in range(max_rows):
            row = {}
            for col_name, col_values in columns.items():
                # Align by index — fill with NA if shorter
                if i < len(col_values):
                    val = col_values[i]
                else:
                    val = ["NA"]

                # Ensure nested [["value"]] format
                if not isinstance(val, list):
                    val = [[val]]
                elif not isinstance(val[0], list):
                    val = [val]

                row[col_name] = val
            items.append(row)

        final_update[table_name] = {
            "fieldType": "table",
            "items": items
        }

    # --- Safe full_text serialization ---
    if not isinstance(full_text, str):
        full_text = json.dumps(full_text, ensure_ascii=False)

    update_query = {
        "$set": {
            "extractedValues": final_update,
            "updatedExtractedValues": final_update,
            "processingStatus": "Completed",
            "extractedText": full_text,
            "updatedAt": datetime.now(timezone.utc),
        }
    }

    result = collection.update_one(filter_query, update_query, upsert=True)
    print("✅ Extracted values stored successfully in MongoDB.")
    return {
        "status": "success" if result.modified_count > 0 else "no-change",
        "storedData": final_update
    }


# ======================================================
# 💳 Credit Management
# ======================================================

def insert_debit_credit(user_id, cluster_id, field_id, credits_to_deduct, job_id, credit_id):
    """
    Update the credit record's 'updatedAt' timestamp by creditId.
    If no record found, returns 'not-found'.
    """
    collection = get_mongo_collection(CREDIT_COLLECTION)

    try:
        if not credit_id:
            print("⚠️ Missing creditId — cannot update credit record.")
            return {"status": "error", "message": "Missing creditId"}

        result = collection.update_one(
            {"_id": ObjectId(credit_id)},
            {"$set": {"updatedAt": datetime.now(timezone.utc), "type": "debited"}},
        )

        if result.matched_count == 0:
            print(f"⚠️ No record found with creditId: {credit_id}")
            return {"status": "not-found", "message": f"No record found for creditId {credit_id}"}

        print(f"✅ Credit record {credit_id} updated successfully for user {user_id}")
        return {"status": "success", "message": f"Updated credit record {credit_id}"}

    except Exception as e:
        print(f"❌ Error updating credit record {credit_id}: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


def delete_credit_record(credit_id):
    """
    Delete a credit record from the CREDIT_COLLECTION using creditId.
    Returns a status dictionary.
    """
    collection = get_mongo_collection(CREDIT_COLLECTION)

    try:
        if not credit_id:
            print("⚠️ Missing creditId — cannot delete credit record.")
            return {"status": "error", "message": "Missing creditId"}

        result = collection.delete_one({"_id": ObjectId(credit_id)})

        if result.deleted_count == 0:
            print(f"⚠️ No credit record found with creditId: {credit_id}")
            return {"status": "not-found", "message": f"No record found for creditId {credit_id}"}

        print(f"✅ Credit record {credit_id} deleted successfully")
        return {"status": "success", "message": f"Deleted credit record {credit_id}"}

    except Exception as e:
        print(f"❌ Error deleting credit record {credit_id}: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}
