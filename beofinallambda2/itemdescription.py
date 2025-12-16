
import os
import re
import time
import json
import traceback
from uuid import uuid4
from datetime import datetime, timezone
import boto3
import httpx
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from datetime import datetime, timezone
from utils import utc_now_iso, detect_currency, update_job_status, jobs

from azure_llm_agent import AzureLLMAgent



invoice_number_var = None


def itemdescription_function(extracted_text: str):
    global invoice_number_var
    agent = AzureLLMAgent()

    prompt = agent.build_prompt(extracted_text)
    structured_json_text = agent.complete(prompt)

    canon = agent.extract_invoice_and_items(extracted_text)
    canon_beo_no = canon.get("beoNumber") or canon.get("invoiceNumber")
    canon_items = canon.get("itemDescriptions", []) or []

    currency_detected = detect_currency(extracted_text)
     # ✅ NEW: extract BEO date

    try:
        s = structured_json_text.strip()
        if s.startswith("```"):
            s = s.strip("`")
            if s.lower().startswith("json"):
                s = s[4:].strip()

        parsed = json.loads(s) if s else {}
        if not isinstance(parsed, dict):
            parsed = {}

        # ✅ Always set invoiceDate = today's date (ignore extracted value)
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        parsed.setdefault("eventName", None)
        parsed.setdefault("billTo", None)
        parsed["invoiceDate"] = current_date
        parsed.setdefault("beoNumber", canon_beo_no)
        parsed.setdefault("eventDate")  # ✅ NEW FIELD
        parsed.setdefault("attentionTo", None)
        
        # ✅ NEW: always include invoiceNo = None
        parsed.setdefault("invoiceNo", None)

        # Remove unwanted legacy keys
        for old_key in ["vendorName", "invoiceNo", "supplierName"]:
            parsed.pop(old_key, None)

        # ✅ Handle items
        if not parsed.get("items"):
            parsed["items"] = []
            for i, desc in enumerate(canon_items):
                parsed["items"].append({
                    "itemDescription": desc,
                    "quantity": None,
                    "unitPrice": 0.00,
                    "totalAmount": 0.00,
                    "currency": currency_detected,
                    "matchConfidence": 0.00,
                    "placeholder": f"object{i}"
                })
        else:
            normalized_items = []
            for i, item in enumerate(parsed["items"]):
                normalized = {}
                for k, v in item.items():
                    key_lower = k.lower()
                    if key_lower == "itemdescription":
                        normalized["itemDescription"] = v
                    elif key_lower == "tabletype":
                        normalized["tableType"] = v
                    else:
                        normalized[k] = v

                normalized.setdefault("tableType", "Unknown")
                normalized["currency"] = currency_detected
                normalized["placeholder"] = f"object{i}"
                normalized_items.append(normalized)

            parsed["items"] = normalized_items

        # Store global variable
        invoice_number_var = parsed.get("beoNumber")

        # Defaults for eventName and billTo
        if not parsed.get("eventName"):
            parsed["eventName"] = "London Business School Event"
        if not parsed.get("billTo"):
            parsed["billTo"] = (
                "London Business School\n"
                "Dubai International Financial Centre (DIFC)\n"
                "The Academy, Gate Village 2, Level 3\n"
                "PO Box 506630, Dubai, UAE"
            )

        print(
            f"[STRUCTURED] Event={parsed.get('eventName')} "
            f"BEO={parsed.get('beoNumber')} "
            f"EventDate={parsed.get('eventDate')} "
            f"InvoiceDate={parsed.get('invoiceDate')} "
            f"BillTo={parsed.get('billTo')} "
            f"Items={len(parsed.get('items', []))}"
        )

        return parsed

    except Exception as e:
        print(f"⚠️ Could not parse BEO JSON: {e}")
        return {
            "rawStructured": structured_json_text,
            "eventName": "London Business School Event",
            "billTo": (
                "London Business School\n"
                "Dubai International Financial Centre (DIFC)\n"
                "The Academy, Gate Village 2, Level 3\n"
                "PO Box 506630, Dubai, UAE"
            ),
            "beoNumber": canon_beo_no,
            "eventDate": None,  # ✅ Include in fallback too
            "invoiceDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "itemDescriptions": canon_items,
            "currency": currency_detected,
        }

        
from bson import ObjectId

def generate_invoice_number(db, invoice_date: str, user_id: str):
    """
    Generate next invoice number for a specific user:
    - Start at 0172
    - Next = 0172 + count(existing invoiceNos for that user)
    """

    PREFIX = "PFI"

    # --- Build Year Code ---
    try:
        year = int(invoice_date.split("-")[0])
        year_code = f"E{str(year)[-2:]}"   # 2025 -> E25
    except:
        raise Exception("Invalid invoiceDate format; expected YYYY-MM-DD")

    # --- Count all documents with invoiceNo belonging to this user ---
    count_existing = db["tb_file_details"].count_documents({
        "userId": ObjectId(user_id),
        "status": "1",  # IMPORTANT
        "extractedValues.invoiceNo": {"$exists": True}
    })

    base = 173  # Start point

    # First invoice for this user
    if count_existing == 0:
        seq = base
    else:
        seq = base + count_existing

    seq_str = str(seq).zfill(4)
    
    invoice_no = f"{PREFIX}-{year_code}-{seq_str}"
    
    print(f"[DEBUG] status=1 invoice count: {count_existing}")

    print(f"[DEBUG] Generated invoiceNo: {invoice_no}")

    return f"{PREFIX}-{year_code}-{seq_str}"


