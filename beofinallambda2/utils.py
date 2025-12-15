from datetime import datetime, timezone
import re

jobs = {}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def detect_currency(text: str) -> str:
    matches = re.findall(r"\b(AED|USD|INR|EUR|GBP)\b", text, flags=re.IGNORECASE)
    if matches:
        return matches[0].upper()
    return "INR"

def update_job_status(job_id, status, message=None, data=None):
    jobs[job_id] = {
        "jobId": job_id,
        "status": status,
        "message": message,
        "data": data,
        "updatedAt": utc_now_iso(),
    }
    print(f"[JOB STATUS] {job_id} → {status}, msg={message}, data={data}")
