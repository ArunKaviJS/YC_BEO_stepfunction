import json
import re
import time
import httpx
from openai import AzureOpenAI, RateLimitError
import os
from dotenv import load_dotenv
load_dotenv()

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")


class AzureLLMAgent:
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_OPENAI_API_VERSION,
            http_client=httpx.Client(),
        )
        self.model = AZURE_OPENAI_DEPLOYMENT
        self.RateLimitError = RateLimitError

    def complete(self, prompt: str) -> str:
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an expert floral invoice processing system that extracts structured "
                            "invoice details (vendor, invoice number, date, items). Always return valid JSON only."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=4000,
                temperature=0.1,
            )
            content = resp.choices[0].message.content
            print(f"[LLM COMPLETE] prompt_len={len(prompt)} → resp_len={len(content)}")
            return content.strip()
        except self.RateLimitError:
            print("⚠️ Rate limit hit. Retrying after 5 seconds…")
            time.sleep(5)
            return self.complete(prompt)
        except Exception as e:
            print(f"❌ LLM Error: {e}")
            return "{}"

    def _parse_code_desc(self, text: str, fallback_code: str = None, fallback_desc: str = None):
        code, desc = "", ""
        for part in text.split("|"):
            part = part.strip()
            if part.lower().startswith("code:"):
                code = part.replace("code:", "").strip()
            elif part.lower().startswith("desc:"):
                desc = part.replace("desc:", "").strip()
        product_code = code.upper() if code else fallback_code
        item_desc = desc if desc else (fallback_desc or text)
        return product_code, item_desc

    def build_prompt(self, extracted_text: str) -> str:
        schema = {
            "eventName": "Event Name",
            "billTo": "Address to whom the bill/order is issued",
            "invoiceDate": None,
            "invoiceNo":None,
            "beoNumber": None,
            "eventDate": None,
            "attentionTo": None,
            "items": [
                {
                    "tableType": "Exact section or table title found in the document — only 'Food' or 'Resources'",
                    "itemDescription": "Package name or line item that appears as a row with quantity/amount — not sub-items or ingredients",
                    "quantity": None,
                    "unitPrice": 0.00,
                    "totalAmount": 0.00,
                    "currency": "AED",
                    "matchConfidence": 0.00
                }
            ],
        }

        return (
            "You are an expert Banquet Event Order (BEO) and event invoice data extraction system.\n"
            "Return ONLY valid JSON (no explanations, no markdown formatting like ```json or ```).\n\n"
            "⚙️ **Extraction Rules:**\n"
            "- Always return valid JSON using the schema below.\n"
            "- Use null for missing or unknown values.\n"
            "- Correct OCR or spelling mistakes if present.\n"
            "- Currency must always be 'AED'.\n"
            "- Dates must be in ISO format: YYYY-MM-DD.\n\n"
            "🎯 **Extract the following fields:**\n"
            "1️⃣ eventName → Name of the event (e.g., 'London Business School Event').\n"
            "2️⃣ billTo → Billing address or organization name.\n"
                "- Compare the Event Name and the Address. If they share a common organization word (e.g., 'Aucta'), ensure a comma is placed immediately after that shared word in the final billTo output.\n"
                "- Example: Event Name = 'Aucta Event', Address = 'Aucta Quaterdeck, QE2 Dubai...', then billTo should start as 'Aucta, Quaterdeck, QE2 Dubai...'\n"
                "- Never duplicate the shared word; simply add the comma after the common prefix.\n"
                "- Clean the address by fixing spacing and ensuring commas separate logical segments.\n"
            "3️⃣ invoiceDate → Always use today’s date (ignore invoice text date).\n"
            "4️⃣ invoiceNo → ALWAYS return None.\n"

            "5️⃣ beoNumber → Banquet Event Order Number or Invoice Number.\n"
            "6️⃣ attentionTo → Extract the main contact person’s full name.\n"
                    "- Handle cases where the line contains multiple labels such as 'Contact Name:'.\n"
                    "- If multiple names appear (e.g., 'Casper Hammer Maryann Chukwurah'), choose the **first full person name**.\n"
                    "- A valid full name is typically (e.g., 'Casper Hammer').\n"
            "7️⃣ eventDate → Date of the event, found near words like 'BEO Date', 'Event Date', etc. Convert to ISO format YYYY-MM-DD.\n"
            "   - If multiple event dates exist, pick the earliest.\n\n"
            "📋 **Items Extraction:**\n"
            "- Each `tableType` must be either **'Food'** or **'Resources'**.\n"
            "- These values must match the **exact section titles found in the document**.\n"
            "- Do NOT invent or assume any table names outside these two.\n"
            "- For each section, extract all valid line items under it.\n"
            "- A valid line item is usually a single line that contains a description and optionally numeric columns like quantity, unit price, or total.\n"
            "- ✅ Include lines such as '6 PIECES CANAPES PACKAGE', '2 HOURS SPIRITS, WINE & BEER PCKG', 'Venue Rental', 'AV Equipment'.\n"
            "- ❌ Ignore lines that are sub-items, ingredients, or extra descriptive text (e.g., 'Mini Pizzetta Margherita', 'Beef Gyoza').\n"
            "- Use null for numeric values if not present.\n\n"
            "🧠 **Grouping Rules:**\n"
            "1. Each `itemDescription` belongs to the **closest previous section name** (table title) found in the text.\n"
            "2. If an item appears without any visible section title (e.g., on a new page), **inherit the last detected tableType** from the previous item.\n"
            "3. Only inherit if it logically follows the previous items — do not create or guess new table names.\n"
            "4. Never assign a `tableType` that does not literally appear somewhere in the full extracted text.\n\n"
            "Example:\n"
            "Text:\n"
            "Food\n"
            "6 PIECES CANAPES PACKAGE    100     140.00     14000.00\n"
            "(page break)\n"
            "Mini Saffron Arancino with Chicken    50    120.00    6000.00\n"
            "Food\n"
            "2 HOURS SPIRITS, WINE & BEER PCKG   1   250.00   250.00\n\n"
            "Output JSON:\n"
            "{\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"tableType\": \"Food\",\n"
            "      \"itemDescription\": \"6 PIECES CANAPES PACKAGE\",\n"
            "      \"quantity\": 100.0,\n"
            "      \"unitPrice\": 140.0,\n"
            "      \"totalAmount\": 14000.0,\n"
            "      \"currency\": \"AED\",\n"
            "      \"matchConfidence\": 1.0\n"
            "    },\n"
            "    {\n"
            "      \"tableType\": \"Food\",\n"
            "      \"itemDescription\": \"Mini Saffron Arancino with Chicken\",\n"
            "      \"quantity\": 50.0,\n"
            "      \"unitPrice\": 120.0,\n"
            "      \"totalAmount\": 6000.0,\n"
            "      \"currency\": \"AED\",\n"
            "      \"matchConfidence\": 0.9\n"
            "    },\n"
            "    {\n"
            "      \"tableType\": \"Food\",\n"
            "      \"itemDescription\": \"2 HOURS SPIRITS, WINE & BEER PCKG\",\n"
            "      \"quantity\": 1.0,\n"
            "      \"unitPrice\": 250.0,\n"
            "      \"totalAmount\": 250.0,\n"
            "      \"currency\": \"AED\",\n"
            "      \"matchConfidence\": 1.0\n"
            "    }\n"
            "  ]\n"
            "}\n\n"
            f"🧩 Expected JSON schema:\n{json.dumps(schema, indent=2)}\n\n"
            "Now extract the JSON accurately from this text:\n"
            f"{extracted_text}"
        )











    def extract_invoice_and_items(self, ocr_text: str) -> dict:
        prompt = f"""
        You are given OCR text extracted from a Banquet Event Order (BEO).
        Task:
        1) Extract the BEO Number (after 'BANQUET EVENT ORDER #').
        2) Extract all listed goods/service descriptions under the relevant table or service section.
        Return strictly in JSON format:
        {{
        "beoNumber": "<string or null>",
        "itemDescriptions": ["<desc1>", "<desc2>", "..."]
        }}
        OCR text:
        ---
        {ocr_text}
        ---
        """
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                temperature=0,
            )
            raw_text = resp.choices[0].message.content.strip()
            start, end = raw_text.find("{"), raw_text.rfind("}") + 1
            if start != -1 and end > start:
                data = json.loads(raw_text[start:end])
            else:
                match = re.search(r"\{[\s\S]*\}", raw_text)
                if match:
                    data = json.loads(match.group(0))
                else:
                    data = {"beoNumber": None, "itemDescriptions": []}
        except Exception as e:
            print(f"⚠️ LLM parsing error: {e}")
            data = {"beoNumber": None, "itemDescriptions": []}
        return data
