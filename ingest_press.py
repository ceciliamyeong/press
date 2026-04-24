import base64, io, json, os
import httpx
from google import genai
import time

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
payload = json.loads(os.environ["PAYLOAD"])

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def extract_pdf(data: bytes) -> str:
    from pdfminer.high_level import extract_text_to_fp
    from pdfminer.layout import LAParams
    out = io.StringIO()
    extract_text_to_fp(io.BytesIO(data), out, laparams=LAParams())
    return out.getvalue().strip()

def extract_docx(data: bytes) -> str:
    import docx
    doc = docx.Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())

def process_attachments(attachments):
    results = []
    for att in attachments:
        raw = base64.b64decode(att["dataBase64"])
        text = ""
        if att["mimeType"] == "application/pdf":
            text = extract_pdf(raw)
        elif "wordprocessingml" in att["mimeType"]:
            text = extract_docx(raw)
        elif att["mimeType"] == "text/plain":
            text = raw.decode("utf-8", errors="ignore")
        results.append({
            "filename": att["filename"],
            "extractedText": text[:20000]
        })
    return results

def parse_with_gemini(subject, body, attachments_text):
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    combined = f"제목: {subject}\n\n본문:\n{body}\n\n첨부파일 내용:\n{attachments_text}"

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"""다음 보도자료를 분석해서 JSON으로만 응답하세요. 다른 텍스트 없이 JSON만.

{{
  "company": "발신 기업/기관명",
  "title": "정제된 한국어 제목",
  "summary_ko": "한국어 요약 3~5줄",
  "category": "listing|partnership|funding|regulation|product|event|crypto|finance|fintech|other 중 하나. 기준: listing=토큰/코인 상장, partnership=파트너십/MOU, funding=투자/펀딩, regulation=규제/정책/법률, product=신제품/서비스 출시, event=행사/컨퍼런스, crypto=크립토 프로젝트·거래소·블록체인 일반 소식, finance=은행·증권사·자산운용사·금융기관, fintech=핀테크·결제·금융IT, other=기타",
  "tokens": ["관련 토큰 심볼 배열, 없으면 []"],
  "keywords": ["핵심 키워드 5개 이내"],
  "language": "ko|en|mixed 중 하나",
  "importance_score": 1에서 5 사이 정수
}}

보도자료:
{combined[:8000]}"""
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
            raw_text = raw_text.strip()
            if not raw_text:
                raise Exception("Gemini returned empty response")
            return json.loads(raw_text)
        except Exception as e:
            if attempt < 2:
                wait = (attempt + 1) * 10
                print(f"Retry {attempt+1}, waiting {wait}s... ({e})")
                time.sleep(wait)
            else:
                raise
                
# 메인 실행
processed_atts = process_attachments(payload.get("attachments", []))
atts_text = "\n\n".join(a["extractedText"] for a in processed_atts)

# 1. raw 저장
raw_row = {
    "message_id": payload["messageId"],
    "received_at": payload["receivedAt"],
    "sender": payload["from"],
    "subject": payload["subject"],
    "body_plain": payload.get("bodyPlain", ""),
    "raw_attachments": processed_atts
}

with httpx.Client() as client:
    res = client.post(
        f"{SUPABASE_URL}/rest/v1/press_raw",
        headers={**headers, "Prefer": "return=representation"},
        json=raw_row
    )
    if res.status_code not in (200, 201):
        if "duplicate" in res.text.lower():
            print("Skipped: duplicate message_id")
            exit(0)
        raise Exception(f"Supabase raw insert failed: {res.text}")

    raw_id = res.json()[0]["id"]
    print(f"Raw saved: {raw_id}")

# 2. Gemini 파싱 + structured 저장
parsed = parse_with_gemini(payload["subject"], payload.get("bodyPlain", ""), atts_text)
parsed["raw_id"] = raw_id

with httpx.Client() as client:
    res = client.post(
        f"{SUPABASE_URL}/rest/v1/press_structured",
        headers=headers,
        json=parsed
    )
    print(f"Structured saved: {res.status_code}")
