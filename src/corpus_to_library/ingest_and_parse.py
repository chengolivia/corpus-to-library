import concurrent.futures
import json
import os
import uuid
from typing import Optional
import boto3
import pymupdf  # PyMuPDF
from google import genai
from google.genai import types
from corpus_to_library.metadata import BibliographicMetadata
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
BUCKET_NAME = "bigml-project-corpus-900136791625-us-east-2-an"
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
OUTPUT_JSONL = f"extracted_metadata_{timestamp}.jsonl"
S3_OUTPUT_KEY = f"extracted_metadata/{OUTPUT_JSONL}"  # Target S3 destination path
MODEL_ID = "gemini-3.5-flash-lite"

PROMPT = """
Extract the bibliographic metadata from this document slice.
Only include information explicitly supported by the document.
Use null for missing values. Do not guess.
"""

# Initialize Clients
s3 = boto3.client("s3")
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])


# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_pdf_slice(pdf_bytes: bytes) -> bytes:
    """
    Slices PDF based on page count rule:
    - total pages < 10  -> first 3 pages (indices 0..2)
    - total pages >= 10 -> first 5 pages (indices 0..4)
    """
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    total_pages = len(doc)
    
    if total_pages < 10:
        max_pages = min(3, total_pages)
    else:
        max_pages = min(5, total_pages)

    sliced_doc = pymupdf.open()
    sliced_doc.insert_pdf(doc, from_page=0, to_page=max_pages - 1)
    
    sliced_bytes = sliced_doc.tobytes()
    sliced_doc.close()
    doc.close()
    return sliced_bytes


def process_single_pdf(key: str) -> Optional[dict]:
    """Worker task: Download, generate UUID, slice, parse via Gemini."""
    try:
        # 1. Download PDF from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        pdf_bytes = response["Body"].read()
        
        # 2. Identifiers
        doc_uuid = str(uuid.uuid4())
        doc_id = f"doc_{doc_uuid[:12]}"  # 12 hex characters
        pdf_name = os.path.basename(key)
        
        # 3. Slice PDF
        slice_bytes = get_pdf_slice(pdf_bytes)
        
        # 4. Gemini API Call with Structured Output
        api_response = client.models.generate_content(
            model=MODEL_ID,
            contents=[
                PROMPT,
                types.Part.from_bytes(
                    data=slice_bytes,
                    mime_type="application/pdf"
                )
            ],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=BibliographicMetadata,
                temperature=0.0,
            )
        )
        
        parsed_metadata = json.loads(api_response.text)
        
        # 5. Combine metadata
        return {
            "document_id": doc_id,
            "uuid": doc_uuid,
            "pdf_name": pdf_name,
            "s3_key": key,
            **parsed_metadata
        }

    except Exception as e:
        print(f"Error processing {key}: {e}")
        return None


# ==========================================
# MAIN PIPELINE
# ==========================================
def run_pipeline(max_docs: Optional[int] = None, num_threads: int = 5):
    """
    Runs the processing pipeline using multithreaded execution and uploads output to S3.
    
    :param max_docs: Maximum number of PDF documents to process (None for all).
    :param num_threads: Number of parallel worker threads / API calls.
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME)
    
    # Gather target S3 PDF keys
    pdf_keys = []
    for page in pages:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                pdf_keys.append(key)
                if max_docs is not None and len(pdf_keys) >= max_docs:
                    break
        if max_docs is not None and len(pdf_keys) >= max_docs:
            break

    total_to_process = len(pdf_keys)
    print(f"Found {total_to_process} PDFs. Processing with {num_threads} parallel threads...")

    processed_count = 0
    with open(OUTPUT_JSONL, "a", encoding="utf-8") as f_out:
        # Process in chunks of num_threads
        for i in range(0, total_to_process, num_threads):
            chunk = pdf_keys[i : i + num_threads]
            
            # Concurrently dispatch chunk to thread pool
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                results = list(executor.map(process_single_pdf, chunk))
            
            # Write valid results to disk
            for record in results:
                if record:
                    f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    processed_count += 1
            
            f_out.flush()
            print(f"Batch {i // num_threads + 1} complete. ({processed_count}/{total_to_process} total records written)")

    print(f"\nLocal processing finished! Wrote {processed_count} records to {OUTPUT_JSONL}.")

    # ==========================================
    # S3 UPLOAD STEP
    # ==========================================
    if processed_count > 0:
        print(f"Uploading {OUTPUT_JSONL} to s3://{BUCKET_NAME}/{S3_OUTPUT_KEY}...")
        s3.upload_file(OUTPUT_JSONL, BUCKET_NAME, S3_OUTPUT_KEY)
        print("Upload complete!")


if __name__ == "__main__":
    run_pipeline(max_docs=5, num_threads=5)