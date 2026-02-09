import json
import asyncio
import aiohttp
import os
import sys

# CONFIGURATION
# -----------------------------------------------------------------------------
LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
DATA_FILE = r"C:\Users\sasch\teststory\story_data.json"
MODEL_NAME = "liquid/lfm2.5-1.2b" 
# Optional: Process only a subset for testing
TEST_CHUNKS = 0  # 0 = Process ALL items automatically
MAX_RETRIES = 3
# -----------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a specialized linguistic analyzer for Ge'ez text artifacts.
Output STRICT JSON only.

EXAMPLE OUTPUT FORMAT:
{
  "graphematic_string": "1:1 ቃለ፡​በረከት፡",
  "punctuation_markers": [":", "፡", "፡"],
  "punctuation_index": [2, 5, 10],
  "removed_artifacts": []
}"""

def fix_malformed_json(text):
    """Attempts to fix common JSON errors from small LLMs."""
    # Fix missing comma between array closing and next key
    # e.g. "index": [1, 2] "removed" -> "index": [1, 2], "removed"
    text = re.sub(r'\]\s*"', '], "', text)
    # Fix missing comma between value and next key
    # e.g. "string": "text" "punctuation" -> "string": "text", "punctuation"
    text = re.sub(r'"\s*\n\s*"', '",\n"', text)
    return text

def build_user_prompt(text):
    return (
        "Instruction: Perform a scientific Graphematic Analysis (Level A) of the Ge'ez text.\n\n"
        "SCIENTIFIC RULES (STRICT):\n"
        "1) GRAPHEMATIC STRING\n"
        "- Copy input EXACTLY.\n"
        "- CRITICAL: Treat \"Zero-Width Space\" (U+200B) as a valid character part of the string. "
        "DO NOT treat it as an artifact.\n"
        "- CRITICAL: Do NOT include any trailing newline characters from the input block. "
        "Stop exactly at the final punctuation \"።\".\n\n"
        "2) PUNCTUATION\n"
        "- Markers: \":\" \"፡\" \"።\"\n"
        "- List ALL occurrences (left-to-right) one after each other.\n"
        "- Indices: 0-based offsets including hidden characters (U+200B).\n"
        "- IMPORTANT: Use EXACTLY these keys: \"graphematic_string\", \"punctuation_markers\", \"punctuation_index\".\n\n"
        "3) ARTIFACTS\n"
        "- Since U+200B is treated as text here, 'removed_artifacts' MUST be empty [] unless there are other "
        "strictly visual glitches (e.g. damaged paper marks).\n"
        "- For this specific text: expected 'removed_artifacts': [].\n\n"
        "- If no artifacts are present, removed_artifacts MUST be [].\n\n"
        "Input Text:\n"
        f"{text}\n\n"
        "Output strictly valid JSON ONLY. No markdown, no explanations."
    )

async def analyze_verse(session, verse_obj):
    # If already analyzed, skip (optional: comment out to re-process)
    if verse_obj.get("analysis_graphematic") is not None:
        return verse_obj

    prompt = build_user_prompt(verse_obj["text"])
    
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,  # Low temp for deterministic analysis
        "stream": False
        # "response_format": {"type": "json_object"} # Removed as it causes 400 errors on some setups
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(LM_STUDIO_URL, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result['choices'][0]['message']['content']
                    
                    # Cleanup: sometimes models wrap in ```json ... ```
                    cleaned_content = content.replace("```json", "").replace("```", "").strip()
                    
                    # Attempt parsing
                    try:
                        verse_obj["analysis_graphematic"] = json.loads(cleaned_content)
                        print(f"✅ Analyzed {verse_obj['verse_id']}")
                        return verse_obj # Success
                    except json.JSONDecodeError:
                        # Try to fix it
                        fixed_content = fix_malformed_json(cleaned_content)
                        try:
                            verse_obj["analysis_graphematic"] = json.loads(fixed_content)
                            print(f"✅ Analyzed {verse_obj['verse_id']} (with repair)")
                            return verse_obj # Success after repair
                        except json.JSONDecodeError:
                            print(f"⚠️ JSON Parse Error (Attempt {attempt+1}/{MAX_RETRIES}) for {verse_obj['verse_id']}")
                            if attempt == MAX_RETRIES - 1:
                                verse_obj["analysis_graphematic"] = {"error": "Invalid JSON", "raw": content}
                else:
                    print(f"❌ HTTP Error {response.status} for {verse_obj['verse_id']}")
        except Exception as e:
            print(f"❌ Exception for {verse_obj['verse_id']}: {str(e)}")
        
        # Small backoff before retry (optional)
        await asyncio.sleep(0.5)

    return verse_obj

async def save_progress(data, filepath):
    # Atomic write to prevent corruption
    temp_path = filepath + ".tmp"
    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, filepath)
    print("💾 Progress saved.")

async def main():
    if not os.path.exists(DATA_FILE):
        print(f"File not found: {DATA_FILE}")
        return

    print("📂 Loading data...")
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Filter for testing?
    # For now, let's grab just the first few unanalyzed ones
    to_process = [v for v in data if v.get("analysis_graphematic") is None]
    
    if TEST_CHUNKS > 0:
        to_process = to_process[:TEST_CHUNKS]
        print(f"🧪 TEST MODE: Processing first {TEST_CHUNKS} unanalyzed items only.")
    else:
        print(f"🚀 FULL MODE: Processing {len(to_process)} items.")

    async with aiohttp.ClientSession() as session:
        # Use a semaphore to limit concurrency if needed (LM Studio is usually single-threaded)
        # Using 5 for parallel processing as requested
        sem = asyncio.Semaphore(5) 
        
        async def bound_analyze(verse):
            async with sem:
                return await analyze_verse(session, verse)

        tasks = [bound_analyze(verse) for verse in to_process]
        
        # Run tasks
        # We process in chunks to save periodically
        chunk_size = 20 # Increased chunk size for better parallelism flow
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            await asyncio.gather(*chunk)
            await save_progress(data, DATA_FILE)

    print("🏁 Done!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
