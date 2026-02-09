import json
import hashlib
import re
import asyncio
import aiohttp
import os
import sys
import random
import subprocess
import time
import shutil
import datetime
import prompts  # Importing the prompt definitions we just created
import urllib.parse
try:
    from fidel_ops import lookup_lex, ROOT_DB
except ImportError:
    from .fidel_ops import lookup_lex, ROOT_DB

# CONFIG LOADING
# -----------------------------------------------------------------------------
# Config is now at ../config/config.json relative to this script
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
ALIASES_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "aliases.json")
ALIASES_FILE_DE = os.path.join(os.path.dirname(__file__), "..", "config", "aliases_de.json")
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

def _load_env_file():
    if not os.path.exists(ENV_FILE):
        return
    try:
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("\"'")  # remove quotes if present
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        pass

_load_env_file()

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

config = load_config()

# CONFIGURATION
# ----------------------------------------------------------------------------- 
# LM_STUDIO_URL = config["api"]["lm_studio_url"]
LM_STUDIO_URL = "http://localhost:1234/api/v1/chat" # STATEFUL API
LMS_LOAD_URL = "http://localhost:1234/api/v0/model/load" # Legacy/Unused
DATA_FILE = os.path.join(os.path.dirname(__file__), config["files"]["data_file"])
MODELS = config["models"]
REGISTRY_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "stories", "template", "subjects", "registry.json")
REGISTRY_CACHE = None
REGISTRY_PUBLIC_FILE = None
DRY_RUN = False
DRY_RUN_LIMIT = None
DRY_RUN_OUT = None
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "logs"))
ERROR_LOG_PATH = os.path.join(LOG_DIR, "error_log.txt")
DRY_RUN_LOCK = None
FORCE_STAGE = False
LMSTUDIO_API_TOKEN = os.getenv("LMSTUDIO_API_TOKEN") or os.getenv("LM_API_TOKEN")
API_HEADERS = {"Authorization": f"Bearer {LMSTUDIO_API_TOKEN}"} if LMSTUDIO_API_TOKEN else None

def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")

def _log(msg: str):
    print(f"[{_ts()}] {msg}")

def _ensure_log_dir():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass

MAX_RETRIES = config["api"]["max_retries"]
REQUEST_TIMEOUT = config["api"]["request_timeout"]
RELOAD_COOLDOWN = config["api"]["reload_cooldown"]

# ADAPTIVE TOKEN SETTINGS
ADAPTIVE_NUM_PREDICT_BASE = config["processing"]["adaptive_token"]["base"]
ADAPTIVE_NUM_PREDICT_CHAR_BASE = config["processing"]["adaptive_token"]["char_base"]
ADAPTIVE_NUM_PREDICT_MAX = config["processing"]["adaptive_token"]["max"]
CONTEXT_WINDOW_ASSUMPTION = config["processing"]["adaptive_token"]["context_window_assumption"]
ADAPTIVE_OUTPUT_MULTIPLIER = config["processing"]["adaptive_token"].get("multiplier", 1.0)
ADAPTIVE_MIN_OUTPUT = config["processing"]["adaptive_token"].get("min_output", ADAPTIVE_NUM_PREDICT_BASE)
ADAPTIVE_CHAR_PER_TOKEN = config["processing"]["adaptive_token"].get("char_per_token", 3.5)
STAGE_MAX_OUTPUT = config["processing"].get("stage_max_output_tokens", {})
STREAM_LM = bool(config.get("api", {}).get("stream", False))
PROMPT_COMPACT_MODE = config["processing"].get("prompt_compact_mode", "auto")
PROMPT_COMPACT_THRESHOLD = config["processing"].get("prompt_compact_threshold", 12000)
MAX_ITEMS = config["processing"].get("max_items", 0)
GRAPHEMATIC_MODE = config["processing"].get("graphematic_mode", "local")
GRAPHEMATIC_PUNCTUATIONS = config["processing"].get("graphematic_punctuations", [])
MORPHOLOGIC_MODE = config["processing"].get("morphologic_mode", "json")
SYNTACTIC_MODE = config["processing"].get("syntactic_mode", "llm")
TRANSLATION_MODE = "text" # Default to Draft/Text mode

# Websearch settings (LLM + tools)
WEBSEARCH_CONFIG = config.get("websearch", {})
WEBSEARCH_MODE = WEBSEARCH_CONFIG.get("mode", "llm")
WEBSEARCH_USE_TOOLS = WEBSEARCH_CONFIG.get("use_tools", False)
WEBSEARCH_TOOL_NAME = WEBSEARCH_CONFIG.get("tool_name", "fetch")
WEBSEARCH_MCP_MODE = WEBSEARCH_CONFIG.get("mcp_mode", "plugin")
WEBSEARCH_MCP_SERVER_ID = WEBSEARCH_CONFIG.get("mcp_server_id")
WEBSEARCH_MCP_SERVER_URL = WEBSEARCH_CONFIG.get("mcp_server_url")
WEBSEARCH_MCP_ALLOWED_TOOLS = WEBSEARCH_CONFIG.get("mcp_allowed_tools")
WEBSEARCH_SEARCH_URL_TEMPLATE = WEBSEARCH_CONFIG.get("search_url_template", "https://duckduckgo.com/html/?q={query}")
WEBSEARCH_SEED_SEARCH_URL = bool(WEBSEARCH_CONFIG.get("seed_search_url", True))
WEBSEARCH_USE_SEED_URLS = bool(WEBSEARCH_CONFIG.get("use_seed_urls", False))
WEBSEARCH_SEARCH_TOOL = WEBSEARCH_CONFIG.get("search_tool_name", WEBSEARCH_TOOL_NAME)
WEBSEARCH_FETCH_TOOL = WEBSEARCH_CONFIG.get("fetch_tool_name", "fetch_content")
WEBSEARCH_CONTEXT_PREFIX = WEBSEARCH_CONFIG.get("context_prefix", "")
WEBSEARCH_MAX_ENTITY_JOBS = int(WEBSEARCH_CONFIG.get("max_entity_jobs", 3) or 3)
WEBSEARCH_MAX_VERSE_JOBS = int(WEBSEARCH_CONFIG.get("max_verse_jobs", 1) or 1)
WEBSEARCH_MAX_TOTAL_JOBS = int(WEBSEARCH_CONFIG.get("max_total_jobs", 6) or 6)
WEBSEARCH_MAX_SOURCES = int(WEBSEARCH_CONFIG.get("max_sources_per_job", 2) or 2)
WEBSEARCH_MAX_FETCH_CHARS = int(WEBSEARCH_CONFIG.get("max_fetch_chars", 4000) or 4000)
WEBSEARCH_SUMMARY_MAX_CHARS = int(WEBSEARCH_CONFIG.get("summary_max_chars", 1600) or 1600)
WEBSEARCH_SUMMARY_LLM = bool(WEBSEARCH_CONFIG.get("summary_llm", True))
WEBSEARCH_IMPORT_PATH = WEBSEARCH_CONFIG.get("import_path") or WEBSEARCH_CONFIG.get("import_results") or ""
WEBSEARCH_CACHE_DIR = WEBSEARCH_CONFIG.get("cache_dir", "")
WEBSEARCH_CACHE_DIR_DE = WEBSEARCH_CONFIG.get("cache_dir_de", "")
WEBSEARCH_CACHE_TTL_HOURS = float(WEBSEARCH_CONFIG.get("cache_ttl_hours", 0) or 0)
WEBSEARCH_SOURCES = WEBSEARCH_CONFIG.get("sources", []) or []
WEBSEARCH_SOURCES_DE = WEBSEARCH_CONFIG.get("sources_de", []) or []
WEBSEARCH_SEED_WIKIPEDIA = bool(WEBSEARCH_CONFIG.get("seed_wikipedia", True))
WEBSEARCH_USER_AGENT = WEBSEARCH_CONFIG.get("user_agent") or "teststory/1.0 (websearch; +https://example.local)"
WEBSEARCH_CONTEXT_WINDOW = int(WEBSEARCH_CONFIG.get("context_window", 1) or 0)
WEBSEARCH_CONTEXT_EXCERPT_CHARS = int(WEBSEARCH_CONFIG.get("context_excerpt_chars", 0) or 0)
WEBSEARCH_CONTEXT_SCOPE = WEBSEARCH_CONFIG.get("context_scope", ["scene"])
WEBSEARCH_COMPACT_CONTEXT = bool(WEBSEARCH_CONFIG.get("compact_context", True))
WEBSEARCH_INCLUDE_REGISTRY_CONTEXT = bool(WEBSEARCH_CONFIG.get("include_registry_context", False))
WEBSEARCH_ACTOR_PROP_TEMPLATES = WEBSEARCH_CONFIG.get("actor_prop_templates") or [
    "{label} props {props}",
    "\"{label}\" artifacts"
]
WEBSEARCH_RERANK = WEBSEARCH_CONFIG.get("rerank") or {}
WEBSEARCH_RERANK_ENABLED = bool(WEBSEARCH_RERANK.get("enabled", False))
WEBSEARCH_RERANK_MAX = int(WEBSEARCH_RERANK.get("max_candidates", 6) or 6)
WEBSEARCH_PARALLEL_LINKS_FILE = WEBSEARCH_CONFIG.get("parallel_links_file")
WEBSEARCH_PARALLEL_MAX_SUBJECTS = int(WEBSEARCH_CONFIG.get("parallel_links_max_subjects", 8) or 8)
WEBSEARCH_ENTITY_TEMPLATES = WEBSEARCH_CONFIG.get("entity_query_templates") or [
    "{label} Book of Enoch",
    "\"{label}\" \"Book of Giants\"",
    "\"{label}\" Qumran fragments"
]
WEBSEARCH_ENTITY_TEMPLATES_BY_TYPE = WEBSEARCH_CONFIG.get("entity_query_templates_by_type") or {}
WEBSEARCH_SCENE_TEMPLATES = WEBSEARCH_CONFIG.get("scene_query_templates") or []
WEBSEARCH_VERSE_TEMPLATES = WEBSEARCH_CONFIG.get("verse_query_templates") or [
    "\"Enoch {verse_id}\" commentary"
]
WEBSEARCH_MIN_TERM_HITS = int(WEBSEARCH_CONFIG.get("min_term_hits", 1) or 1)
WEBSEARCH_REQUIRE_TITLE_HIT = bool(WEBSEARCH_CONFIG.get("require_title_hit", False))

WEBSEARCH_STOPWORDS = set([
    # EN
    "the", "and", "or", "of", "to", "in", "on", "for", "with", "from", "by", "as", "at",
    "is", "are", "was", "were", "be", "being", "been", "this", "that", "these", "those",
    "a", "an", "book", "chapter", "verse",
    # DE
    "der", "die", "das", "und", "oder", "von", "zu", "im", "in", "am", "an", "auf",
    "mit", "aus", "für", "fuer", "ist", "sind", "war", "waren", "sein", "seine",
    "dies", "diese", "dieser", "dieses", "buch", "kapitel", "vers"
])

VERSE_CONTEXT_MAP = {}
ROOT_LABEL_BY_KEY = {}
PARALLEL_LINKS_MAP = {}

def _normalize_root_key(root: str) -> str:
    if not root:
        return ""
    r = root.upper()
    r = r.replace("Ṣ", "S")
    r = r.replace("ʾ", "A").replace("ʼ", "A").replace("'", "A")
    r = r.replace("ʿ", "C")
    for ch in ["-", " ", "·", "_"]:
        r = r.replace(ch, "")
    return r

def _build_root_label_map() -> dict:
    out = {}
    for _k, entry in (ROOT_DB or {}).items():
        root = entry.get("root")
        if not root:
            continue
        label = _extract_label_from_lex(entry)
        if not label:
            label = root
        out[_normalize_root_key(root)] = label
    return out

# Entities outputs
BUILD_REGISTRY = False
BUILD_OCCURRENCES = False
BUILD_ASSET_BIBLE = False
SUBJECTS_DIR = None
STORY_ID = None
TIMELINE_ID = None
PHASE_COUNT = None
PHASE_LABELS = []

SUBJECTS_CONFIG = config.get("subjects", {})
if STORY_ID is None:
    STORY_ID = SUBJECTS_CONFIG.get("story_id")
if TIMELINE_ID is None:
    TIMELINE_ID = SUBJECTS_CONFIG.get("timeline_id", "default")
if PHASE_COUNT is None:
    try:
        PHASE_COUNT = int(SUBJECTS_CONFIG.get("dynamic_phase_max", 3))
    except (TypeError, ValueError):
        PHASE_COUNT = 3
PHASE_LABELS = SUBJECTS_CONFIG.get("dynamic_phase_labels", []) or []

# Per-model concurrency limit
MAX_CONCURRENT_PER_MODEL = config["api"]["max_concurrent_per_model"]
model_semaphores = {} 
last_reload_time = {} 

# Choose which stage to run: 'graphematic', 'morphologic', 'syntactic', 'semantic', 'entities', 'websearch'
CURRENT_STAGE = 'graphematic'  
# -----------------------------------------------------------------------------

async def touch_heartbeat():
    """Updates a heartbeat file so external managers know we are alive."""
    try:
        with open("heartbeat.lock", "w") as f:
            f.write(str(time.time()))
    except:
        pass

async def is_model_loaded(model_id): # DEPRECATED/UNUSED inside this script, but keeping empty stub inf called
    return True

async def check_model_api_readiness(model_id): # DEPRECATED/UNUSED inside this script
    return True

async def ensure_startup_state():
    _log("ℹ️  Startup: Assuming Models are managed externally. Proceeding...")

async def relaunch_model(model_id):
    print(f"⚠️ Request failed for {model_id}. Pausing and hoping external manager fixes it...")
    await asyncio.sleep(5)

def fix_malformed_json(text):
    import re
    # Fix 1: Excessive closing braces in nested objects. 
    # Example: "evidence": {...}}}, "description" -> "evidence": {...}}, "description"
    # This is tricky with regex. Let's try flexible parsing first.
    
    # Fix 2: Missing comma before new keys
    # "database"}}}, "description" -> "database"}}}, ,"description" (roughly)
    text = re.sub(r'\}\s*"', '}, "', text)
    text = re.sub(r'\]\s*"', '], "', text)
    text = re.sub(r'"\s*\n\s*"', '",\n"', text)
    
    # Fix 3: Remove trailing commas before closing braces/brackets
    text = re.sub(r',\s*\}', '}', text)
    text = re.sub(r',\s*\]', ']', text)
    
    # Fix 4: Double closing braces from the error log
    # The error log shows: ...evidence": {...}}},
    text = text.replace("}}}", "}}") 

    # Attempt to close open braces if truncated
    if text.count('{') > text.count('}'):
        text += '}' * (text.count('{') - text.count('}'))
    if text.count('[') > text.count(']'):
        text += ']' * (text.count('[') - text.count(']'))
    return text

def _extract_json_block(text: str) -> str | None:
    """Try to extract a JSON object from arbitrary text."""
    if not text:
        return None
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "\"":
                in_str = False
            continue
        else:
            if ch == "\"":
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
    return None

def parse_custom_graphematic_format(text):
    data = {
        "graphematic_string": "",
        # Optionals for now
        "punctuation_markers": [],
        "punctuation_index": [],
        "removed_artifacts": [],
        "uncertainties": []
    }
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Support both old STRING: and new GRAPH_STR: just in case, but prefer STRING: now
        if line.startswith("STRING:"):
            data["graphematic_string"] = line.replace("STRING:", "").strip()
        elif line.startswith("GRAPH_STR:"):
            data["graphematic_string"] = line.replace("GRAPH_STR:", "").strip()
                 
    return data

def parse_custom_translation_format(text):
    data = {"literal": "", "fluent": ""}
    lines = text.split('\n')
    current_key = None
    for line in lines:
        line = line.strip()
        if not line: continue
        if line.startswith("LITERAL:"):
            data["literal"] = line.replace("LITERAL:", "").strip()
            current_key = "literal"
        elif line.startswith("FLUENT:"):
            data["fluent"] = line.replace("FLUENT:", "").strip()
            current_key = "fluent"
        elif current_key:
             data[current_key] += " " + line
    return data

def parse_custom_morph_format(text):
    tokens = []
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        if line.startswith("TOKEN:"):
            # TOKEN: t1 | FORM: ... | TRANS: ... | TYPE: ...
            obj = {"id": "", "form": "", "transliteration": "", "type": ""}
            parts = line.split("|")
            for part in parts:
                if "TOKEN:" in part: obj["id"] = part.replace("TOKEN:", "").strip()
                elif "FORM:" in part: obj["form"] = part.replace("FORM:", "").strip()
                elif "TRANS:" in part: obj["transliteration"] = part.replace("TRANS:", "").strip()
                elif "TYPE:" in part: obj["type"] = part.replace("TYPE:", "").strip()
            tokens.append(obj)
    return {"tokens": tokens}

def parse_custom_syntax_format(text):
    """Parses the line-based syntax format."""
    parses = []
    current_parse = None
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # PARSE: S1 ...
        if line.upper().startswith("PARSE:"):
            # Try to grab ID and fields from the same line if delimited by pipe
            parts = line.split("|")
            # First part is PARSE: <id>
            first_part = parts[0]
            pid = first_part.split(":", 1)[1].strip()
            
            current_parse = {
                "id": pid, 
                "structure_type": "?", 
                "bracket_notation": "?", 
                "dependencies": [], 
                "notes": ""
            }
            
            # Process remaining parts on the first line
            for part in parts[1:]:
                if ":" in part:
                    k, v = part.split(":", 1)
                    k = k.strip().upper()
                    v = v.strip()
                    if k == "TYPE": current_parse["structure_type"] = v
                    elif k == "BRACKET": current_parse["bracket_notation"] = v
            
            parses.append(current_parse)
            
        elif current_parse is not None:
            # Handle additional lines like DEP: or NOTE:
            if ":" in line:
                key, val = line.split(":", 1)
                key = key.strip().upper()
                val = val.strip()
                
                if key == "TYPE": current_parse["structure_type"] = val
                elif key == "BRACKET": current_parse["bracket_notation"] = val
                elif key == "DEP": current_parse["dependencies"].append(val)
                elif key == "NOTE": current_parse["notes"] = val
                
    return {"syntax": {"parses": parses}}

def parse_custom_semantic_format(text):
    """Parses the line-based semantic format."""
    evaluations = []
    current_eval = None
    final_decision = {}
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # EVALUATION: S1 | PLAUSIBILITY: high | ...
        if line.upper().startswith("EVALUATION:"):
            parts = line.split("|")
            # 0: EVALUATION: S1
            ref = parts[0].split(":", 1)[1].strip()
            
            current_eval = {
                "hypothesis_ref": ref,
                "plausibility": "?",
                "context_invariance": "?",
                "back_translation": "?",
                "reasoning": "",
                "parallels": []
            }
            
            for part in parts[1:]:
                if ":" in part:
                    k, v = part.split(":", 1)
                    k = k.strip().upper().replace(" ", "_") # CTX_INVARIANCE
                    v = v.strip()
                    if k == "PLAUSIBILITY": current_eval["plausibility"] = v
                    elif k == "CTX_INVARIANCE": current_eval["context_invariance"] = v
                    elif k == "BACK_TRANS": current_eval["back_translation"] = v
            
            evaluations.append(current_eval)
        
        elif line.upper().startswith("DECISION:"):
            # DECISION: HYP_REF: <id> | TRANS_ID: <id> | CONFIDENCE: <level>
            # Remove "DECISION:" prefix first
            content = line.split(":", 1)[1].strip()
            parts = content.split("|")
            for part in parts:
                if ":" in part:
                    k, v = part.split(":", 1)
                    k = k.strip().upper()
                    v = v.strip()
                    if k == "HYP_REF": final_decision["hypothesis_ref"] = v
                    elif k == "TRANS_ID": final_decision["translation_id"] = v
                    elif k == "CONFIDENCE": final_decision["confidence"] = v

        elif current_eval is not None:
            if line.upper().startswith("REASONING:"):
                current_eval["reasoning"] = line.split(":", 1)[1].strip()
            elif line.upper().startswith("PARALLEL:"):
                # PARALLEL: <ref> | NOTE: <text>
                content = line.split(":", 1)[1].strip()
                p_parts = content.split("|")
                p_ref = p_parts[0].strip()
                p_note = ""
                if len(p_parts) > 1 and "NOTE:" in p_parts[1]:
                    p_note = p_parts[1].split(":", 1)[1].strip()
                current_eval["parallels"].append({"ref": p_ref, "note": p_note})
                
    return {"evaluation": evaluations, "final_decision": final_decision}

def parse_custom_entities_format(text):
    """Parses the line-based entities/scene format."""
    actors = []
    props = []
    places = []
    scenes = []
    blocking = {"anchors": [], "paths": []}
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Generic parser for field-based lines
        parts = line.split("|")
        head = parts[0]
        if ":" not in head: continue
        
        type_key, primary_val = head.split(":", 1)
        type_key = type_key.strip().upper()
        primary_val = primary_val.strip()
        
        # Helper to extract other fields
        fields = {}
        for part in parts[1:]:
            if ":" in part:
                k, v = part.split(":", 1)
                fields[k.strip().upper()] = v.strip()
        
        if type_key == "ACTOR":
            actors.append({
                "name": primary_val,
                "visualTraits": fields.get("TRAIT", ""),
                "role": fields.get("ROLE", ""),
                "changes": fields.get("CHANGES", "")
            })
        elif type_key == "PROP":
            props.append({
                "name": primary_val,
                "visualTraits": fields.get("TRAIT", ""),
                "role": fields.get("ROLE", ""),
                "changes": fields.get("CHANGES", "")
            })
        elif type_key == "PLACE":
            places.append({
                "name": primary_val,
                "visualTraits": fields.get("TRAIT", ""),
                "role": fields.get("ROLE", ""),
                "changes": fields.get("CHANGES", "")
            })
        elif type_key == "SCENE":
            scenes.append({
                "title": primary_val,
                "location": fields.get("LOC", ""),
                "action": fields.get("ACTION", ""),
                "actorsInvolved": fields.get("CAST", "")
            })
        elif type_key == "BLOCKING_ANCHOR":
            blocking["anchors"].append({
                "id": primary_val,
                "description": fields.get("DESC", "")
            })
        elif type_key == "BLOCKING_PATH":
            blocking["paths"].append({
                "actor": primary_val,
                "start_anchor": fields.get("START", ""),
                "end_anchor": fields.get("END", ""),
                "motion": fields.get("MOTION", ""),
                "duration_sec": fields.get("DUR", "")
            })
            
    return {"actors": actors, "props": props, "places": places, "scenes": scenes, "blocking": blocking}

def parse_custom_asset_card_format(text):
    cards = []
    current_card = {}
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if line.startswith("ASSET_CARD:"):
            if current_card:
                cards.append(current_card)
            current_card = {"name": "", "type": "", "description": "", "visual_details": "", "phases": []}
            
            parts = line.split("|")
            for part in parts:
                if "ASSET_CARD:" in part:
                    current_card["name"] = part.replace("ASSET_CARD:", "").strip()
                elif "TYPE:" in part:
                    current_card["type"] = part.replace("TYPE:", "").strip()
                    
        elif line.startswith("DESC:"):
            current_card["description"] = line.replace("DESC:", "").strip()
        elif line.startswith("VISUAL:"):
            current_card["visual_details"] = line.replace("VISUAL:", "").strip()
        elif line.startswith("PHASE:"):
             current_card["phases"].append(line.replace("PHASE:", "").strip())

    if current_card:
        cards.append(current_card)
        
    return {"cards": cards}

def _extract_tag_value(text: str, key: str) -> str | None:
    if not text:
        return None
    pattern = re.compile(rf"\b{re.escape(key)}\s*[:=]\s*([A-Za-z0-9_\\-]+)", re.IGNORECASE)
    m = pattern.search(text)
    if not m:
        return None
    return m.group(1)

def parse_morph_text_response(text: str, words: list) -> dict:
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    parsed = {}
    ordered_no_id = []
    for line in lines:
        m = re.match(r"^(?:t)?(\d+)\s*[:\\-\\.\\)]\\s*(.*)$", line, re.IGNORECASE)
        if m:
            idx = int(m.group(1))
            rest = m.group(2)
        else:
            idx = None
            rest = line
        pos = _extract_tag_value(rest, "POS")
        role = _extract_tag_value(rest, "ROLE")
        root = _extract_tag_value(rest, "ROOT")
        if idx is not None:
            parsed[idx] = {"pos": pos, "role": role, "root": root, "notes": rest}
        else:
            ordered_no_id.append({"pos": pos, "role": role, "root": root, "notes": rest})

    token_entries = []
    # Map by explicit word_id first; fallback to sequential order
    for i, w in enumerate(words, start=1):
        word_id = w.get("word_id", i)
        rec = parsed.get(word_id)
        if rec is None and i in parsed:
            rec = parsed.get(i)
        if rec is None and ordered_no_id:
            rec = ordered_no_id[i - 1] if i - 1 < len(ordered_no_id) else None
        pp = w.get("pre_processing", {})
        ont = pp.get("ontology", {})
        token_entries.append({
            "token_id": f"t{word_id}",
            "surface": w.get("text"),
            "analysis": {
                "root": (rec or {}).get("root") or ont.get("root_key") or pp.get("root") or "?",
                "pos": (rec or {}).get("pos") or pp.get("pos_hint") or "UNKNOWN",
                "role": (rec or {}).get("role") or pp.get("syntax_role") or "UNKNOWN",
                "notes": (rec or {}).get("notes") or ""
            }
        })

    return {
        "schema": "morph.text.v1",
        "tokens": token_entries
    }

def _build_syntax_heuristic(verse_obj: dict) -> dict:
    words = verse_obj.get("words", []) or []
    has_verb = any((w.get("pre_processing") or {}).get("pos_hint") == "VERB" for w in words)
    structure_type = "Verbal Chain" if has_verb else "Nominal Chain"
    bracket = "[VP " if has_verb else "[NP "
    token_ids = [f"t{w.get('word_id')}" for w in words]
    bracket += " ".join(f"[{t}]" for t in token_ids) + "]"
    deps = []
    for i in range(len(words) - 1):
        role = (words[i + 1].get("pre_processing") or {}).get("syntax_role") or "link"
        deps.append(f"t{i+1}->t{i+2} ({role})")
    return {
        "syntax": {
            "parses": [
                {
                    "id": "H1",
                    "structure_type": structure_type,
                    "bracket_notation": bracket,
                    "dependencies": deps,
                    "notes": "heuristic"
                }
            ]
        }
    }

def _adaptive_num_predict(text_content: str) -> int:
    """Calculates how many tokens to request based on input length."""
    length = len(text_content or "")
    base = ADAPTIVE_NUM_PREDICT_BASE
    
    # Scale up if text is longer than base reference
    scale = max(1.0, length / ADAPTIVE_NUM_PREDICT_CHAR_BASE)
    adaptive = int(base * scale)
    
    # Clamp to configured range
    if adaptive < base:
        adaptive = base
    if ADAPTIVE_NUM_PREDICT_MAX:
        adaptive = min(adaptive, ADAPTIVE_NUM_PREDICT_MAX)
        
    return adaptive

def _compute_dynamic_max_tokens(prompt_str: str) -> int:
    """Estimate safe max tokens given the prompt length."""
    target_max_tokens = _adaptive_num_predict(prompt_str)
    if ADAPTIVE_OUTPUT_MULTIPLIER and ADAPTIVE_OUTPUT_MULTIPLIER != 1.0:
        target_max_tokens = int(target_max_tokens * float(ADAPTIVE_OUTPUT_MULTIPLIER))
    if ADAPTIVE_MIN_OUTPUT:
        target_max_tokens = max(target_max_tokens, int(ADAPTIVE_MIN_OUTPUT))
    if ADAPTIVE_NUM_PREDICT_MAX:
        target_max_tokens = min(target_max_tokens, ADAPTIVE_NUM_PREDICT_MAX)
    est_div = ADAPTIVE_CHAR_PER_TOKEN if ADAPTIVE_CHAR_PER_TOKEN else 3.5
    estimated_input_tokens = int(len(prompt_str) / est_div)
    projected_total = estimated_input_tokens + target_max_tokens
    if CONTEXT_WINDOW_ASSUMPTION and projected_total > (CONTEXT_WINDOW_ASSUMPTION - 100):
        available = CONTEXT_WINDOW_ASSUMPTION - estimated_input_tokens - 100
        if available > 0:
            target_max_tokens = min(target_max_tokens, available)
        else:
            # Input likely exceeds context; keep a minimal output budget
            target_max_tokens = max(200, min(target_max_tokens, 512))
    return int(target_max_tokens)

def _estimate_input_tokens(prompt_str: str) -> int:
    est_div = ADAPTIVE_CHAR_PER_TOKEN if ADAPTIVE_CHAR_PER_TOKEN else 3.5
    return int(len(prompt_str) / est_div)

def _get_state_entry(state_ids: dict, stage: str):
    """Returns (state_id, model) for a stage with backward compatibility."""
    val = state_ids.get(stage)
    if isinstance(val, dict):
        state_id = val.get("id") or val.get("response_id")
        return state_id, val.get("model")
    if isinstance(val, str):
        return val, None
    return None, None

def _state_model_allowed(state_model) -> bool:
    if not state_model:
        return True
    return state_model in MODELS

def _is_stage_pending(verse_obj, stage: str) -> bool:
    """Determine if a stage should be processed."""
    if FORCE_STAGE:
        return True
    # Special handling for LLM Review Modes
    if stage == "graphematic" and GRAPHEMATIC_MODE == "llm":
        return verse_obj.get("analysis_graphematic_review") is None
    if stage == "morphologic" and MORPHOLOGIC_MODE == "llm":
        return verse_obj.get("analysis_morphologic_review") is None
    if stage == "syntactic" and SYNTACTIC_MODE == "llm":
        return verse_obj.get("analysis_syntactic_review") is None
    if stage == "translation" and TRANSLATION_MODE != "json":
        return verse_obj.get("analysis_translation_draft") is None

    key = f"analysis_{stage}"
    val = verse_obj.get(key)
    if val is None:
        return True
    if stage == "websearch":
        if not isinstance(val, dict):
            return True
        status = val.get("status")
        return status not in ("complete", "done")
    if stage == "graphematic" and isinstance(val, dict):
        return val.get("status") != "complete"
    return False

def _build_graphematic_local(text: str):
    graphematic_string = text or ""
    if GRAPHEMATIC_PUNCTUATIONS:
        punct_set = set(GRAPHEMATIC_PUNCTUATIONS)
    else:
        punct_set = {"፡", "።", "፣", "፤", "፥", "፦", "፧", "፨", ".", ",", "!", "?", ";", ":"}
    punctuation_markers = []
    punctuation_index = []
    for idx, ch in enumerate(graphematic_string):
        if ch in punct_set:
            punctuation_markers.append(ch)
            punctuation_index.append(idx)
    return {
        "graphematic_string": graphematic_string,
        "punctuation_markers": punctuation_markers,
        "punctuation_index": punctuation_index,
        "removed_artifacts": [],
        "uncertainties": [],
        "status": "complete"
    }

def _build_graphematic_local_v2(text: str, words: list | None = None):
    graphematic_string = text or ""
    if GRAPHEMATIC_PUNCTUATIONS:
        punct_set = set(GRAPHEMATIC_PUNCTUATIONS)
    else:
        punct_set = {"\u1361", "\u1362", "\u1363", "\u1364", "\u1365", "\u1366", "\u1367", "\u1368", ".", ",", "!", "?", ";", ":"}
    punctuation_markers = []
    punctuation_index0 = []
    punctuation_index = []
    for idx, ch in enumerate(graphematic_string):
        if ch in punct_set:
            punctuation_markers.append(ch)
            punctuation_index0.append(idx)
            punctuation_index.append(idx + 1)

    word_ranges = []
    for w in words or []:
        ids = w.get("char_ids") or []
        if not ids:
            continue
        word_ranges.append((w.get("word_id"), min(ids), max(ids)))

    punctuation_links = []
    for marker, idx0, char_id in zip(punctuation_markers, punctuation_index0, punctuation_index):
        prev_word_id = None
        next_word_id = None
        inside_word_id = None

        for wid, start_id, end_id in word_ranges:
            if start_id <= char_id <= end_id:
                inside_word_id = wid
                break
            if end_id < char_id:
                prev_word_id = wid
                continue
            if start_id > char_id and next_word_id is None:
                next_word_id = wid
                break

        if inside_word_id is not None:
            position = "inside"
        elif prev_word_id is not None and next_word_id is not None:
            position = "between"
        elif prev_word_id is None and next_word_id is not None:
            position = "before_first"
        elif prev_word_id is not None and next_word_id is None:
            position = "after_last"
        else:
            position = "isolated"

        link = {
            "marker": marker,
            "char_id": char_id,
            "index0": idx0,
            "position": position
        }
        if prev_word_id is not None:
            link["prev_word_id"] = prev_word_id
            link["word_id_left"] = prev_word_id
        if next_word_id is not None:
            link["next_word_id"] = next_word_id
            link["word_id_right"] = next_word_id
        if inside_word_id is not None:
            link["word_id"] = inside_word_id
        punctuation_links.append(link)

    return {
        "graphematic_string": graphematic_string,
        "punctuation_markers": punctuation_markers,
        "punctuation_index": punctuation_index,
        "punctuation_links": punctuation_links,
        "removed_artifacts": [],
        "uncertainties": [],
        "status": "complete"
    }

def _build_morphologic_local(words: list) -> dict:
    tokens = []
    for w in words or []:
        pp = w.get("pre_processing", {})
        ont = pp.get("ontology", {})
        root = pp.get("root") or ont.get("root_key") or "?"
        pos = pp.get("pos_hint") or "UNKNOWN"
        role = pp.get("syntax_role") or "UNKNOWN"
        order = pp.get("grammatical_vowel") or 0
        tokens.append({
            "token_id": f"t{w.get('word_id')}",
            "surface": w.get("text"),
            "analysis": {
                "root": root,
                "pos": pos,
                "role": role,
                "grammatical_notes": f"Order {order}"
            }
        })
    return {
        "schema": "morph.local.v1",
        "tokens": tokens
    }

def _entity_category(asset_tag: str | None) -> str:
    if not asset_tag:
        return "unknown"
    if asset_tag.startswith(("ACTOR_", "ENTITY_", "CLASS_")):
        return "actor"
    if asset_tag.startswith(("PROP_", "RESOURCE_")):
        return "prop"
    if asset_tag.startswith("ENVIRONMENT_"):
        return "place"
    if asset_tag.startswith(("STATE_", "CONDITION_", "PERM_")):
        return "state"
    if asset_tag.startswith(("ACTION_", "EVENT_")):
        return "event"
    return "unknown"

def _asset_tag_from_asset_id(asset_id: str | None) -> str | None:
    if not asset_id:
        return None
    parts = asset_id.split("_")
    if len(parts) <= 1:
        return None
    return "_".join(parts[:-1])

def _build_entities_local(verse_obj: dict) -> dict:
    words = verse_obj.get("words", []) or []
    registry = _load_registry()
    assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}
    word_by_id = {w.get("word_id"): w for w in words if w.get("word_id") is not None}

    ent_map: dict[str, dict] = {}
    for w in words:
        pp = w.get("pre_processing", {}) or {}
        if not pp.get("is_asset"):
            continue
        asset_id = pp.get("asset_id")
        if not asset_id:
            continue

        ent = ent_map.get(asset_id)
        if ent is None:
            ontology = pp.get("ontology", {}) or {}
            asset_tag = ontology.get("asset_tag")
            entry = assets.get(asset_id, {})
            ent = {
                "asset_id": asset_id,
                "root": pp.get("root"),
                "root_key": ontology.get("root_key"),
                "concept": ontology.get("concept"),
                "asset_tag": asset_tag,
                "category": _entity_category(asset_tag),
                "current_state": entry.get("current_state"),
                "mentions_global": entry.get("mentions"),
                "word_ids": [],
                "surface_forms": [],
                "pos_hints": [],
                "syntax_roles": [],
                "spatial_mentions": []
            }
            ent_map[asset_id] = ent

        ent["word_ids"].append(w.get("word_id"))
        ent["surface_forms"].append(w.get("text"))
        ent["pos_hints"].append(pp.get("pos_hint"))
        ent["syntax_roles"].append(pp.get("syntax_role"))
        spatial = pp.get("spatial")
        if spatial:
            ent["spatial_mentions"].append(spatial)

    # Add alias-based entities (useful for non-Ge'ez text)
    for ah in verse_obj.get("alias_hits", []) or []:
        alias_id = ah.get("alias_id")
        if not alias_id:
            continue
        ent = ent_map.get(alias_id)
        if ent is None:
            alias_entry = alias_registry.get(alias_id, {}) if isinstance(alias_registry, dict) else {}
            labels = alias_entry.get("labels") or []
            alias_label = ah.get("alias_label")
            asset_tag = _asset_tag_from_asset_id(alias_id)
            entry = assets.get(alias_id, {})
            ent = {
                "asset_id": alias_id,
                "root": None,
                "root_key": None,
                "concept": alias_label or (labels[0] if labels else entry.get("concept")),
                "asset_tag": asset_tag,
                "category": _entity_category(asset_tag),
                "current_state": entry.get("current_state", "ACTIVE"),
                "mentions_global": entry.get("mentions"),
                "word_ids": [],
                "surface_forms": [],
                "pos_hints": [],
                "syntax_roles": [],
                "spatial_mentions": []
            }
            ent_map[alias_id] = ent

        for wid in ah.get("word_ids", []) or []:
            w = word_by_id.get(wid)
            if not w:
                continue
            ent["word_ids"].append(wid)
            ent["surface_forms"].append(w.get("text"))
            pp = w.get("pre_processing", {}) or {}
            ent["pos_hints"].append(pp.get("pos_hint"))
            ent["syntax_roles"].append(pp.get("syntax_role"))
            spatial = pp.get("spatial")
            if spatial:
                ent["spatial_mentions"].append(spatial)

    # De-dup and normalize arrays
    entities = []
    for ent in ent_map.values():
        ent["word_ids"] = sorted({i for i in ent["word_ids"] if i is not None})
        ent["surface_forms"] = sorted({s for s in ent["surface_forms"] if s})
        ent["pos_hints"] = sorted({p for p in ent["pos_hints"] if p})
        ent["syntax_roles"] = sorted({r for r in ent["syntax_roles"] if r})
        if ent["spatial_mentions"]:
            # unique by json serialization
            seen = set()
            unique_spatial = []
            for s in ent["spatial_mentions"]:
                key = json.dumps(s, sort_keys=True, ensure_ascii=False)
                if key in seen:
                    continue
                seen.add(key)
                unique_spatial.append(s)
            ent["spatial_mentions"] = unique_spatial
        entities.append(ent)

    entities.sort(key=lambda e: (e["word_ids"][0] if e["word_ids"] else 0, e["asset_id"]))

    return {
        "schema": "entities.local.v1",
        "entities": entities,
        "alias_hits": verse_obj.get("alias_hits") or [],
        "state_triggers": verse_obj.get("state_triggers") or [],
        "state_updates": verse_obj.get("state_updates") or []
    }

def _entities_stats(verse_obj: dict) -> dict:
    ent = verse_obj.get("analysis_entities") or {}
    return {
        "entities": len(ent.get("entities", []) or []),
        "alias_hits": len(ent.get("alias_hits", []) or []),
        "state_triggers": len(ent.get("state_triggers", []) or []),
        "state_updates": len(ent.get("state_updates", []) or []),
    }

def _load_alias_file(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}
    aliases = raw.get("aliases") if isinstance(raw, dict) else raw
    if not isinstance(aliases, list):
        return {}
    out = {}
    for a in aliases:
        aid = a.get("id")
        if not aid:
            continue
        out[aid] = {
            "id": aid,
            "labels": a.get("labels", []),
            "patterns": a.get("patterns", [])
        }
    return out

def _load_aliases_for_registry() -> dict:
    # Always load base aliases, optionally merge DE aliases for DE context.
    out = _load_alias_file(ALIASES_FILE)
    if _is_de_context() and os.path.exists(ALIASES_FILE_DE):
        de_aliases = _load_alias_file(ALIASES_FILE_DE)
        for aid, entry in de_aliases.items():
            out[aid] = entry
    return out

def _build_registry_from_data(data: list) -> dict:
    registry = {"assets": {}, "aliases": _load_aliases_for_registry()}
    assets = registry["assets"]
    for verse in data or []:
        verse_id = verse.get("verse_id")
        for w in verse.get("words", []) or []:
            pp = w.get("pre_processing", {}) or {}
            asset_id = pp.get("asset_id")
            if not asset_id:
                continue
            entry = assets.get(asset_id)
            if not entry:
                ontology = pp.get("ontology", {}) or {}
                entry = {
                    "id": asset_id,
                    "root": pp.get("root"),
                    "root_key": ontology.get("root_key"),
                    "concept": ontology.get("concept"),
                    "asset_tag": ontology.get("asset_tag"),
                    "current_state": "ACTIVE",
                    "mentions": 0,
                    "first_seen": verse_id
                }
                assets[asset_id] = entry
            entry["mentions"] += 1

        # Include alias hits as lightweight assets (useful for non-Ge'ez text)
        for ah in verse.get("alias_hits", []) or []:
            alias_id = ah.get("alias_id")
            if not alias_id:
                continue
            entry = assets.get(alias_id)
            if not entry:
                alias_entry = registry["aliases"].get(alias_id, {}) if isinstance(registry.get("aliases"), dict) else {}
                labels = alias_entry.get("labels") or []
                alias_label = ah.get("alias_label")
                entry = {
                    "id": alias_id,
                    "root": None,
                    "root_key": None,
                    "concept": alias_label or (labels[0] if labels else None),
                    "asset_tag": _asset_tag_from_asset_id(alias_id),
                    "current_state": "ACTIVE",
                    "mentions": 0,
                    "first_seen": verse_id
                }
                assets[alias_id] = entry
            entry["mentions"] += 1

        for upd in verse.get("state_updates", []) or []:
            asset_id = upd.get("asset_id")
            state = upd.get("state")
            if asset_id and state and asset_id in assets:
                assets[asset_id]["current_state"] = state

    return registry

def _data_has_assets_or_aliases(data: list) -> bool:
    for verse in data or []:
        for w in verse.get("words", []) or []:
            pp = w.get("pre_processing", {}) or {}
            if pp.get("asset_id"):
                return True
        if verse.get("alias_hits"):
            return True
    return False

def _save_registry(registry: dict):
    try:
        os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
        with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(registry, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ Registry write failed: {e}")

def _save_public_registry(registry_list: list):
    if not REGISTRY_PUBLIC_FILE:
        return
    try:
        os.makedirs(os.path.dirname(REGISTRY_PUBLIC_FILE), exist_ok=True)
        with open(REGISTRY_PUBLIC_FILE, "w", encoding="utf-8") as f:
            json.dump(registry_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ Public registry write failed: {e}")

def _subjects_dir() -> str:
    if SUBJECTS_DIR:
        return SUBJECTS_DIR
    return os.path.dirname(REGISTRY_FILE)

def _write_jsonl(path: str, rows: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def _build_occurrences_from_data(data: list, registry: dict | None = None) -> list[dict]:
    assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}
    rows = []
    for verse in data or []:
        verse_id = verse.get("verse_id")
        chapter = verse.get("chapter")
        verse_num = verse.get("verse")
        source_id = f"verse_{str(verse_id).replace(':', '_')}"
        ent = (verse.get("analysis_entities") or {}).get("entities", []) or []
        for e in ent:
            asset_id = e.get("asset_id")
            if not asset_id:
                continue
            asset_entry = assets.get(asset_id, {}) if isinstance(assets, dict) else {}
            alias_entry = alias_registry.get(asset_id, {}) if isinstance(alias_registry, dict) else {}
            alias_labels = alias_entry.get("labels") or []
            surface_forms = e.get("surface_forms") or []
            subject_name = None
            if alias_labels:
                subject_name = alias_labels[0]
            elif surface_forms:
                subject_name = surface_forms[0]
            else:
                subject_name = e.get("root") or asset_entry.get("root") or asset_entry.get("id") or asset_id
            rows.append({
                "subject_id": asset_id,
                "subject_name": subject_name,
                "asset_tag": e.get("asset_tag") or asset_entry.get("asset_tag"),
                "category": e.get("category"),
                "concept": e.get("concept") or asset_entry.get("concept"),
                "root": e.get("root") or asset_entry.get("root"),
                "root_key": e.get("root_key") or asset_entry.get("root_key"),
                "surface_forms": surface_forms,
                "syntax_roles": e.get("syntax_roles", []),
                "pos_hints": e.get("pos_hints", []),
                "source_id": source_id,
                "chapter": chapter,
                "verse": verse_num,
                "segment_label": source_id,
                "segment_type": "verse",
                "word_ids": e.get("word_ids", []),
                "source_path": ""
            })
        for ah in verse.get("alias_hits", []) or []:
            alias_id = ah.get("alias_id")
            if not alias_id:
                continue
            alias_entry = alias_registry.get(alias_id, {}) if isinstance(alias_registry, dict) else {}
            alias_labels = alias_entry.get("labels") or []
            alias_label = ah.get("alias_label") or (alias_labels[0] if alias_labels else None)
            rows.append({
                "subject_id": alias_id,
                "subject_name": alias_label or alias_id,
                "asset_tag": _asset_tag_from_asset_id(alias_id),
                "source_id": source_id,
                "chapter": chapter,
                "verse": verse_num,
                "segment_label": source_id,
                "segment_type": "alias",
                "word_ids": ah.get("word_ids", []),
                "source_path": ""
            })
    return rows

def _dynamic_assets_from_data(data: list) -> set:
    dynamic_assets = set()
    for verse in data or []:
        for upd in verse.get("state_updates", []) or []:
            asset_id = upd.get("asset_id")
            if asset_id:
                dynamic_assets.add(asset_id)
    return dynamic_assets

def _build_phase_states(chapter_start, chapter_end, phase_count: int, phase_labels: list[str]):
    if not phase_count or phase_count <= 0:
        return []
    try:
        start_int = int(chapter_start) if chapter_start is not None else None
        end_int = int(chapter_end) if chapter_end is not None else None
    except (ValueError, TypeError):
        start_int = None
        end_int = None

    if start_int is None or end_int is None:
        chapter_ranges = [(chapter_start, chapter_end) for _ in range(phase_count)]
    else:
        total_chapters = max(1, end_int - start_int + 1)
        chapter_ranges = []
        for idx in range(phase_count):
            phase_start = start_int + int(idx * total_chapters / phase_count)
            phase_end = start_int + int((idx + 1) * total_chapters / phase_count) - 1
            if idx == phase_count - 1:
                phase_end = end_int
            chapter_ranges.append((phase_start, phase_end))

    states = []
    for idx in range(phase_count):
        label = phase_labels[idx] if idx < len(phase_labels) and phase_labels[idx] else f"Phase {idx + 1}"
        chapter_start_value, chapter_end_value = chapter_ranges[idx]
        states.append({
            "state_id": f"phase_{idx + 1:02d}",
            "label": label,
            "chapter_start": chapter_start_value,
            "chapter_end": chapter_end_value,
            "segment_labels": [],
            "scene_labels": [],
            "source_ids": [],
            "notes": [],
        })
    return states

def _build_public_registry(data: list, registry: dict) -> list[dict]:
    assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}
    occurrences = _build_occurrences_from_data(data, registry)
    dynamic_assets = _dynamic_assets_from_data(data)
    occ_map: dict[str, list[dict]] = {}
    for occ in occurrences:
        sid = occ.get("subject_id")
        if not sid:
            continue
        occ_map.setdefault(sid, []).append(occ)

    public = []
    for asset_id, entry in assets.items():
        occs = occ_map.get(asset_id, [])
        chapters = [o.get("chapter") for o in occs if isinstance(o.get("chapter"), int)]
        first_ch = min(chapters) if chapters else None
        last_ch = max(chapters) if chapters else None
        asset_tag = entry.get("asset_tag")
        subject_type = _asset_bible_type(asset_tag)
        name = None
        # Prefer alias label if available
        alias_entry = alias_registry.get(asset_id)
        if alias_entry and alias_entry.get("labels"):
            name = alias_entry.get("labels")[0]
        if not name:
            name = entry.get("root") or entry.get("id") or asset_id
        public.append({
            "id": asset_id,
            "name": name,
            "type": subject_type,
            "occurrence_count": len(occs),
            "first_chapter": first_ch,
            "last_chapter": last_ch,
            "is_dynamic": asset_id in dynamic_assets,
            "timeline_id": TIMELINE_ID,
        })
    return sorted(public, key=lambda x: x.get("id") or "")

def _asset_bible_type(asset_tag: str | None) -> str:
    if not asset_tag:
        return "unknown"
    if asset_tag.startswith(("ACTOR_", "ENTITY_", "CLASS_")):
        return "character"
    if asset_tag.startswith(("PROP_", "RESOURCE_")):
        return "prop"
    if asset_tag.startswith("ENVIRONMENT_"):
        return "environment"
    if asset_tag.startswith(("STATE_", "CONDITION_", "PERM_")):
        return "state"
    if asset_tag.startswith(("ACTION_", "EVENT_")):
        return "event"
    return "unknown"

def _build_asset_bible(data: list, registry: dict) -> dict:
    assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}

    # Track dynamic assets (state updates)
    dynamic_assets = _dynamic_assets_from_data(data)

    # Build occurrences map from entities (preferred) or words (fallback)
    occ_map: dict[str, list[dict]] = {}
    for verse in data or []:
        verse_id = verse.get("verse_id")
        chapter = verse.get("chapter")
        source_id = f"verse_{str(verse_id).replace(':', '_')}" if verse_id else ""
        ent_list = (verse.get("analysis_entities") or {}).get("entities") or []
        if ent_list:
            for e in ent_list:
                asset_id = e.get("asset_id")
                if not asset_id:
                    continue
                occ_map.setdefault(asset_id, []).append({
                    "chapter": chapter,
                    "segment_label": source_id,
                    "scene_label": "",
                    "source_id": source_id,
                    "source_path": ""
                })
        else:
            for w in verse.get("words", []) or []:
                pp = w.get("pre_processing", {}) or {}
                if not pp.get("is_asset"):
                    continue
                asset_id = pp.get("asset_id")
                if not asset_id:
                    continue
                occ_map.setdefault(asset_id, []).append({
                    "chapter": chapter,
                    "segment_label": source_id,
                    "scene_label": "",
                    "source_id": source_id,
                    "source_path": ""
                })

    def _ensure_bible_entry(asset_id: str, asset_tag: str | None) -> dict:
        entry = bible.get(asset_id)
        if entry is None:
            entry = {
                "id": asset_id,
                "name": None,
                "type": _asset_bible_type(asset_tag),
                "aliases": set(),
                "roles": [],
                "visual_traits": [],
                "changes": [],
                "notes": [],
                "sources": [],
                "owner_names": [],
                "owner_subject_ids": [],
                "occurrence_count": 0,
                "is_dynamic": asset_id in dynamic_assets,
                "state_policy": "phases" if asset_id in dynamic_assets else "static",
                "states": [],
                "occurrences_sample": [],
            }
            bible[asset_id] = entry
        return entry

    bible: dict[str, dict] = {}
    for verse in data or []:
        ent_list = (verse.get("analysis_entities") or {}).get("entities") or []
        if ent_list:
            for ent in ent_list:
                asset_id = ent.get("asset_id")
                if not asset_id:
                    continue
                reg = assets.get(asset_id, {})
                asset_tag = ent.get("asset_tag") or reg.get("asset_tag") or _asset_tag_from_asset_id(asset_id)
                entry = _ensure_bible_entry(asset_id, asset_tag)
                for surface in ent.get("surface_forms") or []:
                    if surface:
                        entry["aliases"].add(surface)
                        if entry["name"] is None:
                            entry["name"] = surface
        else:
            for w in verse.get("words", []) or []:
                pp = w.get("pre_processing", {}) or {}
                if not pp.get("is_asset"):
                    continue
                asset_id = pp.get("asset_id")
                if not asset_id:
                    continue
                reg = assets.get(asset_id, {})
                asset_tag = (pp.get("ontology") or {}).get("asset_tag") or reg.get("asset_tag") or _asset_tag_from_asset_id(asset_id)
                entry = _ensure_bible_entry(asset_id, asset_tag)
                surface = w.get("text")
                if surface:
                    entry["aliases"].add(surface)
                    if entry["name"] is None:
                        entry["name"] = surface

    # Merge alias registry labels into bible if IDs match
    for aid, a in alias_registry.items():
        if aid not in bible:
            continue
        labels = a.get("labels", []) or []
        for lab in labels:
            bible[aid]["aliases"].add(lab)
        if bible[aid]["name"] is None and labels:
            bible[aid]["name"] = labels[0]

    subjects = []
    for asset_id, entry in bible.items():
        aliases = sorted(entry.pop("aliases"))
        entry["aliases"] = aliases
        if not entry.get("name"):
            entry["name"] = asset_id

        occ_list = occ_map.get(asset_id, [])
        entry["occurrence_count"] = len(occ_list)
        entry["occurrences_sample"] = occ_list[:50]
        entry["sources"] = sorted({o.get("source_id") for o in occ_list if o.get("source_id")})

        # Basic state scaffolding
        if occ_list:
            chapters = [o.get("chapter") for o in occ_list if isinstance(o.get("chapter"), int)]
            if chapters:
                cmin = min(chapters)
                cmax = max(chapters)
            else:
                cmin = 1
                cmax = 1
        else:
            cmin = 1
            cmax = 1

        if entry["is_dynamic"] and PHASE_COUNT and PHASE_COUNT > 0:
            entry["state_policy"] = "phases"
            entry["states"] = _build_phase_states(cmin, cmax, PHASE_COUNT, PHASE_LABELS)
        else:
            entry["state_policy"] = "static"
            entry["states"] = [{
                "state_id": "default",
                "label": "Default",
                "chapter_start": cmin,
                "chapter_end": cmax,
                "segment_labels": [],
                "scene_labels": [],
                "source_ids": [],
                "notes": []
            }]

        subjects.append(entry)

    data_file_base = os.path.splitext(os.path.basename(DATA_FILE))[0]
    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "story_id": STORY_ID or data_file_base,
        "timeline_id": TIMELINE_ID,
        "subjects": sorted(subjects, key=lambda x: x.get("id") or "")
    }

def _load_registry():
    global REGISTRY_CACHE
    if REGISTRY_CACHE is not None:
        return REGISTRY_CACHE
    if not os.path.exists(REGISTRY_FILE):
        REGISTRY_CACHE = {}
        return REGISTRY_CACHE
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            REGISTRY_CACHE = json.load(f)
    except Exception:
        REGISTRY_CACHE = {}
    return REGISTRY_CACHE

def _build_verse_meta(verse_obj):
    return {
        "verse_id": verse_obj.get("verse_id"),
        "verse_metrics": verse_obj.get("verse_metrics"),
        "alias_hits": verse_obj.get("alias_hits"),
        "state_triggers": verse_obj.get("state_triggers"),
        "state_updates": verse_obj.get("state_updates")
    }

def _build_registry_context(verse_obj, compact: bool = False):
    registry = _load_registry()
    assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
    if not assets:
        return []
    seen = set()
    context = []
    for w in verse_obj.get("words", []):
        pp = w.get("pre_processing", {})
        asset_id = pp.get("asset_id")
        if not asset_id or asset_id in seen:
            continue
        entry = assets.get(asset_id)
        if not entry:
            continue
        if compact:
            context.append({
                "asset_id": asset_id,
                "current_state": entry.get("current_state")
            })
        else:
            context.append({
                "asset_id": asset_id,
                "current_state": entry.get("current_state"),
                "concept": entry.get("concept"),
                "asset_tag": entry.get("asset_tag"),
                "root_key": entry.get("root_key"),
                "mentions": entry.get("mentions")
            })
        seen.add(asset_id)
    return context

def _safe_format_template(template: str, context: dict) -> str:
    class _Safe(dict):
        def __missing__(self, key):
            return ""
    try:
        return template.format_map(_Safe(context))
    except Exception:
        return template

def _extract_label_from_lex(lex: dict | None) -> str | None:
    if not lex or not isinstance(lex, dict):
        return None
    greek = (lex.get("greek_anchor") or "").strip()
    if greek:
        m = re.search(r"\(([^)]+)\)", greek)
        if m:
            return m.group(1).strip()
        ascii_part = re.sub(r"[^A-Za-z0-9 _-]", "", greek).strip()
        if ascii_part:
            return ascii_part
    gloss = (lex.get("gloss") or "").strip()
    if gloss:
        # take first segment before separators
        for sep in ["|", "/", ";"]:
            if sep in gloss:
                gloss = gloss.split(sep)[0]
                break
        gloss = re.sub(r"\s*\([^)]*\)", "", gloss).strip()
        if gloss:
            return gloss
    return None

ROOT_LABEL_BY_KEY = _build_root_label_map()

def _is_de_context() -> bool:
    candidates = [DATA_FILE, SUBJECTS_DIR, REGISTRY_FILE, REGISTRY_PUBLIC_FILE]
    for path in candidates:
        if not path:
            continue
        lower = str(path).lower()
        base = os.path.basename(lower)
        if "subjects_de" in lower or base.endswith("_de.json") or base.endswith("_de.jsonl"):
            return True
        if base in {"registry_de.json", "registry_internal_de.json"}:
            return True
    return False

def _build_wiki_seed(label: str | None) -> str | None:
    if not WEBSEARCH_SEED_WIKIPEDIA or not label:
        return None
    slug = label.strip().replace(" ", "_")
    if not slug:
        return None
    return f"https://en.wikipedia.org/wiki/{slug}"

def _build_search_url(query: str | None) -> str | None:
    if not query:
        return None
    if not WEBSEARCH_SEARCH_URL_TEMPLATE:
        return None
    q = urllib.parse.quote_plus(query)
    try:
        return WEBSEARCH_SEARCH_URL_TEMPLATE.format(query=q)
    except Exception:
        return None

def _websearch_sources_for_context() -> list[dict]:
    sources = WEBSEARCH_SOURCES_DE if _is_de_context() else WEBSEARCH_SOURCES
    if not sources:
        # Default to Wikipedia if nothing configured
        lang = "de" if _is_de_context() else "en"
        return [{"type": "wikipedia", "lang": lang}]
    return sources

def _normalize_query_terms(text: str | None) -> list[str]:
    if not text:
        return []
    cleaned = text.replace('"', " ").replace("'", " ")
    parts = re.split(r"[^\wÄÖÜäöüß]+", cleaned, flags=re.UNICODE)
    terms = []
    for p in parts:
        t = p.strip().casefold()
        if not t or len(t) < 3:
            continue
        if t in WEBSEARCH_STOPWORDS:
            continue
        terms.append(t)
    return sorted(set(terms))

def _term_hits(text: str, terms: list[str]) -> int:
    if not text or not terms:
        return 0
    lower = text.casefold()
    return sum(1 for t in terms if t in lower)

def _websearch_cache_dir() -> str | None:
    path = WEBSEARCH_CACHE_DIR_DE if _is_de_context() else WEBSEARCH_CACHE_DIR
    if not path:
        return None
    return os.path.join(os.path.dirname(__file__), "..", "..", path)

def _cache_path_for_url(url: str) -> str | None:
    cache_root = _websearch_cache_dir()
    if not cache_root:
        return None
    os.makedirs(cache_root, exist_ok=True)
    key = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return os.path.join(cache_root, f"{key}.json")

def _cache_key_for_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()

def _load_cached_source(url: str) -> dict | None:
    cache_path = _cache_path_for_url(url)
    if not cache_path or not os.path.exists(cache_path):
        return None
    if WEBSEARCH_CACHE_TTL_HOURS and WEBSEARCH_CACHE_TTL_HOURS > 0:
        age_sec = time.time() - os.path.getmtime(cache_path)
        if age_sec > WEBSEARCH_CACHE_TTL_HOURS * 3600:
            return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _save_cached_source(url: str, payload: dict) -> None:
    cache_path = _cache_path_for_url(url)
    if not cache_path:
        return
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

async def _wiki_search(session: aiohttp.ClientSession, query: str, lang: str, limit: int) -> list[dict]:
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": max(1, limit),
        "format": "json"
    }
    url = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {"User-Agent": WEBSEARCH_USER_AGENT} if WEBSEARCH_USER_AGENT else None
    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception:
        return []
    results = data.get("query", {}).get("search", []) if isinstance(data, dict) else []
    return results or []

async def _wiki_extract(session: aiohttp.ClientSession, title: str, lang: str) -> dict | None:
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": 1,
        "format": "json",
        "titles": title
    }
    url = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {"User-Agent": WEBSEARCH_USER_AGENT} if WEBSEARCH_USER_AGENT else None
    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None
    pages = data.get("query", {}).get("pages", {}) if isinstance(data, dict) else {}
    if not pages:
        return None
    page = next(iter(pages.values()))
    extract = page.get("extract") or ""
    page_title = page.get("title") or title
    page_id = page.get("pageid")
    return {
        "title": page_title,
        "page_id": page_id,
        "extract": extract
    }

async def _resolve_sources_for_job(session: aiohttp.ClientSession, job: dict) -> list[dict]:
    query = job.get("query") or ""
    max_sources = int(job.get("max_sources") or WEBSEARCH_MAX_SOURCES or 1)
    max_chars = int(job.get("max_fetch_chars") or WEBSEARCH_MAX_FETCH_CHARS or 4000)
    sources_out: list[dict] = []
    ctx = job.get("context") or {}
    preferred_terms = []
    if isinstance(ctx, dict):
        preferred_terms = _normalize_query_terms(
            " ".join([
                str(ctx.get("label") or ""),
                str(ctx.get("surface") or ""),
                str(ctx.get("actors") or ""),
                str(ctx.get("props") or ""),
                str(ctx.get("environments") or "")
            ])
        )
    query_terms = _normalize_query_terms(query)
    terms = sorted(set(query_terms + preferred_terms))
    if not terms:
        return sources_out
    for source in _websearch_sources_for_context():
        stype = (source.get("type") or "").lower()
        if stype != "wikipedia":
            continue
        lang = (source.get("lang") or "en").strip()
        results = await _wiki_search(session, query, lang, max_sources)
        for item in results:
            title = item.get("title")
            if not title:
                continue
            url = f"https://{lang}.wikipedia.org/wiki/{title.replace(' ', '_')}"
            cached = _load_cached_source(url)
            if cached:
                text = cached.get("text") or ""
                title_text = cached.get("title") or title
                title_hits = _term_hits(title_text, terms)
                body_hits = _term_hits(text, terms)
                if (title_hits + body_hits) < WEBSEARCH_MIN_TERM_HITS:
                    continue
                if WEBSEARCH_REQUIRE_TITLE_HIT and title_hits < 1:
                    continue
                sources_out.append({
                    "url": url,
                    "title": title_text,
                    "text": text[:max_chars]
                })
                if len(sources_out) >= max_sources:
                    return sources_out
                continue
            extract_data = await _wiki_extract(session, title, lang)
            if not extract_data:
                continue
            text = (extract_data.get("extract") or "")[:max_chars]
            title_text = extract_data.get("title") or title
            title_hits = _term_hits(title_text, terms)
            body_hits = _term_hits(text, terms)
            if (title_hits + body_hits) < WEBSEARCH_MIN_TERM_HITS:
                continue
            if WEBSEARCH_REQUIRE_TITLE_HIT and title_hits < 1:
                continue
            payload = {
                "url": url,
                "title": title_text,
                "text": text,
                "fetched_at": datetime.datetime.now().isoformat()
            }
            _save_cached_source(url, payload)
            sources_out.append({
                "url": url,
                "title": payload["title"],
                "text": text
            })
            if len(sources_out) >= max_sources:
                return sources_out
    return sources_out

async def _call_llm_text(session: aiohttp.ClientSession, prompt: str, max_tokens: int) -> tuple[str, str | None]:
    sys_msg = "You are a concise research summarizer."
    full_input = f"{sys_msg}\n\n{prompt}"
    current_model = random.choice(MODELS)
    sem = model_semaphores.get(current_model)
    if sem is None:
        sem = asyncio.Semaphore(MAX_CONCURRENT_PER_MODEL)
        model_semaphores[current_model] = sem
    payload = {
        "model": current_model,
        "input": full_input,
        "temperature": 0.2,
        "stream": STREAM_LM,
        "max_output_tokens": int(max_tokens)
    }
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    try:
        async with sem:
            async with session.post(LM_STUDIO_URL, json=payload, timeout=timeout, headers=API_HEADERS) as response:
                if response.status != 200:
                    return "", None
                if STREAM_LM:
                    content, response_id, _ = await _read_sse_response(response)
                    return content.strip(), response_id
                result = await response.json()
    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
        _log(f"⚠️ LLM request failed: {type(e).__name__}")
        return "", None
    except Exception as e:
        _log(f"⚠️ LLM request error: {e}")
        return "", None
    content = ""
    response_id = result.get("response_id")
    if "output" in result and isinstance(result["output"], list):
        for item in result["output"]:
            if isinstance(item, dict):
                item_type = item.get("type")
                if item_type and item_type not in ("message", "text"):
                    continue
                if "content" in item and item.get("content") is not None:
                    content += str(item.get("content", ""))
                elif "text" in item and item.get("text") is not None:
                    content += str(item.get("text", ""))
            elif isinstance(item, str):
                content += item
    elif "choices" in result:
        content = result['choices'][0]['message']['content']
    return content.strip(), response_id

async def _build_websearch_python(session: aiohttp.ClientSession, verse_obj: dict, jobs: list[dict], use_llm: bool = True) -> dict:
    jobs_out: list[dict] = []
    for job in jobs:
        sources = await _resolve_sources_for_job(session, job)
        summary_text = ""
        response_id = None
        if sources and use_llm:
            prompt = prompts.build_websearch_summary_prompt(
                job,
                sources,
                max_chars=WEBSEARCH_SUMMARY_MAX_CHARS,
                context_prefix=WEBSEARCH_CONTEXT_PREFIX
            )
            max_tokens = _compute_dynamic_max_tokens(prompt)
            stage_cap = STAGE_MAX_OUTPUT.get("websearch")
            if stage_cap:
                max_tokens = min(max_tokens, int(stage_cap))
            summary_text, response_id = await _call_llm_text(session, prompt, max_tokens)
            if summary_text and WEBSEARCH_SUMMARY_MAX_CHARS:
                summary_text = summary_text[:WEBSEARCH_SUMMARY_MAX_CHARS].rstrip()
        jobs_out.append({
            "job_id": job.get("job_id"),
            "query": job.get("query"),
            "sources": [
                {
                    "url": s.get("url"),
                    "title": s.get("title"),
                    "cache_key": _cache_key_for_url(s.get("url") or "")
                } for s in sources
            ],
            "summary": summary_text,
            "confidence": "medium" if summary_text else ("low" if use_llm else "none"),
            "notes": ""
        })
        if response_id:
            job_state = verse_obj.setdefault("state_ids", {}).setdefault("websearch_jobs", {})
            job_state[job.get("job_id") or f"job_{len(job_state)+1}"] = {
                "id": response_id,
                "model": None
            }
    return {
        "schema": "websearch.v1",
        "status": "complete",
        "jobs": jobs_out
    }

def _entity_query_label(entity: dict, alias_registry: dict | None) -> tuple[str | None, str | None]:
    asset_id = entity.get("asset_id")
    if alias_registry and asset_id in alias_registry:
        labels = alias_registry[asset_id].get("labels") or []
        if labels:
            return labels[0], None
    surfaces = entity.get("surface_forms") or []
    for surface in surfaces:
        lex = lookup_lex(surface)
        label = _extract_label_from_lex(lex)
        if label:
            return label, surface
    root = entity.get("root") or ""
    if root:
        label = ROOT_LABEL_BY_KEY.get(_normalize_root_key(root))
        if label:
            return label, None
    if surfaces:
        return surfaces[0], surfaces[0]
    return asset_id, None

def _subject_type_from_entity(entity: dict) -> str:
    category = (entity.get("category") or "").lower()
    if category == "actor":
        return "actor"
    if category in ("prop", "resource"):
        return "prop"
    if category in ("place", "environment"):
        return "environment"
    if category in ("event", "state"):
        return category
    # fallback from asset_tag
    asset_tag = entity.get("asset_tag") or ""
    if asset_tag.startswith(("ACTOR_", "ENTITY_", "CLASS_")):
        return "actor"
    if asset_tag.startswith(("PROP_", "RESOURCE_")):
        return "prop"
    if asset_tag.startswith("ENVIRONMENT_"):
        return "environment"
    if asset_tag.startswith(("STATE_", "CONDITION_", "PERM_")):
        return "state"
    if asset_tag.startswith(("ACTION_", "EVENT_")):
        return "event"
    return "unknown"

def _join_labels(labels: list[str], limit: int = 4) -> str:
    if not labels:
        return ""
    uniq = []
    seen = set()
    for l in labels:
        if not l:
            continue
        key = l.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(l)
        if len(uniq) >= limit:
            break
    return ", ".join(uniq)

def _collect_spatial_context(words: list[dict]) -> dict:
    layers = []
    modes = []
    for w in words or []:
        pp = w.get("pre_processing", {}) or {}
        spatial = pp.get("spatial") or {}
        mode = spatial.get("mode")
        layer_id = spatial.get("layer_id")
        if mode:
            modes.append(mode)
        if layer_id:
            layers.append(layer_id)
    # unique
    modes = sorted({m for m in modes if m})
    layers = sorted({l for l in layers if l})
    return {"modes": modes, "layers": layers}

def _shorten_text(text: str, limit: int = 160) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"

def _normalize_scope_list(value) -> set:
    if not value:
        return set()
    if isinstance(value, str):
        return {value.strip()}
    if isinstance(value, (list, tuple, set)):
        return {str(v).strip() for v in value if str(v).strip()}
    return set()

WEBSEARCH_CONTEXT_SCOPE_SET = _normalize_scope_list(WEBSEARCH_CONTEXT_SCOPE)

def _context_scope_allows(scope: str) -> bool:
    if not WEBSEARCH_CONTEXT_SCOPE_SET:
        return True
    if "all" in WEBSEARCH_CONTEXT_SCOPE_SET:
        return True
    return scope in WEBSEARCH_CONTEXT_SCOPE_SET

def _compact_context_window(context_window: dict | None) -> dict | None:
    if not isinstance(context_window, dict):
        return None
    if not WEBSEARCH_COMPACT_CONTEXT:
        return context_window
    current = context_window.get("current")
    return {"current": current} if current else None

def _compact_context_dict(context: dict | None) -> dict | None:
    if not isinstance(context, dict):
        return None
    out = {}
    for key, value in context.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, dict) and not value:
            continue
        out[key] = value
    return out or None

def _summarize_entities_by_type(entities: list[dict], alias_registry: dict | None) -> dict:
    out = {"actors": [], "props": [], "environments": [], "events": [], "states": [], "others": []}
    for ent in entities or []:
        label, _ = _entity_query_label(ent, alias_registry)
        label = label or ent.get("asset_id")
        stype = _subject_type_from_entity(ent)
        if stype == "actor":
            out["actors"].append(label)
        elif stype == "prop":
            out["props"].append(label)
        elif stype == "environment":
            out["environments"].append(label)
        elif stype == "event":
            out["events"].append(label)
        elif stype == "state":
            out["states"].append(label)
        else:
            out["others"].append(label)

    for k in out:
        out[k] = _join_labels(out[k]).split(", ") if out[k] else []
    return out

def _context_entry_for_map(verse_obj: dict, alias_registry: dict | None, excerpt_limit: int = 0) -> dict:
    ent_list = (verse_obj.get("analysis_entities") or {}).get("entities", []) or []
    summary = _summarize_entities_by_type(ent_list, alias_registry)
    spatial = _collect_spatial_context(verse_obj.get("words", []) or [])
    entry = {
        "verse_id": verse_obj.get("verse_id"),
        "chapter": verse_obj.get("chapter"),
        "verse": verse_obj.get("verse"),
        "actors": summary.get("actors", []),
        "props": summary.get("props", []),
        "environments": summary.get("environments", []),
        "events": summary.get("events", []),
        "states": summary.get("states", []),
        "spatial_modes": spatial.get("modes", []),
        "spatial_layers": spatial.get("layers", [])
    }
    if excerpt_limit and excerpt_limit > 0:
        entry["text_excerpt"] = _shorten_text(verse_obj.get("text", ""), excerpt_limit)
    return entry

def _build_websearch_context_map(data: list, registry: dict | None, window: int) -> dict:
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}
    base = [_context_entry_for_map(v, alias_registry, WEBSEARCH_CONTEXT_EXCERPT_CHARS) for v in data or []]
    mapping = {}
    for idx, current in enumerate(base):
        prev_list = []
        next_list = []
        for off in range(1, window + 1):
            if idx - off >= 0:
                prev_list.append(base[idx - off])
            if idx + off < len(base):
                next_list.append(base[idx + off])
        mapping[current.get("verse_id")] = {
            "current": current,
            "prev": prev_list,
            "next": next_list
        }
    return mapping

def _get_context_window(verse_id: str | None) -> dict:
    if not verse_id:
        return {"current": None, "prev": [], "next": []}
    return VERSE_CONTEXT_MAP.get(verse_id) or {"current": None, "prev": [], "next": []}

def _load_parallel_links(path: str | None) -> dict:
    if not path:
        return {}
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    links = data.get("links") if isinstance(data, dict) else None
    if not isinstance(links, list):
        return {}
    mapping = {}
    for link in links:
        vid = link.get("verse_id")
        if not vid:
            continue
        # Keep only the subject lists + shared subjects
        mapping[vid] = {
            "shared_subjects": (link.get("shared_subjects") or [])[:WEBSEARCH_PARALLEL_MAX_SUBJECTS],
            "langs": {k: v for k, v in link.items() if k not in {"verse_id", "chapter", "verse", "shared_subjects"}}
        }
    return mapping

def _parallel_link_context(verse_id: str | None) -> dict | None:
    if not verse_id:
        return None
    return PARALLEL_LINKS_MAP.get(verse_id)

def _build_websearch_jobs(verse_obj: dict, registry: dict | None) -> list[dict]:
    ent_list = (verse_obj.get("analysis_entities") or {}).get("entities", []) or []
    alias_registry = registry.get("aliases", {}) if isinstance(registry, dict) else {}
    verse_id = verse_obj.get("verse_id") or ""
    chapter = verse_obj.get("chapter")
    verse_num = verse_obj.get("verse")
    words = verse_obj.get("words", []) or []
    spatial_ctx = _collect_spatial_context(words)
    context_window = _get_context_window(verse_id)
    parallel_ctx = _parallel_link_context(verse_id)
    env_labels = []
    prop_labels = []
    actor_labels = []

    jobs: list[dict] = []
    seen_queries = set()
    total_limit = WEBSEARCH_MAX_TOTAL_JOBS if WEBSEARCH_MAX_TOTAL_JOBS > 0 else None
    entity_templates_by_type = WEBSEARCH_ENTITY_TEMPLATES_BY_TYPE if isinstance(WEBSEARCH_ENTITY_TEMPLATES_BY_TYPE, dict) else {}

    def _build_job_context(scope: str, base_ctx: dict) -> dict:
        ctx = dict(base_ctx or {})
        include_scope = _context_scope_allows(scope)
        if WEBSEARCH_CONTEXT_WINDOW > 0 and include_scope:
            ctx_window = _compact_context_window(context_window)
            if ctx_window:
                ctx["context_window"] = ctx_window
        if include_scope and parallel_ctx:
            ctx["parallel_links"] = parallel_ctx
        compacted = _compact_context_dict(ctx)
        return compacted if compacted is not None else {}

    def _priority_for(stype: str, scope: str) -> int:
        if scope == "actor_prop":
            return 2
        if scope == "scene":
            return 4
        if stype == "actor":
            return 1
        if stype == "environment":
            return 3
        if stype == "prop":
            return 5
        if stype in ("event", "state"):
            return 6
        return 9

    # Sort entities so actors/environments take priority before prop/state
    ent_list = sorted(ent_list, key=lambda e: _priority_for(_subject_type_from_entity(e), "entity"))

    # Pre-collect labels for context
    for ent in ent_list:
        label, _ = _entity_query_label(ent, alias_registry)
        stype = _subject_type_from_entity(ent)
        if stype == "environment" and label:
            env_labels.append(label)
        elif stype == "prop" and label:
            prop_labels.append(label)
        elif stype == "actor" and label:
            actor_labels.append(label)

    # Entity-driven queries
    for ent in ent_list:
        asset_id = ent.get("asset_id") or ""
        label, surface = _entity_query_label(ent, alias_registry)
        root_key = ent.get("root_key") or ""
        concept = ent.get("concept") or ""
        stype = _subject_type_from_entity(ent)
        ctx = {
            "label": label or "",
            "surface": surface or "",
            "asset_id": asset_id,
            "root_key": root_key,
            "concept": concept,
            "verse_id": verse_id,
            "chapter": chapter,
            "verse": verse_num,
            "subject_type": stype,
            "props": _join_labels(prop_labels),
            "actors": _join_labels(actor_labels),
            "environments": _join_labels(env_labels),
            "spatial_modes": _join_labels(spatial_ctx.get("modes", [])),
            "spatial_layers": _join_labels(spatial_ctx.get("layers", []))
        }
        templates = entity_templates_by_type.get(stype) or WEBSEARCH_ENTITY_TEMPLATES
        for idx, template in enumerate(templates):
            if idx >= WEBSEARCH_MAX_ENTITY_JOBS:
                break
            query = _safe_format_template(template, ctx).strip()
            if not query:
                continue
            key = query.lower()
            if key in seen_queries:
                continue
            seen_queries.add(key)
            job_id = f"ent:{asset_id}:{idx + 1}"
            seeds = []
            wiki_seed = _build_wiki_seed(label)
            if WEBSEARCH_USE_SEED_URLS and wiki_seed:
                seeds.append(wiki_seed)
            search_url = _build_search_url(query)
            jobs.append({
                "job_id": job_id,
                "scope": "entity",
                "subject_type": stype,
                "subject_id": asset_id,
                "label": label,
                "surface": surface,
                "root_key": root_key,
                "concept": concept,
                "query": query,
                "search_url": search_url,
                "context": _build_job_context("entity", {
                    "props": ctx["props"],
                    "actors": ctx["actors"],
                    "environments": ctx["environments"],
                    "spatial_modes": ctx["spatial_modes"],
                    "spatial_layers": ctx["spatial_layers"]
                }),
                "rerank": {
                    "enabled": WEBSEARCH_RERANK_ENABLED,
                    "max_candidates": WEBSEARCH_RERANK_MAX
                },
                "seed_urls": seeds,
                "max_sources": WEBSEARCH_MAX_SOURCES,
                "max_fetch_chars": WEBSEARCH_MAX_FETCH_CHARS,
                "_priority": _priority_for(stype, "entity")
            })

        # Actor -> props linker (extra jobs)
        if stype == "actor" and prop_labels:
            for pidx, tpl in enumerate(WEBSEARCH_ACTOR_PROP_TEMPLATES):
                query = _safe_format_template(tpl, ctx).strip()
                if not query:
                    continue
                key = query.lower()
                if key in seen_queries:
                    continue
                seen_queries.add(key)
                job_id = f"actorprop:{asset_id}:{pidx + 1}"
                search_url = _build_search_url(query)
                seed_urls = list(seeds)
                jobs.append({
                    "job_id": job_id,
                    "scope": "actor_prop",
                    "subject_type": stype,
                    "subject_id": asset_id,
                    "label": label,
                    "surface": surface,
                    "root_key": root_key,
                    "concept": concept,
                    "query": query,
                    "search_url": search_url,
                    "context": _build_job_context("actor_prop", {
                        "props": ctx["props"],
                        "actors": ctx["actors"],
                        "environments": ctx["environments"],
                        "spatial_modes": ctx["spatial_modes"],
                        "spatial_layers": ctx["spatial_layers"]
                    }),
                    "rerank": {
                        "enabled": WEBSEARCH_RERANK_ENABLED,
                        "max_candidates": WEBSEARCH_RERANK_MAX
                    },
                    "seed_urls": seed_urls if WEBSEARCH_USE_SEED_URLS else [],
                    "max_sources": WEBSEARCH_MAX_SOURCES,
                    "max_fetch_chars": WEBSEARCH_MAX_FETCH_CHARS,
                    "_priority": _priority_for(stype, "actor_prop")
                })

    # Scene-driven queries (setting / environment)
    scene_templates = WEBSEARCH_SCENE_TEMPLATES if WEBSEARCH_SCENE_TEMPLATES else WEBSEARCH_VERSE_TEMPLATES
    for idx, template in enumerate(scene_templates):
        if idx >= WEBSEARCH_MAX_VERSE_JOBS:
            break
        ctx = {
            "verse_id": verse_id,
            "chapter": chapter,
            "verse": verse_num,
            "environments": _join_labels(env_labels),
            "spatial_modes": _join_labels(spatial_ctx.get("modes", [])),
            "spatial_layers": _join_labels(spatial_ctx.get("layers", []))
        }
        query = _safe_format_template(template, ctx).strip()
        if not query:
            continue
        key = query.lower()
        if key in seen_queries:
            continue
        seen_queries.add(key)
        job_id = f"scene:{verse_id}:{idx + 1}"
        search_url = _build_search_url(query)
        seeds = []
        jobs.append({
            "job_id": job_id,
            "scope": "scene",
            "subject_type": "scene",
            "subject_id": "",
            "label": "",
            "surface": "",
            "root_key": "",
            "concept": "",
            "query": query,
            "search_url": search_url,
            "context": _build_job_context("scene", {
                "environments": ctx["environments"],
                "spatial_modes": ctx["spatial_modes"],
                "spatial_layers": ctx["spatial_layers"]
            }),
            "rerank": {
                "enabled": WEBSEARCH_RERANK_ENABLED,
                "max_candidates": WEBSEARCH_RERANK_MAX
            },
            "seed_urls": seeds if WEBSEARCH_USE_SEED_URLS else [],
            "max_sources": WEBSEARCH_MAX_SOURCES,
            "max_fetch_chars": WEBSEARCH_MAX_FETCH_CHARS,
            "_priority": _priority_for("scene", "scene")
        })

    if total_limit and len(jobs) > total_limit:
        jobs.sort(key=lambda j: j.get("_priority", 99))
        jobs = jobs[:total_limit]
    for j in jobs:
        if "_priority" in j:
            j.pop("_priority", None)
    return jobs

def _build_websearch_local(verse_obj: dict, registry: dict | None) -> dict:
    jobs = _build_websearch_jobs(verse_obj, registry)
    return {
        "schema": "websearch.v1",
        "status": "pending",
        "jobs": jobs
    }

def _normalize_websearch_output(data: dict | None, input_jobs: list[dict] | None = None) -> dict:
    if not isinstance(data, dict):
        data = {}
    jobs_out = data.get("jobs")
    if not isinstance(jobs_out, list):
        jobs_out = []

    input_map = {}
    for j in input_jobs or []:
        jid = j.get("job_id")
        if jid:
            input_map[jid] = j

    normalized_jobs = []
    for job in jobs_out:
        if not isinstance(job, dict):
            continue
        jid = job.get("job_id")
        base = input_map.get(jid, {})
        if "query" not in job and base:
            job["query"] = base.get("query")
        if "context" not in job and base:
            job["context"] = base.get("context")
        if "rerank" not in job and base:
            job["rerank"] = base.get("rerank")
        sources = job.get("sources")
        if not isinstance(sources, list):
            sources = []
            job["sources"] = sources
        if WEBSEARCH_RERANK_ENABLED:
            if "rerank_candidates" not in job:
                max_c = WEBSEARCH_RERANK_MAX if WEBSEARCH_RERANK_MAX > 0 else len(sources)
                job["rerank_candidates"] = sources[:max_c]
        normalized_jobs.append(job)

    # Add any missing jobs as pending shells
    seen_ids = {j.get("job_id") for j in normalized_jobs}
    for jid, base in input_map.items():
        if jid in seen_ids:
            continue
        normalized_jobs.append({
            "job_id": jid,
            "query": base.get("query"),
            "sources": [],
            "facts": [],
            "confidence": "low",
            "notes": "missing_output",
            "context": base.get("context"),
            "rerank": base.get("rerank"),
            "rerank_candidates": []
        })

    data["schema"] = data.get("schema") or "websearch.v1"
    data["status"] = data.get("status") or "complete"
    data["jobs"] = normalized_jobs
    return data

def _compact_websearch_jobs(
    jobs: list[dict],
    drop_context_window: bool = False,
    drop_parallel_links: bool = False,
    drop_seed_urls: bool = False
) -> list[dict]:
    compacted: list[dict] = []
    for job in jobs or []:
        if not isinstance(job, dict):
            continue
        slim = {}
        for key in ["job_id", "scope", "subject_type", "subject_id", "label", "surface", "root_key", "concept", "query"]:
            val = job.get(key)
            if val is None:
                continue
            if isinstance(val, str) and not val.strip():
                continue
            slim[key] = val
        search_url = job.get("search_url")
        if search_url:
            slim["search_url"] = search_url
        if not drop_seed_urls:
            seeds = job.get("seed_urls") or []
            if seeds:
                slim["seed_urls"] = seeds
        for key in ["max_sources", "max_fetch_chars", "rerank"]:
            if key in job and job.get(key) is not None:
                slim[key] = job.get(key)

        ctx = job.get("context")
        if isinstance(ctx, dict):
            new_ctx = {}
            for key in ["props", "actors", "environments", "spatial_modes", "spatial_layers"]:
                val = ctx.get(key)
                if val:
                    new_ctx[key] = val
            if not drop_context_window:
                ctx_window = ctx.get("context_window")
                if ctx_window:
                    new_ctx["context_window"] = ctx_window
            if not drop_parallel_links:
                parallel_links = ctx.get("parallel_links")
                if parallel_links:
                    new_ctx["parallel_links"] = parallel_links
            new_ctx = _compact_context_dict(new_ctx)
            if new_ctx:
                slim["context"] = new_ctx
        compacted.append(slim)
    return compacted

def _websearch_tool_defs() -> list[dict]:
    return [{
        "type": "function",
        "function": {
            "name": WEBSEARCH_TOOL_NAME,
            "description": "Fetch a URL and return its contents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "max_length": {"type": "integer"},
                    "start_index": {"type": "integer"},
                    "raw": {"type": "boolean"}
                },
                "required": ["url"]
            }
        }
    }]

async def _read_sse_response(response: aiohttp.ClientResponse) -> tuple[str, str | None, dict | None]:
    buffer = ""
    content_parts: list[str] = []
    response_id = None
    result_obj = None

    async for chunk in response.content.iter_any():
        try:
            buffer += chunk.decode("utf-8", errors="ignore")
        except Exception:
            continue
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            line = line.strip()
            if not line.startswith("data:"):
                continue
            data_str = line[5:].lstrip()
            if data_str == "[DONE]":
                continue
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            if "result" in data and isinstance(data["result"], dict):
                if data["result"].get("response_id"):
                    response_id = data["result"]["response_id"]

            event_type = data.get("type")
            if event_type in ("message.delta", "output_text.delta", "response.output_text.delta"):
                delta = data.get("content") or data.get("delta") or ""
                if delta:
                    content_parts.append(str(delta))
            elif event_type in ("message", "message.completed", "response.output_text"):
                content = data.get("content") or ""
                if content:
                    content_parts.append(str(content))
            elif event_type == "chat.end":
                result_obj = data.get("result")
                if isinstance(result_obj, dict) and result_obj.get("response_id"):
                    response_id = result_obj["response_id"]

    return "".join(content_parts), response_id, result_obj

async def _log_dry_run(record: dict):
    global DRY_RUN_LOCK
    if not DRY_RUN_OUT:
        return
    if DRY_RUN_LOCK is None:
        DRY_RUN_LOCK = asyncio.Lock()
    async with DRY_RUN_LOCK:
        out_dir = os.path.dirname(DRY_RUN_OUT)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(DRY_RUN_OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

async def analyze_stage(session, verse_obj, stage):
    # Store results in distinct keys: analysis_morphologic, analysis_syntactic, etc.
    result_key = f"analysis_{stage}"
    
    # Special handling for LLM Review Modes (Save to _review)
    if stage == "graphematic" and GRAPHEMATIC_MODE == "llm":
        result_key = "analysis_graphematic_review"
    elif stage == "morphologic" and MORPHOLOGIC_MODE == "llm":
        result_key = "analysis_morphologic_review"
    elif stage == "syntactic" and SYNTACTIC_MODE == "llm":
        result_key = "analysis_syntactic_review"
    elif stage == "translation" and TRANSLATION_MODE != "json":
        result_key = "analysis_translation_draft"

    # Skip if already done
    if not _is_stage_pending(verse_obj, stage):
        return verse_obj

    # Prepare Context & Prompt
    text = verse_obj.get("text", "")
    prompt = ""
    prompt_full = None
    prompt_chained = None
    previous_response_id = None
    previous_model = None
    websearch_input_jobs = None
    verse_meta = _build_verse_meta(verse_obj)
    compact_meta = PROMPT_COMPACT_MODE in {"compact", "auto"}
    registry_context = _build_registry_context(verse_obj, compact=compact_meta)
    if DRY_RUN:
        if stage == 'graphematic':
            if GRAPHEMATIC_MODE == "llm":
                prompt = prompts.build_graphematic_prompt(text, verse_meta=verse_meta)
            else:
                print(f"ℹ️ Dry Run: Graphematic Local Mode (No LLM prompt)")
                return verse_obj

        elif stage == 'morphologic':
            words = verse_obj.get("words") or []
            token_source = words if words else re.split(r'[፡።\s]+', text)
            if MORPHOLOGIC_MODE == "text":
                print("ℹ️ Dry Run: Morphologic Text Mode (Python-only, no LLM prompt)")
                return verse_obj
            elif MORPHOLOGIC_MODE == "llm":
                morph_data = verse_obj.get("analysis_morphologic") or _build_morphologic_local(token_source)
                prompt = prompts.build_morphology_review_prompt(morph_data, verse_meta=verse_meta)
            else:
                prompt = prompts.build_morphology_prompt(token_source, ["N", "V", "ADJ"], verse_meta=verse_meta)

        elif stage == 'syntactic':
            if SYNTACTIC_MODE == 'heuristic':
                print(f"ℹ️ Dry Run: Syntactic Heuristic Mode (No LLM prompt)")
                return verse_obj
            if SYNTACTIC_MODE == "llm":
                syn_data = verse_obj.get("analysis_syntactic") or _build_syntax_heuristic(verse_obj)
                prompt = prompts.build_syntax_review_prompt(syn_data, verse_meta=verse_meta)
            else:
                morph_data = verse_obj.get("analysis_morphologic") or {}
                prompt_full = prompts.build_syntax_prompt(morph_data, verse_meta=verse_meta, registry_context=registry_context)
                prompt = prompt_full

        elif stage == 'semantic':
            syn_data = verse_obj.get("analysis_syntactic") or {}
            parses = syn_data.get("syntax", {}).get("parses", [])
            # Fallback logic mirroring analyze_stage
            prompt_full = prompts.build_semantic_prompt_with_skins(parses, genre="neutral", verse_meta=verse_meta, registry_context=registry_context)
            prompt = prompt_full

        elif stage == 'translation':
            syn_data = verse_obj.get("analysis_syntactic") or {}
            morph_data = verse_obj.get("analysis_morphologic") or {}
            sem_data = verse_obj.get("analysis_semantic")
            draft_data = verse_obj.get("analysis_translation_draft")
            parses = syn_data.get("syntax", {}).get("parses", [])
            tokens = morph_data.get("tokens", [])
            
            if TRANSLATION_MODE == 'json':
                combined_context = str(sem_data or "")
                if draft_data:
                    combined_context += "\n\nTRANSLATION DRAFT:\n" + str(draft_data)
                prompt_full = prompts.build_translation_prompt(parses, tokens, semantic_analysis=combined_context, verse_meta=verse_meta, registry_context=registry_context)
            else:
                prompt_full = prompts.build_translation_draft_prompt(parses, tokens, semantic_analysis=sem_data, verse_meta=verse_meta)
            
            prompt = prompt_full

        elif stage == 'entities':
            print("ℹ️ Dry Run: Entities Local Mode (Python-only, no LLM prompt)")
            return verse_obj
        elif stage == 'websearch':
            if WEBSEARCH_MODE == "local":
                print("ℹ️ Dry Run: Websearch Local Mode (Python-only, no LLM prompt)")
                return verse_obj
            registry = _load_registry()
            jobs = _build_websearch_jobs(verse_obj, registry)
            websearch_input_jobs = jobs
            ws_registry_context = None
            if WEBSEARCH_INCLUDE_REGISTRY_CONTEXT:
                ws_registry_context = _build_registry_context(verse_obj, compact=True)
            drop_context = WEBSEARCH_COMPACT_CONTEXT
            jobs_for_prompt = _compact_websearch_jobs(
                jobs,
                drop_context_window=drop_context,
                drop_parallel_links=drop_context
            )
            prompt = prompts.build_websearch_prompt(
                jobs_for_prompt,
                verse_meta=verse_meta,
                registry_context=ws_registry_context,
                tool_name=WEBSEARCH_TOOL_NAME,
                search_tool=WEBSEARCH_SEARCH_TOOL,
                fetch_tool=WEBSEARCH_FETCH_TOOL,
                context_prefix=WEBSEARCH_CONTEXT_PREFIX
            )
            if PROMPT_COMPACT_MODE == "auto" and len(str(prompt)) > PROMPT_COMPACT_THRESHOLD:
                jobs_for_prompt = _compact_websearch_jobs(
                    jobs,
                    drop_context_window=True,
                    drop_parallel_links=True
                )
                ws_registry_context = None
                prompt = prompts.build_websearch_prompt(
                    jobs_for_prompt,
                    verse_meta=verse_meta,
                    registry_context=ws_registry_context,
                    tool_name=WEBSEARCH_TOOL_NAME,
                    search_tool=WEBSEARCH_SEARCH_TOOL,
                    fetch_tool=WEBSEARCH_FETCH_TOOL,
                    context_prefix=WEBSEARCH_CONTEXT_PREFIX
                )

    if DRY_RUN and prompt:
        sys_msg = "You are a specialized linguistic analyzer. Follow the custom format strictly."
        dynamic_max_tokens = _compute_dynamic_max_tokens(str(prompt))
        use_stateful = previous_response_id is not None
        dry_input = prompt if use_stateful else f"{sys_msg}\n\n{prompt}"
        est_input = _estimate_input_tokens(dry_input)
        record = {
            "stage": stage,
            "verse_id": verse_obj.get("verse_id"),
            "use_stateful": use_stateful,
            "previous_response_id": previous_response_id,
            "model_hint": previous_model,
            "prompt_chars": len(dry_input),
            "prompt": dry_input,
            "max_output_tokens": int(dynamic_max_tokens),
            "estimated_input_tokens": est_input,
            "projected_total_tokens": int(est_input + dynamic_max_tokens),
            "context_window_assumption": CONTEXT_WINDOW_ASSUMPTION
        }
        await _log_dry_run(record)
        return verse_obj
    
    # Check for State Context from previous stages
    state_ids = verse_obj.get("state_ids", {})
    
    if stage == 'graphematic':
        if GRAPHEMATIC_MODE == "local":
            verse_obj[result_key] = _build_graphematic_local_v2(text, verse_obj.get("words"))
            print(f"✅ {stage.upper()} {verse_obj['verse_id']} [local]")
            return verse_obj
        # LLM mode
        prompt = prompts.build_graphematic_prompt(text, verse_meta=verse_meta)

    elif stage == 'morphologic':
        # Linguistic Compiler Mode: We tokenize first
        words = verse_obj.get("words") or []
        if words:
            token_source = words
        else:
            raw_tokens = re.split(r'[፡።\s]+', text)
            token_source = [t.strip() for t in raw_tokens if t.strip()]
        if MORPHOLOGIC_MODE == "text":
            verse_obj[result_key] = _build_morphologic_local(token_source)
            print(f"✅ {stage.upper()} {verse_obj['verse_id']} [local]")
            return verse_obj
        elif MORPHOLOGIC_MODE == "llm":
            if verse_obj.get("analysis_morphologic") is None:
                verse_obj["analysis_morphologic"] = _build_morphologic_local(token_source)
            prompt = prompts.build_morphology_review_prompt(verse_obj.get("analysis_morphologic") or {}, verse_meta=verse_meta)
        else:
            compact_request = PROMPT_COMPACT_MODE == "compact"
            reg_ctx = None if compact_request else registry_context
            prompt = prompts.build_morphology_prompt(
                token_source,
                ["N", "V", "ADJ", "PRON", "PREP", "ADV", "CONJ"],
                verse_meta=verse_meta,
                registry_context=reg_ctx,
                compact=compact_request
            )
            if PROMPT_COMPACT_MODE == "auto" and len(str(prompt)) > PROMPT_COMPACT_THRESHOLD:
                prompt = prompts.build_morphology_prompt(
                    token_source,
                    ["N", "V", "ADJ", "PRON", "PREP", "ADV", "CONJ"],
                    verse_meta=verse_meta,
                    registry_context=None,
                    compact=True
                )
        
    elif stage == 'syntactic':
        if SYNTACTIC_MODE == "heuristic":
            verse_obj[result_key] = _build_syntax_heuristic(verse_obj)
            print(f"✅ {stage.upper()} {verse_obj['verse_id']} [heuristic]")
            return verse_obj
        if SYNTACTIC_MODE == "llm":
            if verse_obj.get("analysis_syntactic") is None:
                verse_obj["analysis_syntactic"] = _build_syntax_heuristic(verse_obj)
            prompt = prompts.build_syntax_review_prompt(verse_obj.get("analysis_syntactic") or {}, verse_meta=verse_meta)
            previous_response_id = None
            previous_model = None
        else:
            # Check if we have a stateful context from morphology
            morph_id, morph_model = _get_state_entry(state_ids, "morphologic")
            morph_data = verse_obj.get("analysis_morphologic", {})
            prompt_full = prompts.build_syntax_prompt(morph_data, verse_meta=verse_meta, registry_context=registry_context) if morph_data else None
            prompt_chained = prompts.build_syntax_prompt_chained(verse_meta=verse_meta, registry_context=registry_context)

            if morph_id and not _state_model_allowed(morph_model):
                print("⚠️ Unknown model for morphologic state. Falling back to full prompt.")
                morph_id = None
                morph_model = None
            elif morph_id and morph_model is None:
                if len(MODELS) == 1:
                    morph_model = MODELS[0]
                else:
                    print("⚠️ Legacy morphologic state without model. Falling back to full prompt.")
                    morph_id = None
                    morph_model = None
            
            if morph_id:
                print(f"🔗 Chaining SYNTAX to MORPHOLOGY (ID: {morph_id})")
                prompt = prompt_chained
                previous_response_id = morph_id
                previous_model = morph_model
            else:
                if not prompt_full:
                     print("⚠️ Missing morphologic data for syntactic stage")
                     return verse_obj
                prompt = prompt_full
        
    elif stage == 'semantic':
        syn_id, syn_model = _get_state_entry(state_ids, "syntactic")
        syn_data = verse_obj.get("analysis_syntactic", {})
        parses = syn_data.get("syntax", {}).get("parses", [])
        prompt_full = prompts.build_semantic_prompt_with_skins(parses, genre="neutral", verse_meta=verse_meta, registry_context=registry_context) if parses else None
        prompt_chained = prompts.build_semantic_prompt_chained(genre="neutral", verse_meta=verse_meta, registry_context=registry_context)
        
        if syn_id and not _state_model_allowed(syn_model):
            print("⚠️ Unknown model for syntactic state. Falling back to full prompt.")
            syn_id = None
            syn_model = None
        elif syn_id and syn_model is None:
            if len(MODELS) == 1:
                syn_model = MODELS[0]
            else:
                print("⚠️ Legacy syntactic state without model. Falling back to full prompt.")
                syn_id = None
                syn_model = None
        
        if syn_id:
            print(f"🔗 Chaining SEMANTIC to SYNTAX (ID: {syn_id})")
            prompt = prompt_chained
            previous_response_id = syn_id
            previous_model = syn_model
        else:
            if not prompt_full:
                 print("⚠️ Missing syntax parses for semantic stage")
                 return verse_obj
            prompt = prompt_full
        
    elif stage == 'translation':
        sem_id, sem_model = _get_state_entry(state_ids, "semantic")
        syn_data = verse_obj.get("analysis_syntactic", {})
        morph_data = verse_obj.get("analysis_morphologic", {})
        sem_data = verse_obj.get("analysis_semantic")
        draft_data = verse_obj.get("analysis_translation_draft")
        
        parses = syn_data.get("syntax", {}).get("parses", [])
        tokens = morph_data.get("tokens", [])
        
        # MODE SWITCH: Draft (Text) vs. Final (JSON)
        if TRANSLATION_MODE == 'json':
            # STAGE 2: JSON Generation (uses Draft if available)
            combined_context = str(sem_data or "")
            if draft_data:
                combined_context += "\n\nTRANSLATION DRAFT:\n" + str(draft_data)
                
            prompt_full = prompts.build_translation_prompt(parses, tokens, semantic_analysis=combined_context, verse_meta=verse_meta, registry_context=registry_context)
            prompt_chained = prompts.build_translation_prompt_chained(verse_meta=verse_meta, registry_context=registry_context)
        else:
            # STAGE 1: Reasoning / Draft (Text)
            prompt_full = prompts.build_translation_draft_prompt(parses, tokens, semantic_analysis=sem_data, verse_meta=verse_meta)
            prompt_chained = None # Draft should usually see the full semantic context, not just rely on implicit state

        if sem_id and not _state_model_allowed(sem_model):
            print("⚠️ Unknown model for semantic state. Falling back to full prompt.")
            sem_id = None
            sem_model = None
        elif sem_id and sem_model is None:
            if len(MODELS) == 1:
                sem_model = MODELS[0]
            else:
                print("⚠️ Legacy semantic state without model. Falling back to full prompt.")
                sem_id = None
                sem_model = None
        
        # Determine if we can chain
        # If we are in JSON mode, we might chain to the Draft (if it was the last action)
        # But Draft was text, so state might be messy. Safer to use prompt_full if switching modes.
        if sem_id and prompt_chained and TRANSLATION_MODE == 'json': 
             # Only chain in JSON mode if we trust the semantic state ID
             print(f"🔗 Chaining TRANSLATION (JSON) to SEMANTIC (ID: {sem_id})")
             prompt = prompt_chained
             previous_response_id = sem_id
             previous_model = sem_model
        else:
             if not prompt_full:
                 print("⚠️ Missing context for translation stage")
                 return verse_obj
             prompt = prompt_full

    elif stage == 'entities':
        verse_obj[result_key] = _build_entities_local(verse_obj)
        return verse_obj
    elif stage == 'websearch':
        registry = _load_registry()
        if WEBSEARCH_MODE == "local":
            verse_obj[result_key] = _build_websearch_local(verse_obj, registry)
            return verse_obj
        jobs = _build_websearch_jobs(verse_obj, registry)
        websearch_input_jobs = jobs
        if not WEBSEARCH_USE_TOOLS:
            use_llm = WEBSEARCH_SUMMARY_LLM and WEBSEARCH_MODE != "fetch"
            verse_obj[result_key] = await _build_websearch_python(session, verse_obj, jobs, use_llm=use_llm)
            _log(f"✅ WEBSEARCH {verse_obj['verse_id']} [{'python+llm' if use_llm else 'python-only'}]")
            return verse_obj
        ws_registry_context = None
        if WEBSEARCH_INCLUDE_REGISTRY_CONTEXT:
            ws_registry_context = _build_registry_context(verse_obj, compact=True)
        drop_context = WEBSEARCH_COMPACT_CONTEXT
        jobs_for_prompt = _compact_websearch_jobs(
            jobs,
            drop_context_window=drop_context,
            drop_parallel_links=drop_context
        )
        prompt = prompts.build_websearch_prompt(
            jobs_for_prompt,
            verse_meta=verse_meta,
            registry_context=ws_registry_context,
            tool_name=WEBSEARCH_TOOL_NAME,
            search_tool=WEBSEARCH_SEARCH_TOOL,
            fetch_tool=WEBSEARCH_FETCH_TOOL,
            context_prefix=WEBSEARCH_CONTEXT_PREFIX
        )
        if PROMPT_COMPACT_MODE == "auto" and len(str(prompt)) > PROMPT_COMPACT_THRESHOLD:
            jobs_for_prompt = _compact_websearch_jobs(
                jobs,
                drop_context_window=True,
                drop_parallel_links=True
            )
            ws_registry_context = None
            prompt = prompts.build_websearch_prompt(
                jobs_for_prompt,
                verse_meta=verse_meta,
                registry_context=ws_registry_context,
                tool_name=WEBSEARCH_TOOL_NAME,
                search_tool=WEBSEARCH_SEARCH_TOOL,
                fetch_tool=WEBSEARCH_FETCH_TOOL,
                context_prefix=WEBSEARCH_CONTEXT_PREFIX
            )

    elif stage == 'asset_cards':
        entities_context = json.dumps(verse_obj.get("analysis_entities", {}), ensure_ascii=False)
        prompt = prompts.get_prompt_asset_card(text, entities_context)

    # Convert prompt to string to ensure length check works
    prompt_str = str(prompt)
    if not prompt_str.strip():
        print(f"⚠️ Empty prompt for stage {stage}. Skipping.")
        return verse_obj

    sys_msg = "You are a specialized linguistic analyzer. Follow the custom format strictly."
    dynamic_max_tokens = _compute_dynamic_max_tokens(prompt_str)
    
    # Determine Token Cap based on Mode
    is_review_mode = (
        (stage == 'graphematic' and GRAPHEMATIC_MODE == 'llm') or
        (stage == 'morphologic' and MORPHOLOGIC_MODE == 'llm') or
        (stage == 'syntactic' and SYNTACTIC_MODE == 'llm')
    )
    
    cap_key = stage
    if is_review_mode:
        cap_key = "review"
    elif stage == 'translation' and TRANSLATION_MODE != 'json':
        cap_key = "translation_draft"
        
    stage_cap = STAGE_MAX_OUTPUT.get(cap_key)
    
    if stage_cap:
        dynamic_max_tokens = min(dynamic_max_tokens, int(stage_cap))
    
    use_stateful = previous_response_id is not None

    if DRY_RUN:
        dry_input = prompt_str if use_stateful else f"{sys_msg}\n\n{prompt_str}"
        est_input = _estimate_input_tokens(dry_input)
        record = {
            "stage": stage,
            "verse_id": verse_obj.get("verse_id"),
            "use_stateful": use_stateful,
            "previous_response_id": previous_response_id,
            "model_hint": previous_model,
            "prompt_chars": len(dry_input),
            "prompt": dry_input,
            "max_output_tokens": int(dynamic_max_tokens),
            "estimated_input_tokens": est_input,
            "projected_total_tokens": int(est_input + dynamic_max_tokens),
            "context_window_assumption": CONTEXT_WINDOW_ASSUMPTION
        }
        await _log_dry_run(record)
        return verse_obj
    
    # Send to LLM
    for attempt in range(MAX_RETRIES):
        # Dynamic model selection: Pin to the stateful model when chaining
        if use_stateful and previous_model:
            current_model = previous_model
        elif use_stateful and len(MODELS) == 1:
            current_model = MODELS[0]
        else:
            current_model = random.choice(MODELS)
        
        sem = model_semaphores.get(current_model)
        if sem is None:
            sem = asyncio.Semaphore(MAX_CONCURRENT_PER_MODEL)
            model_semaphores[current_model] = sem
        
        # Stateful API Payload Construction
        stream_enabled = STREAM_LM

        if use_stateful and previous_response_id:
             # Continued Conversation
             payload = {
                "model": current_model,
                "input": prompt_str, 
                "previous_response_id": previous_response_id,
                "temperature": 0.2,
                "stream": stream_enabled,
                "max_output_tokens": int(dynamic_max_tokens)
            }
        else:
            # New Conversation
            # The /api/v1/chat endpoint uses 'input' for the message content.
            # Combining System Message + User Prompt into one block.
            full_input = f"{sys_msg}\n\n{prompt_str}"
            
            payload = {
                "model": current_model,
                "input": full_input,
                "temperature": 0.2,
                "stream": stream_enabled,
                "max_output_tokens": int(dynamic_max_tokens)
            }

        if stage == "websearch" and WEBSEARCH_USE_TOOLS:
            use_mcp = bool(WEBSEARCH_MCP_SERVER_ID or WEBSEARCH_MCP_SERVER_URL)
            if use_mcp:
                if WEBSEARCH_MCP_MODE == "ephemeral" and WEBSEARCH_MCP_SERVER_URL:
                    integration = {
                        "type": "ephemeral_mcp",
                        "server_label": WEBSEARCH_MCP_SERVER_ID or WEBSEARCH_TOOL_NAME,
                        "server_url": WEBSEARCH_MCP_SERVER_URL
                    }
                    if WEBSEARCH_MCP_ALLOWED_TOOLS:
                        integration["allowed_tools"] = WEBSEARCH_MCP_ALLOWED_TOOLS
                    payload["integrations"] = [integration]
                else:
                    mcp_id = WEBSEARCH_MCP_SERVER_ID or WEBSEARCH_TOOL_NAME
                    if "/" in mcp_id:
                        payload["integrations"] = [mcp_id]
                    else:
                        payload["integrations"] = [f"mcp/{mcp_id}"]
            else:
                payload["rawTools"] = {
                    "type": "toolArray",
                    "tools": _websearch_tool_defs(),
                    "force": False
                }
    
        try:
            # Added timeout to automate the "eject" process for hanging requests
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            
            # Acquire semaphore for the specific model
            async with sem:
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] 🚀 SEND {verse_obj['verse_id']} -> [{current_model}]")
                async with session.post(LM_STUDIO_URL, json=payload, timeout=timeout, headers=API_HEADERS) as response:
                    if response.status == 200:
                        await touch_heartbeat() # ALIVE SIGNAL
                        if stream_enabled:
                            content, response_id, result_obj = await _read_sse_response(response)
                            result = result_obj or {
                                "response_id": response_id,
                                "output": [{"type": "message", "content": content}]
                            }
                        else:
                            result = await response.json()
                        
                        # Handle Output Structure (Stateful API might differ slightly)
                        # User snippet:
                        # { "output": [ { "type": "message", "content": "..." } ], "response_id": "..." }
                        
                        content = ""
                        response_id = result.get("response_id")
                        
                        if "output" in result and isinstance(result["output"], list):
                            # New Stateful Format
                            for item in result["output"]:
                                if isinstance(item, dict):
                                    item_type = item.get("type")
                                    if item_type and item_type not in ("message", "text"):
                                        continue
                                    # Prefer explicit content/text fields regardless of type
                                    if "content" in item and item.get("content") is not None:
                                        content += str(item.get("content", ""))
                                    elif "text" in item and item.get("text") is not None:
                                        content += str(item.get("text", ""))
                                elif isinstance(item, str):
                                    content += item
                        elif "choices" in result:
                            # Standard OpenAI Format (Fallback)
                            content = result['choices'][0]['message']['content']
                        
                        cleaned = content.replace("```json", "").replace("```", "").strip()
                        
                        try:
                            parsed_data = None
                            
                            # REVIEW MODES (Text Dump)
                            # If we are in LLM mode for stages that are normally python/deterministic,
                            # we treat the LLM output as a text review/commentary.
                            is_review_mode = (
                                (stage == 'graphematic' and GRAPHEMATIC_MODE == 'llm') or
                                (stage == 'morphologic' and MORPHOLOGIC_MODE == 'llm') or
                                (stage == 'syntactic' and SYNTACTIC_MODE == 'llm') or
                                (stage == 'semantic') or # Semantic is always text essay now
                                (stage == 'translation' and TRANSLATION_MODE != 'json') or # Translation Draft
                                (stage == 'websearch')  # Websearch returns plain text
                            )

                            if is_review_mode:
                                parsed_data = cleaned
                            
                            # STANDARD JSON MODES
                            elif stage in ['graphematic', 'morphologic', 'syntactic', 'translation', 'entities', 'websearch', 'asset_cards']:
                                if stage == 'morphologic' and MORPHOLOGIC_MODE == "text":
                                    parsed_data = parse_morph_text_response(content, verse_obj.get("words", []))
                                else:
                                    try:
                                        parsed_data = json.loads(cleaned)
                                    except json.JSONDecodeError:
                                        # Try to extract a JSON block from the output
                                        candidate = _extract_json_block(cleaned) or _extract_json_block(content)
                                        if candidate:
                                            parsed_data = json.loads(candidate)
                                        else:
                                            # Attempt fix for common small model JSON issues
                                            fixed = fix_malformed_json(cleaned or content)
                                            parsed_data = json.loads(fixed)
                                        
                                    # Optional: Validate specific keys if needed
                                    if stage == 'graphematic':
                                        if 'graphematic_analysis' in parsed_data:
                                            parsed_data = parsed_data['graphematic_analysis']
                                        # Minimal validation
                                        if 'graphematic_string' not in parsed_data:
                                            raise ValueError("Missing 'graphematic_string'")

                                    if stage == 'morphologic' and 'tokens' not in parsed_data:
                                        raise ValueError("Missing 'tokens' in morphologic JSON")
                                    if stage == 'syntactic' and 'syntax' not in parsed_data:
                                        raise ValueError("Missing 'syntax' in syntactic JSON")
                                    if stage == 'websearch':
                                        parsed_data = _normalize_websearch_output(parsed_data, websearch_input_jobs)

                            else:
                                # Fallback
                                parsed_data = json.loads(cleaned)
                                
                            verse_obj[result_key] = parsed_data
                            
                            # SAVE STATE ID
                            if response_id:
                                if "state_ids" not in verse_obj:
                                    verse_obj["state_ids"] = {}
                                verse_obj["state_ids"][stage] = {
                                    "id": response_id,
                                    "model": current_model
                                }
                                ts = datetime.datetime.now().strftime("%H:%M:%S")
                                print(f"[{ts}] 💾 State Saved: {stage} -> {response_id}")
                            
                            ts = datetime.datetime.now().strftime("%H:%M:%S")
                            print(f"[{ts}] ✅ {stage.upper()} {verse_obj['verse_id']} [{current_model}]")
                            return verse_obj
                            
                        except Exception as e:
                            print(f"⚠️ Parse Error {stage} {verse_obj['verse_id']}: {e}")
                            # For now, we return without saving invalid data, effectively skipping
                            # But we could implement fallback logic here
                            
                            _ensure_log_dir()
                            with open(ERROR_LOG_PATH, "a", encoding="utf-8") as err_log:
                                err_log.write(f"\n--- ERROR {verse_obj['verse_id']} ---\n{cleaned}\n--------------------------\n")
                                if not cleaned:
                                    try:
                                        err_log.write(f"RAW_RESPONSE:\n{json.dumps(result, ensure_ascii=False)[:2000]}\n")
                                    except Exception:
                                        pass
                    else:
                        error_text = ""
                        try:
                            error_text = await response.text()
                        except Exception:
                            pass
                        
                        if use_stateful and previous_response_id and response.status in [400, 404] and prompt_full:
                            print("⚠️ Stateful id rejected or stale. Falling back to full prompt.")
                            use_stateful = False
                            previous_response_id = None
                            previous_model = None
                            prompt = prompt_full
                            prompt_str = str(prompt)
                            dynamic_max_tokens = _compute_dynamic_max_tokens(prompt_str)
                            continue

                        print(f"❌ HTTP {response.status} [{current_model}]")
                        if error_text:
                            snippet = error_text.strip().replace("\n", " ")
                            if len(snippet) > 200:
                                snippet = snippet[:200] + "…"
                            if snippet:
                                print(f"⚠️ Error body: {snippet}")
                        if response.status == 404:
                            # Model temporarily unavailable (e.g., reload). Short retry to keep throughput.
                            await asyncio.sleep(0.5)
                            continue
                        if response.status == 400:
                            # 400 Context Window Limit? 
                            # If we hit 400 with NO truncation, it means we genuinely exceeded the hard limit of the backend.
                            # We must reduce tokens or truncate as last resort.
                            # Strategy: Reduce output tokens first (giving up on completion length), then simple retry.
                            
                            # Log the incident
                            print(f"⚠️ 400 Encountered. InputLen: {len(prompt_str)}. MaxOutputTokens: {dynamic_max_tokens}")
                            
                            # Reduce max_tokens aggressively
                            new_max = int(dynamic_max_tokens * 0.7)
                            if new_max < 200: 
                                # If we are already low on output tokens, the INPUT is too big.
                                # Should we fall back to truncation? The user hates it, but it might be unavoidable.
                                # For now, let's keep retrying with lower tokens until nearly zero, then fail.
                                pass
                            
                            dynamic_max_tokens = new_max
                            print(f"📉 Reducing max_output_tokens to {dynamic_max_tokens} for retry...")

                        # If 400 or 500, model might be unloaded/broken
                        if response.status in [400, 500]:
                            await asyncio.sleep(5)  # Wait for external manager
                            
        except asyncio.TimeoutError:
             print(f"⌛ Timeout {verse_obj['verse_id']} [{current_model}]")
        except Exception as e:
            print(f"❌ Ex: {e} [{current_model}]")
            await asyncio.sleep(1) # Brief pause
            
        # Minimal sleep for aggressive retry
        await asyncio.sleep(0.1)

    return verse_obj

async def save_progress(data, filepath):
    temp = filepath + ".tmp"

    # 1. Standard Serialize with Indentation
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    
    # 2. Compactify "base_chars": [ { "id": 1, "char": "X" }, ... ]
    # Collapse objects: { \n "id": 1, \n "char": "X" \n } -> { "id": 1, "char": "X" }
    json_str = re.sub(
        r'\{\s*"id":\s*(\d+),\s*"char":\s*"([^"]+)"\s*\}',
        r'{ "id": \1, "char": "\2" }',
        json_str,
        flags=re.DOTALL
    )

    # 3. Compactify "words": [ { "word_id": 1, "text": "...", "char_ids": [ ... ] }, ... ]
    # A. Flatten simple integer lists (char_ids): [ \n 1, \n 2 \n ] -> [ 1, 2 ]
    json_str = re.sub(
        r'\[\s*((?:\d+(?:,\s*)?)+)\s*\]',
        lambda m: "[" + re.sub(r'\s+', ' ', m.group(1)).strip() + "]",
        json_str,
        flags=re.DOTALL
    )
    
    # B. Flatten the word objects
    # Matches: { \n "word_id": 1, \n "text": "...", \n "char_ids": [ ... ] \n }
    json_str = re.sub(
        r'\{\s*"word_id":\s*(\d+),\s*"text":\s*"([^"]*)",\s*"char_ids":\s*(\[[^\]]*\])\s*\}',
        r'{ "word_id": \1, "text": "\2", "char_ids": \3 }',
        json_str,
        flags=re.DOTALL
    )

    with open(temp, 'w', encoding='utf-8') as f:
        f.write(json_str)

    # Retry loop for Windows file locking issues
    for attempt in range(5):
        try:
            os.replace(temp, filepath)
            print("💾 Saved (Compact Format).")
            return
        except PermissionError:
            print(f"⚠️ File locked, retrying save in 1s ({attempt+1}/5)...")
            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"❌ Save failed: {e}")
            return
            
    print("❌ Critical: Could not save file after 5 attempts.")

async def main():
    global DRY_RUN_OUT, DRY_RUN_LIMIT, REGISTRY_CACHE
    if not os.path.exists(DATA_FILE):
        return
    
    # NEW STARTUP SEQUENCE
    await ensure_startup_state()

    # CHECKPOINTING / BACKUP
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{DATA_FILE}.{timestamp}.bak"
    try:
        shutil.copy(DATA_FILE, backup_file)
        _log(f"📦 Created Checkpoint: {backup_file}")
    except Exception as e:
        print(f"⚠️ Backup failed: {e}")

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if CURRENT_STAGE == "entities":
        subjects_dir = _subjects_dir()
        if not os.path.isdir(subjects_dir):
            os.makedirs(subjects_dir, exist_ok=True)

        if BUILD_REGISTRY or not os.path.exists(REGISTRY_FILE):
            registry = _build_registry_from_data(data)
            _save_registry(registry)
            REGISTRY_CACHE = registry
            print(f"📚 Registry rebuilt (internal): {REGISTRY_FILE} | assets={len(registry.get('assets', {}))}")
            public_registry = _build_public_registry(data, registry)
            _save_public_registry(public_registry)
            if REGISTRY_PUBLIC_FILE:
                print(f"📘 Registry written (public): {REGISTRY_PUBLIC_FILE} | subjects={len(public_registry)}")
        else:
            registry = _load_registry()
            assets = registry.get("assets", {}) if isinstance(registry, dict) else {}
            if (not assets) and _data_has_assets_or_aliases(data):
                registry = _build_registry_from_data(data)
                _save_registry(registry)
                REGISTRY_CACHE = registry
                print(f"📚 Registry rebuilt (internal): {REGISTRY_FILE} | assets={len(registry.get('assets', {}))}")
                public_registry = _build_public_registry(data, registry)
                _save_public_registry(public_registry)
                if REGISTRY_PUBLIC_FILE:
                    print(f"📘 Registry written (public): {REGISTRY_PUBLIC_FILE} | subjects={len(public_registry)}")
    elif CURRENT_STAGE == "websearch":
        registry = _load_registry()
        if not registry:
            registry = _build_registry_from_data(data)
        global VERSE_CONTEXT_MAP
        VERSE_CONTEXT_MAP = _build_websearch_context_map(data, registry, max(0, WEBSEARCH_CONTEXT_WINDOW))
        global PARALLEL_LINKS_MAP
        links_path = WEBSEARCH_PARALLEL_LINKS_FILE
        if links_path and not os.path.isabs(links_path):
            links_path = os.path.join(os.path.dirname(__file__), "..", "..", links_path)
        PARALLEL_LINKS_MAP = _load_parallel_links(links_path)

    # Filter: Process only items that HAVE the previous stage but MISSING the current stage
    # (Chain dependency check)
    to_process = []
    
    # Dependency Logic
    required_prev = {
        'graphematic': None,
        'morphologic': None,
        'translation': 'analysis_syntactic',
        'syntactic': 'analysis_morphologic',
        'semantic': 'analysis_syntactic',
        'entities': 'analysis_semantic',
        'websearch': 'analysis_entities',
        'asset_cards': 'analysis_entities'
    }
    
    prev_key = required_prev.get(CURRENT_STAGE)
    target_key = f"analysis_{CURRENT_STAGE}"

    for v in data:
        # Check if previous stage exists (if there is a dependency)
        has_prev = True
        if prev_key and not v.get(prev_key):
             # For morphologic, we proceed even if graphematic had errors, 
             # as long as we have text. But strictly, we should have it.
             # Let's be lenient for this test:
             if 'error' in str(v.get(prev_key, "")):
                 has_prev = False 
             elif v.get(prev_key) is None:
                 has_prev = False

        if has_prev and _is_stage_pending(v, CURRENT_STAGE):
            to_process.append(v)
        elif DRY_RUN and _is_stage_pending(v, CURRENT_STAGE):
            to_process.append(v)

    # Optional cap for fast debug runs
    if MAX_ITEMS and MAX_ITEMS > 0:
        to_process = to_process[:MAX_ITEMS]

    if DRY_RUN:
        if DRY_RUN_LIMIT is None and (not MAX_ITEMS or MAX_ITEMS <= 0):
            DRY_RUN_LIMIT = 3
        if DRY_RUN_LIMIT:
            to_process = to_process[:DRY_RUN_LIMIT]
        if not DRY_RUN_OUT:
            _ensure_log_dir()
            DRY_RUN_OUT = os.path.join(LOG_DIR, f"dryrun_{CURRENT_STAGE}.jsonl")

    
    _log(f"🚀 Starting STAGE: {CURRENT_STAGE}")
    _log(f"🎯 Targets: {len(to_process)} verses")
    
    # Local-only graphematic: skip LM Studio entirely
    if CURRENT_STAGE == "graphematic" and GRAPHEMATIC_MODE == "local":
        async def bound_analyze_local(verse):
            return await analyze_stage(None, verse, CURRENT_STAGE)

        tasks = [bound_analyze_local(v) for v in to_process]
        chunk_size = 50
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            await asyncio.gather(*chunk)
            await save_progress(data, DATA_FILE)
        print("🏁 Stage Complete!")
        return

    # Local-only entities: skip LM Studio entirely
    if CURRENT_STAGE == "entities":
        async def bound_analyze_local(verse):
            return await analyze_stage(None, verse, CURRENT_STAGE)

        tasks = [bound_analyze_local(v) for v in to_process]
        chunk_size = 50
        processed = 0
        totals = {"entities": 0, "alias_hits": 0, "state_triggers": 0, "state_updates": 0}
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            results = await asyncio.gather(*chunk)
            processed += len(results)
            for r in results:
                stats = _entities_stats(r)
                for k in totals:
                    totals[k] += stats.get(k, 0)
            await save_progress(data, DATA_FILE)
            print(
                f"📊 Entities Progress: {processed}/{len(to_process)} | "
                f"entities={totals['entities']} alias_hits={totals['alias_hits']} "
                f"state_triggers={totals['state_triggers']} state_updates={totals['state_updates']}"
            )

        subjects_dir = _subjects_dir()
        registry = _load_registry()
        if BUILD_OCCURRENCES:
            occ_path = os.path.join(subjects_dir, "occurrences.jsonl")
            rows = _build_occurrences_from_data(data, registry)
            _write_jsonl(occ_path, rows)
            print(f"🧾 Wrote occurrences: {occ_path} | rows={len(rows)}")
        if BUILD_ASSET_BIBLE:
            bible_path = os.path.join(subjects_dir, "asset_bible.json")
            bible = _build_asset_bible(data, registry)
            with open(bible_path, "w", encoding="utf-8") as f:
                json.dump(bible, f, ensure_ascii=False, indent=2)
            print(f"📘 Wrote asset_bible: {bible_path} | subjects={len(bible.get('subjects', []))}")
        print("🏁 Stage Complete!")
        return

    # Initialize per-model semaphores
    global model_semaphores
    model_semaphores = {m: asyncio.Semaphore(MAX_CONCURRENT_PER_MODEL) for m in MODELS}

    async with aiohttp.ClientSession() as session:
        # Increase the global semaphore significantly because we now throttle per-model.
        # If we have 2 models * 6 concurrent = 12 total capacity.
        # Let's set global sem to 20 to be safe and let the model_semaphores handle the real limits.
        sem = asyncio.Semaphore(20) 
        
        async def bound_analyze(verse):
            async with sem:
                try:
                    return await analyze_stage(session, verse, CURRENT_STAGE)
                except Exception as e:
                    vid = verse.get("verse_id") if isinstance(verse, dict) else None
                    _log(f"⚠️ {CURRENT_STAGE} failed for {vid or 'unknown'}: {e}")
                    return verse

        tasks = [bound_analyze(v) for v in to_process]
        
        chunk_size = 50
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            await asyncio.gather(*chunk)
            await save_progress(data, DATA_FILE)

    print("🏁 Stage Complete!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    import argparse
    parser = argparse.ArgumentParser(description="Run linguistic analysis stages.")
    parser.add_argument("stage", nargs="?", default=None, help="The stage to run (graphematic, morphologic, syntactic, semantic, translation, entities, websearch, asset_cards)")
    parser.add_argument("--mode", choices=["local", "llm", "heuristic", "json", "text", "fetch"], help="Override processing mode for the current stage")
    parser.add_argument("--dry-run", action="store_true", help="Simulate prompt generation without calling LLM")
    parser.add_argument("--dry-run-limit", type=int, default=3, help="Number of items for dry run")
    parser.add_argument("--dry-run-out", help="Output file for dry run logs")
    parser.add_argument("--stream", action="store_true", help="Enable streaming for LLM requests (stateful /api/v1/chat)")
    parser.add_argument("--force", action="store_true", help="Force re-run even if stage is already complete")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of items to process")
    parser.add_argument("--data-file", help="Override story_data.json path for this run")
    parser.add_argument("--registry-file", help="Override registry.json path for this run")
    parser.add_argument("--subjects-dir", help="Output directory for registry/occurrences/asset_bible (entities stage)")
    parser.add_argument("--build-registry", action="store_true", help="Rebuild registry.json from story_data (entities stage)")
    parser.add_argument("--build-occurrences", action="store_true", help="Write occurrences.jsonl (entities stage)")
    parser.add_argument("--build-asset-bible", action="store_true", help="Write asset_bible.json (entities stage)")
    parser.add_argument("--story-id", help="Override story_id for asset_bible.json (entities stage)")
    parser.add_argument("--timeline-id", help="Override timeline_id for asset_bible.json (entities stage)")
    parser.add_argument("--phase-count", type=int, help="Phase count for dynamic subjects (entities stage)")
    parser.add_argument("--phase-labels", help="Comma-separated phase labels (entities stage)")

    args = parser.parse_args()

    # 0. Data/Registry file overrides
    if args.data_file:
        DATA_FILE = args.data_file
    if args.registry_file:
        REGISTRY_FILE = args.registry_file
        REGISTRY_CACHE = None
        # If user points to registry.json, treat it as public and use registry_internal.json for pipeline.
        if os.path.basename(REGISTRY_FILE).lower() == "registry.json":
            REGISTRY_PUBLIC_FILE = REGISTRY_FILE
            REGISTRY_FILE = os.path.join(os.path.dirname(REGISTRY_PUBLIC_FILE), "registry_internal.json")
            REGISTRY_CACHE = None
    if args.subjects_dir:
        SUBJECTS_DIR = args.subjects_dir
        if not args.registry_file:
            REGISTRY_PUBLIC_FILE = os.path.join(SUBJECTS_DIR, "registry.json")
            REGISTRY_FILE = os.path.join(SUBJECTS_DIR, "registry_internal.json")
            REGISTRY_CACHE = None
        elif REGISTRY_PUBLIC_FILE is None:
            REGISTRY_PUBLIC_FILE = os.path.join(SUBJECTS_DIR, "registry.json")

    if args.story_id:
        STORY_ID = args.story_id
    if args.timeline_id:
        TIMELINE_ID = args.timeline_id
    if args.phase_count is not None:
        PHASE_COUNT = args.phase_count
    if args.phase_labels:
        PHASE_LABELS = [p.strip() for p in args.phase_labels.split(",") if p.strip()]

    if args.build_registry:
        BUILD_REGISTRY = True
    if args.build_occurrences:
        BUILD_OCCURRENCES = True
    if args.build_asset_bible:
        BUILD_ASSET_BIBLE = True

    if CURRENT_STAGE == "entities" and not (BUILD_REGISTRY or BUILD_OCCURRENCES or BUILD_ASSET_BIBLE):
        # Default behavior: entities rebuilds registry unless explicitly disabled.
        BUILD_REGISTRY = True

    # 1. Stage Override
    if args.stage:
        CURRENT_STAGE = args.stage
    
    # 2. Mode Override
    if args.mode:
        if CURRENT_STAGE == 'graphematic':
            GRAPHEMATIC_MODE = args.mode
        elif CURRENT_STAGE == 'morphologic':
            MORPHOLOGIC_MODE = args.mode
        elif CURRENT_STAGE == 'syntactic':
            SYNTACTIC_MODE = args.mode # e.g. 'heuristic' vs 'llm'
        elif CURRENT_STAGE == 'translation':
            TRANSLATION_MODE = args.mode # 'text' (draft) or 'json' (final)
        elif CURRENT_STAGE == 'websearch':
            if args.mode in ("local", "llm", "fetch"):
                WEBSEARCH_MODE = args.mode
            else:
                print("⚠️ Websearch mode supports only local|llm|fetch. Using config default.")
    
    # 3. Dry Run / Limits
    if args.dry_run:
        DRY_RUN = True
    if args.dry_run_limit:
        DRY_RUN_LIMIT = args.dry_run_limit
    if args.dry_run_out:
        DRY_RUN_OUT = args.dry_run_out
    if args.stream:
        STREAM_LM = True
    if args.limit:
        MAX_ITEMS = args.limit
    if args.force:
        FORCE_STAGE = True

    _log(f"🔧 CONFIG: Stage={CURRENT_STAGE}, Mode={args.mode if args.mode else 'Config Default'}")
    try:
        _log(f"📂 DATA_FILE: {os.path.abspath(DATA_FILE)}")
    except Exception:
        _log(f"📂 DATA_FILE: {DATA_FILE}")
    try:
        _log(f"📚 REGISTRY_FILE: {os.path.abspath(REGISTRY_FILE)}")
    except Exception:
        _log(f"📚 REGISTRY_FILE: {REGISTRY_FILE}")
    
    asyncio.run(main())
