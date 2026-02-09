import json
import os
import re

try:
    from .fidel_ops import build_pre_processing, normalize_root_key, normalize_geez_to_root_key
except ImportError:
    from fidel_ops import build_pre_processing, normalize_root_key, normalize_geez_to_root_key

# CONFIG (Loaded from ../config/config.json)
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
ALIASES_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "aliases.json")
ALIASES_DE_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "aliases_de.json")
REGISTRY_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "stories", "template", "subjects")
REGISTRY_FILE = os.path.join(REGISTRY_DIR, "registry.json")

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

config = load_config()

# Paths relative to this script
DATA_FILE = os.path.join(os.path.dirname(__file__), config["files"]["data_file"])
INPUT_FILE = os.path.join(os.path.dirname(__file__), config["files"]["input_file"])
TRANSLATION_FILE = os.path.join(os.path.dirname(__file__), config["files"].get("translation_file", "")) if isinstance(config.get("files"), dict) else ""

SEPARATOR = config["processing"]["separator"]
ADDITIONAL_SEPARATORS = config["processing"]["additional_separators"]
GRAPHEMATIC_PREFILL = config["processing"].get("graphematic_prefill", True)
GRAPHEMATIC_PUNCTUATIONS = config["processing"].get("graphematic_punctuations", [])
DE_ENTITIES_CFG = config.get("de_entities", {})

STATE_TRIGGERS = {
    "MSN": "CORRUPTED"
}
STATEFUL_TAG_PREFIXES = ("ACTOR", "CLASS", "ENTITY")

# -----------------------------------------------------------------------------

def clean_text(text):
    """
    Entfernt unsichtbare Steuerzeichen (\u200b, BOM, Joiner).
    Normale Leerzeichen bleiben erhalten!
    """
    if not text: return ""
    # Zero Width Space (\u200b), BOM (\ufeff), Word Joiner (\u2060)
    cleaned = text.replace('\u200b', '').replace('\ufeff', '').replace('\u2060', '')
    return cleaned.strip()

def parse_input_file(filepath):
    """
    Parses the complete_story.txt file.
    Expected format:
    Chapter X
    X:Y Text...
    """
    print(f"📖 Reading {filepath}...")
    if not os.path.exists(filepath):
        print(f"❌ Input-Datei nicht gefunden: {filepath}")
        return []

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    entries = []
    current_chapter = 0
    
    # Regex for "1:1 Text..." or "10:5 Text..."
    verse_pattern = re.compile(r"^(\d+):(\d+)\s+(.*)$")
    
    lines = content.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Check for Chapter header
        if line.lower().startswith("chapter"):
            # "Chapter 1"
            parts = line.split()
            if len(parts) > 1 and parts[1].isdigit():
                current_chapter = int(parts[1])
            continue
            
        # Check for Verse
        match = verse_pattern.match(line)
        if match:
            c_str, v_str, text_raw = match.groups()
            c_int = int(c_str)
            v_int = int(v_str)

            # CLEAN TEXT HERE
            text_clean = clean_text(text_raw)

            entries.append({
                "verse_id": f"{c_int}:{v_int}",
                "chapter": c_int,
                "verse": v_int,
                "text": text_clean
            })
            
    print(f"✅ Parsed {len(entries)} verses.")
    return entries

def generate_ids(text, separator):
    """
    Erzeugt atomare IDs (base_chars) und Wort-Gruppierungen (words).
    """
    if not text:
        return [], []

    base_chars = []
    words = []
    
    current_word_char_ids = []
    current_word_text = []
    
    global_char_id = 1
    word_id = 1
    
    all_separators = set(ADDITIONAL_SEPARATORS)
    all_separators.add(separator)
    all_separators.add("።") 

    for char in text:
        # 1. Jedes Zeichen bekommt eine ID -> base_chars
        base_chars.append({"id": global_char_id, "char": char})
        
        # 2. Wort-Logik
        is_sep = char in all_separators
        
        if is_sep:
            if current_word_char_ids:
                word_text_str = "".join(current_word_text)
                words.append({
                    "word_id": word_id,
                    "text": word_text_str,
                    "char_ids": list(current_word_char_ids)
                })
                word_id += 1
                
                current_word_char_ids = []
                current_word_text = []
        else:
            current_word_char_ids.append(global_char_id)
            current_word_text.append(char)
        
        global_char_id += 1
        
    if current_word_char_ids:
        word_text_str = "".join(current_word_text)
        words.append({
            "word_id": word_id,
            "text": word_text_str,
            "char_ids": list(current_word_char_ids)
        })
        
    return base_chars, words

def _get_punct_set():
    if GRAPHEMATIC_PUNCTUATIONS:
        return set(GRAPHEMATIC_PUNCTUATIONS)
    return {"፡", "።", "፣", "፤", "፥", "፦", "፧", "፨", ".", ",", "!", "?", ";", ":"}

def build_graphematic_analysis(text: str, words: list):
    graphematic_string = text or ""
    punct_set = _get_punct_set()
    punctuation_markers = []
    punctuation_index0 = []
    punctuation_index = []

    for idx, ch in enumerate(graphematic_string):
        if ch in punct_set:
            punctuation_markers.append(ch)
            punctuation_index0.append(idx)
            punctuation_index.append(idx + 1)  # 1-based to align with char_ids

    # Build word ranges for linking
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

def _detect_language(input_path: str | None, data_path: str | None, lang_arg: str | None):
    if lang_arg:
        return lang_arg.strip().lower()
    for p in [input_path, data_path]:
        if not p:
            continue
        name = os.path.basename(p).lower()
        if "_de" in name or name.endswith("de.txt") or name.endswith("de.json"):
            return "de"
    return "gez"

def _resolve_alias_files(lang: str | None, extra_files: list[str] | None):
    files = []
    if os.path.exists(ALIASES_FILE):
        files.append(ALIASES_FILE)
    if lang == "de" and os.path.exists(ALIASES_DE_FILE):
        files.append(ALIASES_DE_FILE)
    for f in extra_files or []:
        if f and os.path.exists(f):
            files.append(f)
    # de-dup
    seen = set()
    out = []
    for f in files:
        if f in seen:
            continue
        seen.add(f)
        out.append(f)
    return out

def load_aliases(alias_files: list[str] | None = None):
    if not alias_files:
        return []
    merged = {}
    for path in alias_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            continue
        aliases = raw.get("aliases") if isinstance(raw, dict) else raw
        if not isinstance(aliases, list):
            continue
        for a in aliases:
            aid = a.get("id")
            if not aid:
                continue
            entry = merged.get(aid, {
                "id": aid,
                "labels": [],
                "patterns": [],
                "mode": a.get("mode")
            })
            entry["labels"] = list({*entry.get("labels", []), *a.get("labels", [])})
            for p in a.get("patterns", []):
                if isinstance(p, list) and p not in entry["patterns"]:
                    entry["patterns"].append(p)
            if a.get("mode"):
                entry["mode"] = a.get("mode")
            merged[aid] = entry

    normalized = []
    for a in merged.values():
        patterns = a.get("patterns", [])
        norm_patterns = []
        for p in patterns:
            if not isinstance(p, list):
                continue
            norm_patterns.append([normalize_root_key(x) for x in p])
        normalized.append({
            "id": a.get("id"),
            "labels": a.get("labels", []),
            "patterns": patterns,
            "patterns_norm": norm_patterns,
            "mode": a.get("mode")
        })
    return normalized

def compute_verse_metrics(words):
    total = len(words)
    if total == 0:
        return {"avg_word_len": 0, "sadis_ratio": 0.0, "pacing_multiplier": 1.0}
    lengths = [len(w.get("text", "")) for w in words]
    avg_len = sum(lengths) / max(1, total)
    sadis_count = 0
    for w in words:
        pp = w.get("pre_processing", {})
        if pp.get("grammatical_vowel") == 6:
            sadis_count += 1
    sadis_ratio = sadis_count / total
    length_factor = 1.0
    if avg_len <= 3:
        length_factor += 0.2
    elif avg_len >= 6:
        length_factor -= 0.2
    pacing = (1.0 + (sadis_ratio * 0.5)) * length_factor
    pacing = max(0.6, min(1.6, pacing))
    return {
        "avg_word_len": round(avg_len, 3),
        "sadis_ratio": round(sadis_ratio, 3),
        "pacing_multiplier": round(pacing, 3)
    }

def _normalize_alias_id(token: str) -> str:
    if not token:
        return ""
    trans = str.maketrans({
        "Ä": "AE", "Ö": "OE", "Ü": "UE",
        "ä": "AE", "ö": "OE", "ü": "UE",
        "ß": "SS"
    })
    t = token.translate(trans)
    t = re.sub(r"[^A-Za-z0-9]+", "", t).upper()
    return t

def _normalize_label_key(text: str) -> str:
    if not text:
        return ""
    trans = str.maketrans({
        "Ä": "AE", "Ö": "OE", "Ü": "UE",
        "ä": "AE", "ö": "OE", "ü": "UE",
        "ß": "SS"
    })
    t = text.translate(trans)
    t = re.sub(r"[^\w]+", "", t, flags=re.UNICODE)
    return t.casefold()

def _de_candidate_bases(token: str) -> list[str]:
    token = token or ""
    bases = {token}
    # Common German suffixes (genitive/plural/inflection)
    suffixes = ["s", "es", "n", "en", "ern", "er", "e"]
    for suf in suffixes:
        if token.lower().endswith(suf) and len(token) > len(suf) + 1:
            bases.add(token[:-len(suf)])
    return list(bases)

def _build_alias_label_index(aliases: list) -> dict:
    idx = {}
    for a in aliases or []:
        aid = a.get("id")
        if not aid:
            continue
        for lab in a.get("labels", []) or []:
            key = _normalize_label_key(lab)
            if not key:
                continue
            idx.setdefault(key, aid)
    return idx

def _is_capitalized_token(token: str) -> bool:
    if not token:
        return False
    return token[0].isupper()

def _clean_surface_token(token: str) -> str:
    if not token:
        return ""
    return re.sub(r"^[^\w]+|[^\w]+$", "", token, flags=re.UNICODE)

def compute_capitalized_counts(data: list, cfg: dict) -> dict:
    counts = {}
    if not cfg or not cfg.get("enable_capitalized_heuristic"):
        return counts
    min_len = int(cfg.get("min_length", 3) or 3)
    stopwords = set([w.casefold() for w in cfg.get("stopwords", [])])
    for verse in data or []:
        words = verse.get("words", []) or []
        for w in words:
            token = _clean_surface_token(w.get("text", ""))
            if not token:
                continue
            if len(token) < min_len:
                continue
            if token.casefold() in stopwords:
                continue
            if not _is_capitalized_token(token):
                continue
            key = token.casefold()
            counts[key] = counts.get(key, 0) + 1
    return counts

def find_capitalized_hits(words, cfg: dict, counts: dict, alias_label_index: dict | None = None) -> list:
    hits = []
    if not cfg or not cfg.get("enable_capitalized_heuristic"):
        return hits
    min_len = int(cfg.get("min_length", 3) or 3)
    min_freq = int(cfg.get("min_frequency", 2) or 2)
    exclude_start = bool(cfg.get("exclude_sentence_start", True))
    stopwords = set([w.casefold() for w in cfg.get("stopwords", [])])
    for idx, w in enumerate(words):
        token_raw = _clean_surface_token(w.get("text", ""))
        if not token_raw:
            continue
        if exclude_start and idx == 0:
            continue
        if len(token_raw) < min_len:
            continue
        if token_raw.casefold() in stopwords:
            continue
        if not _is_capitalized_token(token_raw):
            continue
        if counts.get(token_raw.casefold(), 0) < min_freq:
            continue
        alias_id = None
        if alias_label_index:
            for base in _de_candidate_bases(token_raw):
                key = _normalize_label_key(base)
                if key in alias_label_index:
                    alias_id = alias_label_index[key]
                    break
        if not alias_id:
            alias_key = _normalize_alias_id(token_raw)
            if not alias_key:
                continue
            alias_id = f"DE_ENTITY_{alias_key}"
        hits.append({
            "alias_id": alias_id,
            "alias_label": token_raw,
            "pattern": [token_raw],
            "word_ids": [w.get("word_id")]
        })
    return hits

def find_alias_hits(words, aliases, language: str | None = None, cap_counts: dict | None = None, cap_cfg: dict | None = None):
    hits = []
    if not aliases:
        aliases = []
    alias_label_index = _build_alias_label_index(aliases)
    def _contains_geez(text):
        return any(0x1200 <= ord(ch) <= 0x137F for ch in text)

    def _normalize_surface_token(token):
        if not token:
            return ""
        if _contains_geez(token):
            return normalize_geez_to_root_key(token)
        # For non-Ge'ez (e.g., DE), casefold + strip punctuation
        t = token.casefold()
        t = re.sub(r"[^\w]+", "", t, flags=re.UNICODE)
        return t

    def _looks_like_root(token):
        if not token:
            return False
        if _contains_geez(token):
            return False
        if any(ch in token for ch in ["-", "ʾ", "ʿ", "'"]):
            return True
        t = re.sub(r"[^A-Za-z0-9]+", "", token)
        if not t:
            return False
        return len(t) <= 5 and t.upper() == t

    roots = []
    norm_surfaces = []
    for w in words:
        pp = w.get("pre_processing", {})
        roots.append(pp.get("ontology", {}).get("root_key") or "")
        norm_surfaces.append(_normalize_surface_token(w.get("text", "")))
    for alias in aliases:
        alias_mode = (alias.get("mode") or "").strip().lower()
        for pattern in alias.get("patterns", []):
            if not pattern:
                continue
            # Decide whether this pattern is Ge'ez-form or root-key form
            is_geez = any(_contains_geez(part) for part in pattern)
            if alias_mode in ("root", "surface"):
                mode = alias_mode
            elif is_geez:
                mode = "surface"
            elif all(_looks_like_root(p) for p in pattern):
                mode = "root"
            else:
                mode = "surface"

            if mode == "surface":
                norm_pattern = [_normalize_surface_token(p) for p in pattern]
                if len(norm_pattern) > len(norm_surfaces):
                    continue
                for i in range(0, len(norm_surfaces) - len(norm_pattern) + 1):
                    if norm_surfaces[i:i + len(norm_pattern)] == norm_pattern:
                        word_ids = [words[i + j].get("word_id") for j in range(len(norm_pattern))]
                        hits.append({
                            "alias_id": alias.get("id"),
                            "pattern": pattern,
                            "word_ids": word_ids
                        })
            else:
                norm_pattern = [normalize_root_key(p) for p in pattern]
                if len(norm_pattern) > len(roots):
                    continue
                for i in range(0, len(roots) - len(norm_pattern) + 1):
                    if roots[i:i + len(norm_pattern)] == norm_pattern:
                        word_ids = [words[i + j].get("word_id") for j in range(len(norm_pattern))]
                        hits.append({
                            "alias_id": alias.get("id"),
                            "pattern": pattern,
                            "word_ids": word_ids
                        })
    # Capitalized heuristic (DE)
    if language == "de" and cap_cfg:
        hits.extend(find_capitalized_hits(words, cap_cfg, cap_counts or {}, alias_label_index))

    return hits

def register_asset(registry, pp, verse_id):
    asset_id = pp.get("asset_id")
    if not asset_id:
        return
    assets = registry.setdefault("assets", {})
    entry = assets.get(asset_id)
    if not entry:
        entry = {
            "id": asset_id,
            "root": pp.get("root"),
            "root_key": pp.get("ontology", {}).get("root_key"),
            "concept": pp.get("ontology", {}).get("concept"),
            "asset_tag": pp.get("ontology", {}).get("asset_tag"),
            "current_state": "ACTIVE",
            "mentions": 0,
            "first_seen": verse_id
        }
        assets[asset_id] = entry
    entry["mentions"] += 1

def apply_state_triggers(words, registry):
    triggers = []
    updates = []
    assets = registry.get("assets", {})

    trigger_indices = []
    for i, w in enumerate(words):
        rk = w.get("pre_processing", {}).get("ontology", {}).get("root_key")
        if rk in STATE_TRIGGERS:
            trigger_indices.append((i, rk, w.get("word_id")))

    if not trigger_indices:
        return triggers, updates

    updated_assets = set()
    window_after = 4
    window_before = 2

    for idx, rk, word_id in trigger_indices:
        state = STATE_TRIGGERS[rk]
        triggers.append({"root_key": rk, "state": state, "word_id": word_id})

        candidate_indices = list(range(idx + 1, min(len(words), idx + 1 + window_after)))
        candidate_indices += list(range(max(0, idx - window_before), idx))

        updated_this_trigger = False
        for j in candidate_indices:
            pp = words[j].get("pre_processing", {})
            asset_id = pp.get("asset_id")
            asset_tag = pp.get("ontology", {}).get("asset_tag") or ""
            if not asset_id or not asset_tag.startswith(STATEFUL_TAG_PREFIXES):
                continue
            if pp.get("ontology", {}).get("root_key") == rk:
                continue
            if asset_id in updated_assets:
                continue
            if asset_id in assets:
                assets[asset_id]["current_state"] = state
            updates.append({
                "asset_id": asset_id,
                "state": state,
                "trigger_root": rk,
                "trigger_word_id": word_id,
                "target_word_id": words[j].get("word_id")
            })
            updated_assets.add(asset_id)
            updated_this_trigger = True

        if not updated_this_trigger:
            # Fallback: nearest asset anywhere in verse (excluding the trigger itself)
            nearest = None
            nearest_dist = None
            for j, w in enumerate(words):
                pp = w.get("pre_processing", {})
                asset_id = pp.get("asset_id")
                asset_tag = pp.get("ontology", {}).get("asset_tag") or ""
                if not asset_id or not asset_tag.startswith(STATEFUL_TAG_PREFIXES):
                    continue
                if pp.get("ontology", {}).get("root_key") == rk:
                    continue
                dist = abs(j - idx)
                if nearest is None or dist < nearest_dist:
                    nearest = j
                    nearest_dist = dist
            if nearest is not None:
                pp = words[nearest].get("pre_processing", {})
                asset_id = pp.get("asset_id")
                if asset_id and asset_id not in updated_assets:
                    if asset_id in assets:
                        assets[asset_id]["current_state"] = state
                    updates.append({
                        "asset_id": asset_id,
                        "state": state,
                        "trigger_root": rk,
                        "trigger_word_id": word_id,
                        "target_word_id": words[nearest].get("word_id"),
                        "fallback": True
                    })
                    updated_assets.add(asset_id)

    return triggers, updates


# Custom dumper function to achieve:
# "base_chars": [
#   { "id": 1, "char": "ቃ" },
#   ...
# ]
def custom_json_dump(data, filepath):
    # Standard dump first to get valid structure
    raw_json = json.dumps(data, ensure_ascii=False, indent=2)
    
    # Post-process via Regex to collapse small objects
    # Pattern: Look for objects like:
    # {
    #   "id": 1,
    #   "char": "X"
    # }
    # and replace with { "id": 1, "char": "X" }
    
    import re
    
    def collapse_match(match):
        content = match.group(0)
        # Collapse whitespace/newlines within this block
        collapsed = re.sub(r'\s+', ' ', content)
        # Fix spaces around braces
        collapsed = collapsed.replace('{ ', '{').replace(' }', '}')
        collapsed = collapsed.replace('{', '{ ').replace('}', ' }')
        return collapsed

    # Regex for simple objects containing 'id' and 'char' keys (and maybe others)
    # Be careful not to match too much.
    # Matches: {\s*"id": \d+,\s*"char": "[^"]+"\s*} 
    pattern_char = re.compile(r'\{\s*"id":\s*\d+,\s*"char":\s*"[^"]+"\s*\}')
    
    # Matches: words objects with 'word_id', 'text', 'char_ids'
    # {\s*"word_id": \d+,\s*"text": "[^"]+",\s*"char_ids": \[[^\]]+\]\s*}
    pattern_word = re.compile(r'\{\s*"word_id":\s*\d+,\s*"text":\s*"[^"]+",\s*"char_ids":\s*\[[^\]]+\]\s*\}')
    
    # Apply replacements
    # Since re.sub processes the whole string, this is efficient enough for this file size
    better_json = re.sub(pattern_char, collapse_match, raw_json)
    better_json = re.sub(pattern_word, collapse_match, better_json)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(better_json)


def main():
    global DATA_FILE, INPUT_FILE, REGISTRY_FILE, REGISTRY_DIR
    import argparse
    parser = argparse.ArgumentParser(description="Initialize story_data.json from input text.")
    parser.add_argument("--input", dest="input_file", help="Override input text file path.")
    parser.add_argument("--data", dest="data_file", help="Override output data file path.")
    parser.add_argument("--registry", dest="registry_file", help="Override registry.json path.")
    parser.add_argument("--language", dest="language", help="Override language hint (e.g., de, gez).")
    parser.add_argument("--translation", dest="translation_file", help="Optional translation input file to attach as analysis_translation_draft.")
    parser.add_argument("--translation-lang", dest="translation_lang", help="Optional translation language code (e.g., de, en).")
    parser.add_argument("--aliases-file", action="append", dest="alias_files", help="Additional aliases.json paths (repeatable).")
    args = parser.parse_args()

    if args.input_file:
        INPUT_FILE = args.input_file
    if args.data_file:
        DATA_FILE = args.data_file
    if args.registry_file:
        REGISTRY_FILE = args.registry_file
        REGISTRY_DIR = os.path.dirname(REGISTRY_FILE)
    translation_file = args.translation_file or TRANSLATION_FILE
    translation_lang = args.translation_lang

    # 1. Parse Input
    data = parse_input_file(INPUT_FILE)

    if not data:
        print("⚠️ Keine Daten gefunden oder Datei leer.")
        return

    language = _detect_language(INPUT_FILE, DATA_FILE, args.language)
    alias_files = _resolve_alias_files(language, args.alias_files or [])
    aliases = load_aliases(alias_files)
    registry = {"assets": {}, "aliases": {}}
    for a in aliases:
        if a.get("id"):
            registry["aliases"][a["id"]] = {
                "id": a.get("id"),
                "labels": a.get("labels", []),
                "patterns": a.get("patterns", [])
            }

    translation_map = {}
    if translation_file:
        if os.path.exists(translation_file):
            translation_entries = parse_input_file(translation_file)
            translation_map = {e.get("verse_id"): e.get("text") for e in translation_entries if e.get("verse_id")}
            print(f"✅ Parsed {len(translation_map)} translation verses from {translation_file}")
        else:
            print(f"⚠️ Translation file not found: {translation_file}")

    count = 0
    for entry in data:
        text = entry.get("text", "")
        if not text:
            continue
            
        # 2. Generiere IDs
        chars, word_list = generate_ids(text, SEPARATOR)

        # Pre-processing for words (root, vowel order, stopword flag)
        for w in word_list:
            w["pre_processing"] = build_pre_processing(w.get("text", ""))
            w["genre_overlay"] = {
                "current_genre": None,
                "timeline": None,
                "mapped_id": None,
                "visual_anchor": None
            }
            w["enrichment_data"] = {
                "core_concept": None,
                "impact_on_actors": None
            }
            register_asset(registry, w["pre_processing"], entry.get("verse_id"))
        
        # NEUE STRUKTUR: base_chars und words auf Verse-Ebene (Preprocessing)
        entry["base_chars"] = chars
        entry["words"] = word_list

        # Placeholders for Analysis Stages (Linguistic Compiler Order)
        # Stage 1: Graphematic can be prefilled or left pending for run_stage.
        if GRAPHEMATIC_PREFILL:
            entry["analysis_graphematic"] = build_graphematic_analysis(text, word_list)
        else:
            entry["analysis_graphematic"] = {
                "punctuation_markers": [],
                "punctuation_index": [],
                "punctuation_links": [],
                "removed_artifacts": [],
                "uncertainties": [],
                "status": "pending"
            }
        entry["analysis_graphematic_review"] = None # Optional LLM Review
        entry["analysis_morphologic"] = None   # 2. Tokenization & Math
        entry["analysis_morphologic_review"] = None # Optional LLM Review
        entry["analysis_syntactic"] = None     # 3. Structure
        entry["analysis_syntactic_review"] = None # Optional LLM Review
        entry["analysis_semantic"] = None      # 4. Skins & Meaning
        draft_translation = translation_map.get(entry.get("verse_id")) if translation_map else None
        entry["analysis_translation_draft"] = draft_translation # 5a. Draft / Reasoning
        entry["analysis_translation"] = None   # 5b. Final JSON Output
        if draft_translation:
            entry["analysis_translation_lang"] = translation_lang or "unknown"
        entry["analysis_entities"] = None      # 6. Vision/World (Optional)
        entry["analysis_websearch"] = None     # 7. External Context (Optional)

        entry["verse_metrics"] = compute_verse_metrics(word_list)
        entry["alias_hits"] = []
        triggers, updates = apply_state_triggers(word_list, registry)
        entry["state_triggers"] = triggers
        entry["state_updates"] = updates

        # Stateful chat tracking (predefined structure)
        entry["state_ids"] = {
            "graphematic": {"id": None, "model": None},
            "morphologic": {"id": None, "model": None},
            "syntactic": {"id": None, "model": None},
            "semantic": {"id": None, "model": None},
            "translation": {"id": None, "model": None},
            "entities": {"id": None, "model": None},
            "websearch": {"id": None, "model": None},
            "asset_cards": {"id": None, "model": None}
        }
        
        count += 1


    # 3. Alias Hits (post-pass, supports DE capitalized heuristic)
    cap_counts = {}
    if language == "de" and DE_ENTITIES_CFG.get("enable_capitalized_heuristic"):
        cap_counts = compute_capitalized_counts(data, DE_ENTITIES_CFG)
    for entry in data:
        entry["alias_hits"] = find_alias_hits(
            entry.get("words", []) or [],
            aliases,
            language=language,
            cap_counts=cap_counts,
            cap_cfg=DE_ENTITIES_CFG
        )

    # 4. Speichern
    print(f"💾 Speichere {DATA_FILE}...")
    custom_json_dump(data, DATA_FILE) # Use custom dumper

    # 5. Registry speichern
    try:
        os.makedirs(REGISTRY_DIR, exist_ok=True)
        with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(registry, f, ensure_ascii=False, indent=2)
        print(f"📚 Registry geschrieben: {REGISTRY_FILE}")
    except Exception as e:
        print(f"⚠️ Registry nicht geschrieben: {e}")

    print(f"✅ Fertig. {count} Verse initialisiert.")
    print(f"🔗 Separator war: '{SEPARATOR}'")


if __name__ == "__main__":
    main()
