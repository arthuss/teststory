# fidel_ops.py

# -----------------------------------------------------------------------------
# GE'EZ SUFFIX MATHEMATICS (Constraints for the Model)
# -----------------------------------------------------------------------------
GEZ_SUFFIX_MATH = {
    "1": "Accusative / Construct (Target or part-asset marker)",
    "2": "Nominative Plural (instance set marker)",
    "3": "Genitive (origin marker, rare)",
    "4": "Accusative (direct target of action)",
    "5": "Pronominal Suffix (possession marker)",
    "6": "Consonantal End (root boundary marker)",
    "7": "Object Suffix (verb object marker)"
}

# -----------------------------------------------------------------------------
# ROOT LOOKUP (Simulation of Vocabulary DB)
# -----------------------------------------------------------------------------
# In a real scenario, this would be a large JSON or SQLite lookup.
# Root DB uses normalized (1st order) Ge'ez keys.
ROOT_DB = {
    "ቀለ": {"root": "Q-L", "greek_anchor": "λόγος (logos)", "gloss": "word | voice | message"},
    "በረከ": {"root": "B-R-K", "greek_anchor": "εὐλογία (eulogia)", "gloss": "vital increase | blessing"},
    "ሀነከ": {"root": "H-N-K", "greek_anchor": "Ἑνώχ (Henoch)", "gloss": "Enoch (proper name)"},
    "ሀረየ": {"root": "H-R-Y", "greek_anchor": "ἐκλεκτός (eklektos)", "gloss": "chosen | selected"},
    "ሰደቀ": {"root": "Ṣ-D-Q", "greek_anchor": "δίκαιος (dikaios)", "gloss": "straight | correct"},
    "አከየ": {"root": "ʾ-K-Y", "greek_anchor": "πονηρός (poneros)", "gloss": "malice | corruption"},
    "ረሰዐ": {"root": "R-S-ʿ", "greek_anchor": "ἀσεβής (asebes)", "gloss": "lawless | rogue"},
    "ገዘአ": {"root": "G-Z-ʾ", "greek_anchor": "κύριος (kyrios)", "gloss": "sovereign power"},
    "አዘዘ": {"root": "ʾ-Z-Z", "greek_anchor": "?", "gloss": "command | script"},
    "ለአከ": {"root": "L-A-K", "greek_anchor": "?", "gloss": "sent proxy | messenger"},
    "ሰመየ": {"root": "S-M-Y", "greek_anchor": "?", "gloss": "upper layer"},
    "መጸአ": {"root": "M-Ṣ-A", "greek_anchor": "?", "gloss": "entry | arrival"},
    "ወለደ": {"root": "W-L-D", "greek_anchor": "?", "gloss": "instantiation | beget"},
    "ነጸረ": {"root": "N-Ṣ-R", "greek_anchor": "?", "gloss": "monitor | observe"},
    "ቀደሰ": {"root": "Q-D-S", "greek_anchor": "ἅγιος (hagios)", "gloss": "set apart | secure"},
    "መነደበ": {"root": "M-N-D-B", "greek_anchor": "θλῖψις (thlipsis)", "gloss": "pressure | load"},
    "መሰነ": {"root": "M-S-N", "greek_anchor": "?", "gloss": "corrupt | spoil"},
    "መነፈሰ": {"root": "M-N-F-S", "greek_anchor": "?", "gloss": "spirit | breath"},
    "መወዐለ": {"root": "M-W-A", "greek_anchor": "?", "gloss": "days | cycles"},
    "ረአሰ": {"root": "R-ʾ-S", "greek_anchor": "?", "gloss": "head | chief"},
    "ፈጠረ": {"root": "F-T-R", "greek_anchor": "?", "gloss": "compilation | create"},
    "ሰገ": {"root": "S-G", "greek_anchor": "?", "gloss": "physical resource"},
    "ሀለወ": {"root": "H-L-W", "greek_anchor": "?", "gloss": "existence | runtime"},
    "ከወነ": {"root": "K-W-N", "greek_anchor": "?", "gloss": "transformation | change"}
}

# Root-Ontology Matrix (neutral core, genre-agnostic)
# Keys are compact and normalized by normalize_root_key().
ROOT_ONTOLOGY_MATRIX = {
    "GZ'": {"concept": "SUPREME_AUTHORITY", "asset_tag": "ENTITY_SUPREME", "system_role": "Root Admin / Ursprung"},
    "HNK": {"concept": "INITIATED_DEDICATED", "asset_tag": "ACTOR_PRIMARY", "system_role": "User / Protagonist"},
    "MLK": {"concept": "MESSENGER_PROXY", "asset_tag": "ACTOR_AGENT", "system_role": "Interface / Bruecke"},
    "BRK": {"concept": "VITAL_INCREASE", "asset_tag": "STATE_POSITIVE", "system_role": "Optimierung / Segen"},
    "HRY": {"concept": "SELECTED_CHOSEN", "asset_tag": "PERM_HIGH", "system_role": "Privileg / Auswahl"},
    "SDQ": {"concept": "STRAIGHT_CORRECT", "asset_tag": "STATE_VERIFIED", "system_role": "Integritaet / Wahrheit"},
    "MNDB": {"concept": "COMPRESSION_DISTRESS", "asset_tag": "CONDITION_STRESS", "system_role": "System-Druck / Leid"},
    "MSN": {"concept": "CORRUPTION", "asset_tag": "ACTION_CORRUPT", "system_role": "Verderben / Korrumpieren"},
    "QL": {"concept": "TRANSMITTED_SOUND", "asset_tag": "PROP_SIGNAL", "system_role": "Information / Wort"},
    "AKY": {"concept": "MALICE_CORRUPTION", "asset_tag": "STATE_CORRUPT", "system_role": "Fehler / Bosheit"},
    "RSC": {"concept": "DEVIATION_LAWLESS", "asset_tag": "PERM_DENIED", "system_role": "Regelbruch / Gottlos"},
    "AZZ": {"concept": "COMMAND_SCRIPT", "asset_tag": "ACTION_COMMAND", "system_role": "Befehl / Direktive"},
    "LAK": {"concept": "SENT_PROXY", "asset_tag": "CLASS_ACTOR", "system_role": "Gesandter / Proxy"},
    "SMY": {"concept": "UPPER_LAYER", "asset_tag": "ENVIRONMENT_LAYER", "system_role": "Oberes Segment"},
    "MSA": {"concept": "ENTRY_ARRIVAL", "asset_tag": "EVENT_INGRESS", "system_role": "Eintritt / Ankunft"},
    "WLD": {"concept": "INSTANTIATION", "asset_tag": "ACTION_SPAWN", "system_role": "Instanziierung"},
    "NSR": {"concept": "MONITOR_OBSERVE", "asset_tag": "ACTION_OBSERVE", "system_role": "Beobachtung"},
    "QDS": {"concept": "SET_APART_SECURE", "asset_tag": "STATE_POSITIVE", "system_role": "Absonderung / Sicherung"},
    "FTR": {"concept": "COMPILATION_CREATE", "asset_tag": "ACTION_INITIALIZE", "system_role": "Erstellung / Aufbau"},
    "SG": {"concept": "PHYSICAL_RESOURCE", "asset_tag": "RESOURCE_PHYSICAL", "system_role": "Materielle Ressource"},
    "HLW": {"concept": "EXISTENCE_RUNTIME", "asset_tag": "STATE_ACTIVE", "system_role": "Laufzeit / Existenz"},
    "KWN": {"concept": "TRANSFORMATION", "asset_tag": "ACTION_STATE_CHANGE", "system_role": "Zustaendswechsel"}
}

# Minimal linker set (prefix order matters: longest first)
PREFIXES = ["እም", "እለ", "ዘ", "ወ", "በ", "ለ", "ይ"]
NOMINAL_PREFIXES = ["መ"]
SPATIAL_PREFIXES = {
    "በ": "IN",
    "እም": "SOURCE",
    "ለ": "TO"
}
SPATIAL_LAYER_IDS = {
    "SMY": "SKY_DOME"
}
STOPWORDS = set(PREFIXES)

SUFFIX_RULES = {
    1: {"syntax_role": "ACCUSATIVE_CONSTRUCT", "asset_rule": "Target / part-asset"},
    2: {"syntax_role": "NOMINATIVE_PLURAL", "asset_rule": "Instance set"},
    3: {"syntax_role": "GENITIVE_ORIGIN", "asset_rule": "Origin marker"},
    4: {"syntax_role": "ACCUSATIVE_DIRECT", "asset_rule": "Direct action target"},
    5: {"syntax_role": "PRONOMINAL_SUFFIX", "asset_rule": "Possession"},
    6: {"syntax_role": "CONSONANTAL_END", "asset_rule": "Root boundary"},
    7: {"syntax_role": "OBJECT_SUFFIX", "asset_rule": "Verb object"},
}

def get_fidel_order(char):
    """
    Returns the order (1-7) of a Ge'ez character.
    This is a simplified heuristic. A full implementation would need a comprehensive map.
    """
    # Basic heuristic: Ge'ez syllables are often blocks of 8 (including 2 special).
    # Common Syllables:
    # 1: ä, 2: u, 3: i, 4: a, 5: e, 6: ə, 7: o
    # We can try to deduce from unicode, but for the "Linguistic Compiler" demo,
    # we might just simulate or handle specific ranges.
    
    # For now, let's just return a placeholder or simple logic if feasible.
    # U+1200 matches HA (1). U+1201 HU (2)...
    
    val = ord(char)
    if 0x1200 <= val <= 0x137C: 
        # CAUTION: Not all blocks are 8 chars long strictly, but many are.
        # This is strictly a helper for the "Compiler" effect demonstration.
        offset = (val - 0x1200) % 8
        if offset < 7:
            return offset + 1
            
    # Default to 0 or 'unknown' for non-syllables
    return 0

def normalize_geez_to_root_key(text: str) -> str:
    """
    Normalizes Ge'ez text to the 1st order (vowel-agnostic).
    This yields a consonant-stable key for root lookup.
    """
    if not text:
        return ""
    normalized = []
    for char in text:
        code = ord(char)
        # Ge'ez syllables are primarily in U+1200..U+135A
        if 0x1200 <= code <= 0x135A:
            base_code = code - ((code - 0x1200) % 8)
            normalized.append(chr(base_code))
        else:
            normalized.append(char)
    return "".join(normalized)

def decompose_word(surface):
    """
    Decomposes a word into its Fidel constituents with math properties.
    """
    math_tokens = []
    for char in surface:
        order = get_fidel_order(char)
        # Compact format for LLM context optimization
        # Use simple keys: c=Char, o=Order. The model looks up Suffix Math in the Prompt Reference.
        math_tokens.append({
            "c": char,
            "o": order
        })
    
    # Check DB
    lex_info = lookup_lex(surface) or {
        "root": "?",
        "greek_anchor": "?",
        "gloss": "?"
    }
    
    return {
        "surface": surface,
        "fidel_math": math_tokens,
        "lexical_info": lex_info
    }

def normalize_root_key(root: str) -> str:
    if not root:
        return ""
    r = root.upper()
    # Normalize special consonants to stable ASCII keys
    r = r.replace("Ṣ", "S")
    r = r.replace("ʾ", "A").replace("ʼ", "A").replace("'", "A")
    r = r.replace("ʿ", "C")
    for ch in ["-", " ", "·", "_"]:
        r = r.replace(ch, "")
    return r

ROOT_ONTOLOGY_MATRIX_NORM = {normalize_root_key(k): v for k, v in ROOT_ONTOLOGY_MATRIX.items()}

def lookup_root_ontology(root: str):
    key = normalize_root_key(root)
    if not key:
        return None
    return ROOT_ONTOLOGY_MATRIX_NORM.get(key)

ROOT_DB_NORM = {normalize_geez_to_root_key(k): v for k, v in ROOT_DB.items()}

ALIAS_NORM_MAP = {
    "ከነ": "ከወነ"  # K-W-N (ex: ይኩኑ -> ኩኑ -> ከነ)
}

def _lookup_norm_key(norm_key: str):
    if not norm_key:
        return None
    lex = ROOT_DB_NORM.get(norm_key)
    if lex:
        return lex
    alias = ALIAS_NORM_MAP.get(norm_key)
    if alias:
        lex = ROOT_DB_NORM.get(alias)
        if lex:
            return lex
    if len(norm_key) > 1:
        trimmed = norm_key[:-1]
        lex = ROOT_DB_NORM.get(trimmed)
        if lex:
            return lex
        alias_trim = ALIAS_NORM_MAP.get(trimmed)
        if alias_trim:
            lex = ROOT_DB_NORM.get(alias_trim)
            if lex:
                return lex
    return None

def lookup_lex(surface: str):
    if not surface:
        return None
    # Normalized (vowel-agnostic) match first
    norm = normalize_geez_to_root_key(surface)
    lex = _lookup_norm_key(norm)
    if lex:
        return lex
    # Fallback: exact match (compat)
    return ROOT_DB.get(surface)

def infer_grammatical_vowel(surface: str) -> int:
    if not surface:
        return 0
    return get_fidel_order(surface[-1])

def infer_syntax_role(order: int) -> str:
    if order in SUFFIX_RULES:
        return SUFFIX_RULES[order]["syntax_role"]
    return "UNKNOWN"

def build_unknown_fallback(surface: str) -> dict:
    if not surface:
        return {"orders": [], "pattern": "", "last_order": 0, "length": 0}
    orders = []
    for ch in surface:
        order = get_fidel_order(ch)
        if order:
            orders.append(order)
    pattern = "-".join(str(o) for o in orders) if orders else ""
    return {
        "orders": orders,
        "pattern": pattern,
        "last_order": orders[-1] if orders else 0,
        "length": len(surface)
    }

def _infer_pos_from_asset_tag(asset_tag: str | None) -> str:
    if not asset_tag:
        return "UNKNOWN"
    if asset_tag.startswith("ACTION_") or asset_tag.startswith("EVENT_"):
        return "V"
    if asset_tag.startswith(("STATE_", "CONDITION_", "PERM_")):
        return "ADJ"
    if asset_tag.startswith(("PROP_", "RESOURCE_")):
        return "N"
    if asset_tag.startswith(("ACTOR_", "ENTITY_", "CLASS_")):
        return "N"
    return "N"

def build_pre_processing(surface: str) -> dict:
    surface = surface or ""
    order = infer_grammatical_vowel(surface)

    prefix_found = None
    nominal_prefix = None
    core_text = surface

    # Always attempt de-prefixing first (longest prefix wins), but fall back if no match.
    prefix_candidate = None
    base_candidates = [(surface, None)]
    for p in PREFIXES:
        if surface.startswith(p) and len(surface) > len(p):
            core_text = surface[len(p):]
            prefix_candidate = p
            base_candidates = [(core_text, p), (surface, None)]
            break

    candidates = []
    for cand, pref in base_candidates:
        # Optional nominal prefix stripping (safe with fallback)
        if any(cand.startswith(np) and len(cand) > len(np) for np in NOMINAL_PREFIXES):
            candidates.append((cand[1:], pref, "መ"))
        candidates.append((cand, pref, None))

    lex_info = None
    matched_candidate = surface
    for cand, pref, n_pref in candidates:
        candidate_lex = lookup_lex(cand)
        if candidate_lex:
            lex_info = candidate_lex
            matched_candidate = cand
            prefix_found = pref
            nominal_prefix = n_pref
            break

    if not lex_info:
        lex_info = {
            "root": "?",
            "greek_anchor": "?",
            "gloss": "?"
        }
        if prefix_candidate and not prefix_found:
            prefix_found = prefix_candidate
            matched_candidate = core_text

    # Recompute order based on matched core text so suffix math ignores prefixes.
    order = infer_grammatical_vowel(matched_candidate)

    is_stopword = surface in STOPWORDS
    root = lex_info.get("root", "?")
    ontology = lookup_root_ontology(root)
    root_in_gram = ontology is not None
    asset_tag = ontology.get("asset_tag") if ontology else None
    pos_hint = _infer_pos_from_asset_tag(asset_tag) if root_in_gram else "UNKNOWN"
    if not root_in_gram:
        if nominal_prefix == "መ":
            pos_hint = "N"
        elif prefix_found == "ይ":
            pos_hint = "V"
        elif prefix_found in {"በ", "ለ", "እም"}:
            pos_hint = "PREP"
        elif prefix_found in {"ዘ", "ወ", "እለ"}:
            pos_hint = "CONJ"
    role = "LINKER" if is_stopword else infer_syntax_role(order)
    root_key = normalize_root_key(root)
    asset_id = f"{asset_tag}_{root_key}" if asset_tag and root_key else None
    concept = ontology.get("concept") if ontology else None
    system_role = ontology.get("system_role") if ontology else None
    spatial = None
    if prefix_found in SPATIAL_PREFIXES:
        layer_id = SPATIAL_LAYER_IDS.get(root_key) if root_key else None
        spatial = {
            "mode": SPATIAL_PREFIXES[prefix_found],
            "layer_root": root_key if root_in_gram else None,
            "layer_tag": asset_tag if asset_tag else None,
            "layer_concept": concept if concept else None,
            "layer_id": layer_id
        }
    unknown_fallback = None if root_in_gram else build_unknown_fallback(matched_candidate)
    return {
        "root": root,
        "root_in_gram": root_in_gram,
        "phonetic_key": "",  # placeholder; fill later with a real transliteration
        "is_stopword": is_stopword,
        "grammatical_vowel": order,
        "syntax_role": role,
        "asset_type": asset_tag,
        "asset_id": asset_id,
        "ontology": {
            "root_key": root_key,
            "concept": concept,
            "asset_tag": asset_tag,
            "system_role": system_role
        },
        "is_asset": bool(ontology),
        "multi_instance": order == 2,
        "root_boundary": order == 6,
        "prefix": prefix_found,
        "nominal_prefix": nominal_prefix,
        "pos_hint": pos_hint,
        "core_text": matched_candidate,
        "normalized_surface": normalize_geez_to_root_key(matched_candidate),
        "spatial": spatial,
        "unknown_fallback": unknown_fallback
    }
