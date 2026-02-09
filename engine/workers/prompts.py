import json
try:
    from .fidel_ops import GEZ_SUFFIX_MATH, decompose_word
except ImportError:
    from fidel_ops import GEZ_SUFFIX_MATH, decompose_word

MORPHOLOGY_PROMPT_VERSION = "v1.0.0"
SYNTAX_PROMPT_VERSION = "v1.0.0"
SEMANTIC_PROMPT_VERSION = "v1.0.0"
TRANSLATION_PROMPT_VERSION = "v1.0.0"

# -----------------------------------------------------------------------------
# 0. GRAPHEMATIC
# -----------------------------------------------------------------------------
def build_graphematic_prompt(text: str, verse_meta: dict | None = None) -> str:
    payload = {"text": text}
    if verse_meta:
        payload["verse_meta"] = verse_meta
        
    return (
        "SYSTEM CHECK: Did you receive the input text completely? Confirm with 'OK'. "
        "If the text is incomplete or cut off, output ONLY the error location.\n\n"
        "Instruction: Perform a scientific Graphematic Review (Level A).\n"
        "Goal: Validate the physical text state, checking for artifacts or punctuation errors.\n\n"
        "Rules:\n"
        "1. Check if the text matches standard Ge'ez punctuation rules.\n"
        "2. Identify any scanning artifacts or anomalies.\n"
        "3. Provide a brief status report.\n"
        "4. Do NOT output JSON.\n\n"
        f"Input Text:\n{text}\n\n"
        "REVIEW:"
    )

# -----------------------------------------------------------------------------
# 1. MORPHOLOGY (With Fidel Math)
# -----------------------------------------------------------------------------
def build_morphology_prompt(tokens: list, pos_tags: list[str], verse_meta: dict | None = None, registry_context: list | None = None, compact: bool = False) -> str:
    """
    Constructs the prompt injecting Fidel Math to guide the small model.
    Input "tokens" should be a list of surface strings or pre-decomposed objects.
    """
    
    # Pre-process tokens using the Linguistic Compiler logic
    processed_tokens = []
    for t in tokens:
        if isinstance(t, dict):
            surface = t.get("surface") or t.get("text") or ""
            if not surface:
                continue
            decomp = decompose_word(surface)
            if compact:
                pp = t.get("pre_processing", {})
                ont = pp.get("ontology", {})
                fm = decomp.get("fidel_math") or []
                order_seq = [m.get("o") for m in fm if isinstance(m, dict) and m.get("o")]
                entry = {
                    "i": t.get("word_id"),
                    "s": surface,
                    "o": pp.get("grammatical_vowel"),
                    "rk": ont.get("root_key"),
                    "a": ont.get("asset_tag"),
                    "h": pp.get("pos_hint"),
                    "p": pp.get("prefix"),
                    "n": pp.get("nominal_prefix"),
                    "sp": (pp.get("spatial") or {}).get("mode"),
                    "spid": (pp.get("spatial") or {}).get("layer_id"),
                    "u": (pp.get("unknown_fallback") or {}).get("pattern"),
                    "fm": order_seq
                }
                # drop empty keys
                entry = {k: v for k, v in entry.items() if v is not None and v != "" and v != []}
            else:
                entry = {
                    "surface": surface,
                    "fidel_math": decomp.get("fidel_math"),
                    "lexical_info": decomp.get("lexical_info")
                }
                for key in ["word_id", "char_ids", "pre_processing"]:
                    if key in t:
                        entry[key] = t[key]
            processed_tokens.append(entry)
        else:
            surface = t if isinstance(t, str) else str(t)
            if surface:
                if compact:
                    decomp = decompose_word(surface)
                    fm = decomp.get("fidel_math") or []
                    order_seq = [m.get("o") for m in fm if isinstance(m, dict) and m.get("o")]
                    processed_tokens.append({"s": surface, "fm": order_seq})
                else:
                    processed_tokens.append(decompose_word(surface))

    payload = {
        "tokens_context": processed_tokens,
        "available_pos_tags": pos_tags
    }
    if verse_meta:
        payload["verse_meta"] = verse_meta
    if registry_context:
        payload["registry_context"] = registry_context

    if compact:
        input_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        suffix_line = "; ".join([f"{k}={v}" for k, v in GEZ_SUFFIX_MATH.items()])
        keys_line = (
            "Keys: s=surface, fm=order-seq, o=last-order, rk=root_key, a=asset_tag, "
            "p=prefix, n=nominal_prefix, h=pos_hint, sp/spid=spatial, u=unknown"
        )
        return (
            "Instruction: Use Ge'ez Suffix Mathematics to decode tokens.\n"
            "Context: You are a logic engine for an Abugida script. Do NOT translate freely.\n"
            "Keep outputs SHORT. No extra commentary.\n\n"
            f"{keys_line}\n"
            f"SuffixMath: {suffix_line}\n\n"
            "TASK:\n"
            "1. Analyze the 'fm' sequence for each token.\n"
            "2. If rk/a are provided, USE THEM strictly.\n"
            "3. Derive POS and syntax role from suffix order.\n\n"
            f"INPUT DATA:\n{input_json}\n\n"
            "OUTPUT FORMAT (Strict JSON):\n"
            "{\n"
            "  \"schema\": \"morph.v2.compact\",\n"
            "  \"tokens\": [\n"
            "    {\n"
            "      \"i\": 1,\n"
            "      \"s\": \"...\",\n"
            "      \"r\": \"...\",\n"
            "      \"p\": \"N|V|ADJ|PRON|PREP|ADV|CONJ\",\n"
            "      \"role\": \"ACCUSATIVE_CONSTRUCT\",\n"
            "      \"o\": 1,\n"
            "      \"fx\": [\"OPTIONAL_FLAGS\"]\n"
            "    }\n"
            "  ]\n"
            "}"
        )

    return (
        "Instruction: Use Ge'ez Suffix Mathematics to decode tokens.\n"
        "Context: You are a logic engine for an Abugida script. Do NOT translate freely.\n\n"
        "REFERENCE TABLE (Suffix Math):\n"
        f"{json.dumps(GEZ_SUFFIX_MATH, indent=2, ensure_ascii=False)}\n\n"
        "TASK:\n"
        "1. Analyze the 'fidel_math' (c=Char, o=Order) for each token.\n"
        "2. If 'lexical_info' is provided (from DB), USE IT strictly.\n"
        "3. Determine the POS and Morphological breakdown based on the suffix order.\n\n"
        f"INPUT DATA:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "OUTPUT FORMAT (Strict JSON):\n"
        "{\n"
        "  \"tokens\": [\n"
        "    {\n"
        "      \"token_id\": \"t1\",\n"
        "      \"surface\": \"...\",\n"
        "      \"analysis\": {\n"
        "        \"root\": \"...\",\n"
        "        \"pos\": \"...\",\n"
        "        \"gloss\": \"...\",\n"
        "        \"grammatical_notes\": \"Order X indicates...\"\n"
        "      }\n"
        "    }\n"
        "  ]\n"
        "}"
    )

def build_morphology_prompt_text(tokens: list, pos_tags: list[str], verse_meta: dict | None = None) -> str:
    """
    Plain-text morphologic prompt (no JSON output).
    """
    lines = []
    for t in tokens:
        if not isinstance(t, dict):
            continue
        surface = t.get("text") or t.get("surface") or ""
        if not surface:
            continue
        pp = t.get("pre_processing", {})
        ont = pp.get("ontology", {})
        word_id = t.get("word_id")
        order = pp.get("grammatical_vowel")
        pattern = (pp.get("unknown_fallback") or {}).get("pattern")
        if not pattern:
            decomp = decompose_word(surface)
            fm = decomp.get("fidel_math") or []
            pattern = "-".join(str(m.get("o")) for m in fm if isinstance(m, dict) and m.get("o"))
        root_key = ont.get("root_key") or "?"
        asset_tag = ont.get("asset_tag") or "?"
        prefix = pp.get("prefix") or ""
        nominal = pp.get("nominal_prefix") or ""
        pos_hint = pp.get("pos_hint") or ""
        role_hint = pp.get("syntax_role") or ""
        spatial = (pp.get("spatial") or {}).get("mode") or ""
        line = (
            f"t{word_id} surface=\"{surface}\"; order={order}; suffix={pattern}; "
            f"root_key={root_key}; asset_tag={asset_tag}; "
            f"prefix={prefix}; nominal={nominal}; pos_hint={pos_hint}; "
            f"role_hint={role_hint}; spatial={spatial}"
        )
        lines.append(line)

    suffix_line = "; ".join([f"{k}={v}" for k, v in GEZ_SUFFIX_MATH.items()])
    pos_line = ", ".join(pos_tags)
    meta_line = ""
    if verse_meta:
        meta_line = f"Verse: {verse_meta.get('verse_id')} | pacing={verse_meta.get('verse_metrics')}\n"

    return (
        "Instruction: Perform Morphological Analysis (Level B) for each token.\n"
        "Goal: Determine POS and syntax role using suffix math and the hints.\n"
        "Do NOT translate or paraphrase the verse.\n"
        "Do NOT output JSON or bullet lists.\n"
        "Write one short line per token, in the same order, starting with the token id (t1, t2, ...).\n"
        "Each line must include POS=<tag> and ROLE=<tag> and may include ROOT=<root_key>.\n\n"
        f"{meta_line}"
        f"Allowed POS tags: {pos_line}\n"
        f"Suffix Math: {suffix_line}\n\n"
        "TOKENS:\n"
        + "\n".join(lines)
        + "\n\n"
        "Respond now."
    )

def build_morphology_review_prompt(morph_data: dict, verse_meta: dict | None = None) -> str:
    meta = verse_meta or {}
    return (
        "Instruction: Review the Morphological Analysis (Level B).\n"
        "Goal: Identify errors or inconsistencies. If none, say 'OK'.\n"
        "Format: Plain text review. Do NOT output JSON.\n\n"
        f"MORPHOLOGIC DATA:\n{json.dumps(morph_data, ensure_ascii=False, indent=2)}\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "REVIEW:"
    )

# -----------------------------------------------------------------------------
# 2. SYNTAX
# -----------------------------------------------------------------------------
def build_syntax_prompt(morph_analysis: dict, verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    # Legacy / Standalone Mode
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context
    compact_hint = ""
    if isinstance(morph_analysis, dict) and morph_analysis.get("schema") == "morph.v2.compact":
        compact_hint = (
            "Compact Morph Schema: tokens[i,s,r,p,role,o,fx]. "
            "Use r=root, p=POS, role=syntax role, o=order.\n\n"
        )
    return (
        "Instruction: Perform Syntactic Analysis (Level C).\n"
        "Goal: Link the analyzed tokens based on their Grammatical Notes.\n\n"
        f"{compact_hint}"
        f"Morphological Data:\n{json.dumps(morph_analysis, ensure_ascii=False, indent=2)}\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "OUTPUT FORMAT (Strict JSON):\n"
        "{\n"
        "  \"syntax\": {\n"
        "    \"parses\": [\n"
        "      {\n"
        "        \"id\": \"S1\",\n"
        "        \"structure_type\": \"Nominal Chain\",\n"
        "        \"bracket_notation\": \"[NP ...]\",\n"
        "        \"dependencies\": [\"t1->t2\"]\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}"
    )

def build_syntax_review_prompt(syntax_data: dict, verse_meta: dict | None = None) -> str:
    meta = verse_meta or {}
    return (
        "Instruction: Review the Syntactic Analysis (Level C).\n"
        "Goal: Identify errors or inconsistencies. If none, say 'OK'.\n"
        "Format: Plain text review. Do NOT output JSON.\n\n"
        f"SYNTACTIC DATA:\n{json.dumps(syntax_data, ensure_ascii=False, indent=2)}\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "REVIEW:"
    )

def build_syntax_prompt_chained(verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    # Conversational Mode: Relies on previous Morphological Logic Output
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context
    return (
        "Instruction: Perform Syntactic Analysis (Level C) on the tokens you just analyzed.\n"
        "Goal: Link the tokens based on the Grammatical Notes you identified.\n\n"
        "If previous morphology was compact, tokens use keys: i,s,r,p,role,o,fx.\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "OUTPUT FORMAT (Strict JSON):\n"
        "{\n"
        "  \"syntax\": {\n"
        "    \"parses\": [\n"
        "      {\n"
        "        \"id\": \"S1\",\n"
        "        \"structure_type\": \"Nominal Chain (e.g., Construct State)\",\n"
        "        \"bracket_notation\": \"[NP ...]\",\n"
        "        \"dependencies\": [\"t1->t2\"]\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}"
    )

# -----------------------------------------------------------------------------
# 3. SEMANTIC (Genre/Skin Selector)
# -----------------------------------------------------------------------------
def build_semantic_prompt_with_skins(parses: list[dict], genre: str = "neutral", verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    genre_rules = {
        "sci_fi": "Translate theological entities as SYSTEM_COMPONENTS or PROXIES.",
        "drama": "Translate with archaic, Shakespearean intensity.",
        "neutral": "Standard philological literal translation."
    }
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context
    
    return (
        f"Instruction: Perform a com Semantic and Historical Analysis of the verse.\n"
        f"Context: Apply the '{genre}' perspective. {genre_rules.get(genre, genre_rules['neutral'])}\n\n"
        "TASK:\n"
        "1. Analyze the meaning of the verse based on the provided syntactic structure.\n"
        "2. discuss historical context, theological implications, or parallels (e.g., Book of Enoch, Bible).\n"
        "3. Explain specific word choices or ambiguities.\n"
        "4. Provide a coherent interpretation.\n\n"
        "Write a clear, structured essay.\n\n"
        f"Input Syntax Parses:\n{json.dumps(parses, ensure_ascii=False, indent=2)}\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "ANALYSIS:"
    )

def build_semantic_prompt_chained(genre: str = "neutral", verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    genre_rules = {
        "sci_fi": "Translate theological entities as SYSTEM_COMPONENTS or PROXIES.",
        "drama": "Translate with archaic, Shakespearean intensity.",
        "neutral": "Standard philological literal translation."
    }
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context
    
    return (
        f"Instruction: Perform a deep Semantic and Historical Analysis based on your previous syntax work.\n"
        f"Context: Apply the '{genre}' perspective. {genre_rules.get(genre, genre_rules['neutral'])}\n\n"
        "TASK:\n"
        "1. Analyze the meaning of the verse.\n"
        "2. Discuss historical context, theological implications, or parallels.\n"
        "3. Explain specific word choices or ambiguities.\n"
        "4. Provide a coherent interpretation.\n\n"
        "Write a clear, structured essay.\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "ANALYSIS:"
    )

# -----------------------------------------------------------------------------
# 4. TRANSLATION SPACE
# -----------------------------------------------------------------------------
def build_translation_prompt(parses: list, tokens: list, semantic_analysis: str | dict | None = None, verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    # Prepare payload with syntactic and morphologic data
    data_context = {"parses": parses, "tokens": tokens}
    
    # Semantic analysis can be a JSON object (old) or a text essay (new)
    sem_text = ""
    if isinstance(semantic_analysis, str):
        sem_text = semantic_analysis
    elif isinstance(semantic_analysis, dict):
        sem_text = json.dumps(semantic_analysis, ensure_ascii=False, indent=2)
    
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context

    return (
        "Instruction: Generate a constrained translation space.\n"
        "Goal: Provide literal variants mapped to parse + token option IDs.\n\n"
        "Rules:\n"
        "1. Use the provided Semantic Analysis as the guide for meaning.\n"
        "2. Map each variant to parse_ref and token_map.\n"
        "3. No free paraphrase outside the variants.\n\n"
        f"SEMANTIC ANALYSIS (Guide):\n{sem_text}\n\n"
        f"SYNTAX & MORPHOLOGY (Data):\n{json.dumps(data_context, ensure_ascii=False, indent=2)}\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "Output strictly valid JSON:\n"
        "{\n"
        "  \"translation_space\": {\n"
        "    \"variants\": [\n"
        "      {\n"
        "        \"id\": \"T1\",\n"
        "        \"text\": \"...\",\n"
        "        \"parse_ref\": \"S1\",\n"
        "        \"token_map\": [{\"token_id\": \"t1\", \"option_id\": \"A\"}],\n"
        "        \"notes\": \"\"\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}"
    )

def build_translation_draft_prompt(parses: list, tokens: list, semantic_analysis: str | None = None, verse_meta: dict | None = None) -> str:
    sem_text = semantic_analysis if semantic_analysis else "No semantic analysis provided."
    data_context = {"parses": parses, "tokens": tokens}
    meta = verse_meta or {}

    return (
        "Instruction: Draft a Translation Strategy (Reasoning Phase).\n"
        "Goal: Explore translation options, ambiguities, and style before finalizing.\n\n"
        "TASK:\n"
        "1. Review the Semantic Analysis and Syntax.\n"
        "2. Discuss difficult terms or grammatical constructs.\n"
        "3. Propose 2-3 variants (Literal vs. Fluent).\n"
        "4. Justify your choices.\n\n"
        "Provide a draft translation and reasoning.\n\n"
        f"SEMANTIC CONTEXT:\n{sem_text}\n\n"
        f"SYNTACTIC DATA:\n{json.dumps(data_context, ensure_ascii=False, indent=2)}\n\n"
        "DRAFT:"
    )

def build_translation_prompt_chained(verse_meta: dict | None = None, registry_context: list | None = None) -> str:
    # In chained mode (Stateful), the semantic essay is in the context window.
    # We just trigger the JSON generation.
    meta = {}
    if verse_meta:
        meta["verse_meta"] = verse_meta
    if registry_context:
        meta["registry_context"] = registry_context
    return (
        "Instruction: Based on your Semantic Analysis above, generate the constrained translation space now.\n"
        "Goal: Convert your analysis into the formal JSON format.\n\n"
        "Rules:\n"
        "1. Map variants to the syntax structure you analyzed.\n"
        "2. Output strictly valid JSON.\n\n"
        f"Context Meta:\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "Output strictly valid JSON:\n"
        "{\n"
        "  \"translation_space\": {\n"
        "    \"variants\": [\n"
        "      {\n"
        "        \"id\": \"T1\",\n"
        "        \"text\": \"...\",\n"
        "        \"parse_ref\": \"S1\",\n"
        "        \"token_map\": [{\"token_id\": \"t1\", \"option_id\": \"A\"}],\n"
        "        \"notes\": \"\"\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}"
    )

# -----------------------------------------------------------------------------
# 5. WEBSEARCH (LLM + Tools)
# -----------------------------------------------------------------------------
def build_websearch_prompt(
    jobs: list,
    verse_meta: dict | None = None,
    registry_context: list | None = None,
    tool_name: str = "fetch",
    search_tool: str | None = None,
    fetch_tool: str | None = None,
    context_prefix: str | None = None
) -> str:
    lines = []
    if context_prefix:
        lines.append(context_prefix.strip())
    search_name = (search_tool or "").strip()
    fetch_name = (fetch_tool or "").strip()
    tool = (tool_name or "fetch").strip()
    for job in jobs or []:
        search_url = (job.get("search_url") or "").strip()
        query = (job.get("query") or "").strip()
        if search_name and query:
            if fetch_name:
                lines.append(f"{search_name} {query} -> {fetch_name} TOP")
            else:
                lines.append(f"{search_name} {query}")
            continue
        if search_url:
            lines.append(f"{tool} {search_url}")
        elif query:
            lines.append(f"Nutze {tool} suche nach: {query}")
    lines.append("")
    return "\n".join(lines)

def build_websearch_summary_prompt(
    job: dict,
    sources: list[dict],
    max_chars: int = 1600,
    context_prefix: str | None = None
) -> str:
    lines: list[str] = []
    if context_prefix:
        lines.append(context_prefix.strip())
    label = (job.get("label") or job.get("query") or "").strip()
    concept = (job.get("concept") or "").strip()
    scope = (job.get("scope") or "").strip()
    lines.append("Aufgabe: Schreibe eine dichte, zusammenhängende Beschreibung in Fließtext.")
    lines.append("Keine Listen, kein Markdown, kein JSON.")
    lines.append("Nutze ausschließlich die Quellen unten. Quellen inline als (URL) nennen.")
    if label:
        lines.append(f"Thema: {label}")
    if concept:
        lines.append(f"Konzept: {concept}")
    if scope:
        lines.append(f"Scope: {scope}")
    lines.append(f"Maximale Länge: {max_chars} Zeichen.")
    for idx, src in enumerate(sources, 1):
        title = (src.get("title") or "").strip()
        url = (src.get("url") or "").strip()
        text = (src.get("text") or "").strip()
        lines.append(f"Quelle {idx}: {title} | {url}")
        if text:
            lines.append(text)
    lines.append("")
    return "\n".join(lines)
