import json
import os

content = r'''import json
try:
    from .fidel_ops import GEZ_SUFFIX_MATH, decompose_word
except ImportError:
    from fidel_ops import GEZ_SUFFIX_MATH, decompose_word

MORPHOLOGY_PROMPT_VERSION = "v1.0.0"
SYNTAX_PROMPT_VERSION = "v1.0.0"
SEMANTIC_PROMPT_VERSION = "v1.0.0"
TRANSLATION_PROMPT_VERSION = "v1.0.0"

# -----------------------------------------------------------------------------
# 1. MORPHOLOGY (With Fidel Math)
# -----------------------------------------------------------------------------
def build_morphology_prompt(tokens: list, pos_tags: list[str]) -> str:
    """
    Constructs the prompt injecting Fidel Math to guide the small model.
    Input "tokens" should be a list of surface strings or pre-decomposed objects.
    """
    
    # Pre-process tokens using the Linguistic Compiler logic
    processed_tokens = []
    for t in tokens:
        # Assuming t is a string surface form for now
        # If it is already a dict, adjust access
        surface = t if isinstance(t, str) else t.get("surface", "")
        if surface:
            processed_tokens.append(decompose_word(surface))

    payload = {
        "tokens_context": processed_tokens,
        "available_pos_tags": pos_tags
    }

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

# -----------------------------------------------------------------------------
# 2. SYNTAX
# -----------------------------------------------------------------------------
def build_syntax_prompt(morph_analysis: dict) -> str:
    # Legacy / Standalone Mode
    return (
        "Instruction: Perform Syntactic Analysis (Level C).\n"
        "Goal: Link the analyzed tokens based on their Grammatical Notes.\n\n"
        f"Morphological Data:\n{json.dumps(morph_analysis, ensure_ascii=False, indent=2)}\n\n"
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

def build_syntax_prompt_chained() -> str:
    # Conversational Mode: Relies on previous Morphological Logic Output
    return (
        "Instruction: Perform Syntactic Analysis (Level C) on the tokens you just analyzed.\n"
        "Goal: Link the tokens based on the Grammatical Notes you identified.\n\n"
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
def build_semantic_prompt_with_skins(parses: list[dict], genre: str = "neutral") -> str:
    genre_rules = {
        "sci_fi": "Translate theological entities as SYSTEM_COMPONENTS or PROXIES.",
        "drama": "Translate with archaic, Shakespearean intensity.",
        "neutral": "Standard philological literal translation."
    }
    
    return (
        f"Instruction: Apply the '{genre}' Semantic Skin to the following parses.\n"
        f"Constraint: {genre_rules.get(genre, genre_rules['neutral'])}\n\n"
        "Rules:\n"
        "1. Map the 'Root' to the appropriate asset class.\n"
        "2. Ensure the Suffix Math (Grammar) is preserved in the output logic.\n\n"
        f"Input Parses:\n{json.dumps(parses, ensure_ascii=False, indent=2)}\n\n"
        "Output strictly valid JSON with 'evaluation' and 'decision_log'."
    )

def build_semantic_prompt_chained(genre: str = "neutral") -> str:
    genre_rules = {
        "sci_fi": "Translate theological entities as SYSTEM_COMPONENTS or PROXIES.",
        "drama": "Translate with archaic, Shakespearean intensity.",
        "neutral": "Standard philological literal translation."
    }
    
    return (
        f"Instruction: Apply the '{genre}' Semantic Skin to the syntax structure you defined.\n"
        f"Constraint: {genre_rules.get(genre, genre_rules['neutral'])}\n\n"
        "Rules:\n"
        "1. Map the 'Root' to the appropriate asset class.\n"
        "2. Ensure the Suffix Math (Grammar) is preserved in the output logic.\n\n"
        "Output strictly valid JSON with 'evaluation' and 'decision_log'."
    )

# -----------------------------------------------------------------------------
# 4. TRANSLATION SPACE
# -----------------------------------------------------------------------------
def build_translation_prompt(parses: list, tokens: list) -> str:
    payload = {"parses": parses, "tokens": tokens}
    return (
        "Instruction: Generate a constrained translation space.\n"
        "Goal: Provide literal variants mapped to parse + token option IDs.\n\n"
        "Rules:\n"
        "1. No free paraphrase.\n"
        "2. Map each variant to parse_ref and token_map.\n\n"
        f"Input JSON:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
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

def build_translation_prompt_chained() -> str:
    return (
        "Instruction: Generate a constrained translation space based on your Semantic Analysis.\n"
        "Goal: Provide literal variants mapped to parse + token option IDs.\n\n"
        "Rules:\n"
        "1. No free paraphrase.\n"
        "2. Map each variant to parse_ref and token_map.\n\n"
        "Output strictly valid JSON:\n"
        "{\n"
        "  \"translation_space\": {\n"
        "    \"variants\": [\n"
        "      {\n"
        "        \"id\": \"T1\",\n"
        "        \"text\": \"...\",\n"
        "        \"parse_ref\": \"S1 (from your syntax)\",\n"
        "        \"token_map\": [{\"token_id\": \"t1\", \"option_id\": \"A\"}],\n"
        "        \"notes\": \"\"\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}"
    )
'''

with open(r'c:\Users\sasch\teststory\engine\workers\prompts.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("prompts.py updated with chained prompts.")
