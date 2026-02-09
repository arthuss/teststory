import json
import os
import re
import argparse
import datetime

from init_structure import (
    custom_json_dump,
    load_aliases,
    find_alias_hits,
    compute_capitalized_counts,
    DE_ENTITIES_CFG,
)


def load_story(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_translation_map(data: list, prefer_field: str = "text") -> dict:
    mapping = {}
    for v in data or []:
        vid = v.get("verse_id")
        if not vid:
            continue
        value = None
        if prefer_field == "analysis_translation_draft":
            value = v.get("analysis_translation_draft")
        elif prefer_field == "analysis_translation":
            value = v.get("analysis_translation")
        else:
            value = v.get("text")
        if value:
            mapping[vid] = value
    return mapping


def tokenize_translation(text: str) -> list[dict]:
    if not text:
        return []
    # Keep letters/numbers/underscores and common hyphen/apostrophes
    tokens = re.findall(r"[\wÄÖÜäöüß'-]+", text, flags=re.UNICODE)
    words = []
    wid = 1
    for tok in tokens:
        if not tok:
            continue
        words.append({"word_id": wid, "text": tok})
        wid += 1
    return words


def _dedupe_alias_hits(hits: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for h in hits or []:
        key = (
            h.get("alias_id"),
            tuple(h.get("word_ids") or []),
            h.get("alias_label"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def main():
    parser = argparse.ArgumentParser(description="Merge translation text into story_data.json as analysis_translation_draft.")
    parser.add_argument("--data", required=True, help="Primary story_data.json (Ge'ez)")
    parser.add_argument("--translation-data", required=True, help="Secondary story_data.json (translation source)")
    parser.add_argument("--lang", default="de", help="Language code for translation (default: de)")
    parser.add_argument("--prefer-field", default="text",
                        choices=["text", "analysis_translation_draft", "analysis_translation"],
                        help="Field to copy from translation-data (default: text)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing analysis_translation_draft")
    parser.add_argument("--backup", action="store_true", help="Create a .bak copy of the target file")
    parser.add_argument("--update-alias-hits", action="store_true", help="Also compute alias_hits from translation text")
    parser.add_argument("--aliases-file", action="append", dest="aliases_files",
                        help="Alias JSON path(s) to use for translation hits (repeatable)")
    parser.add_argument("--language", default="de", help="Language hint for translation alias matching (default: de)")
    args = parser.parse_args()

    if not os.path.exists(args.data) or not os.path.exists(args.translation_data):
        raise SystemExit("Data file not found.")

    data = load_story(args.data)
    tdata = load_story(args.translation_data)
    tmap = build_translation_map(tdata, prefer_field=args.prefer_field)

    if args.backup:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = f"{args.data}.{ts}.bak"
        with open(args.data, "r", encoding="utf-8") as f:
            raw = f.read()
        with open(bak, "w", encoding="utf-8") as f:
            f.write(raw)
        print(f"Backup written: {bak}")

    # Optional: build alias hits from translation text
    aliases = []
    cap_counts = {}
    if args.update_alias_hits:
        alias_files = args.aliases_files or []
        if not alias_files:
            # Default to German alias seed if none provided
            alias_files = [os.path.join(os.path.dirname(__file__), "..", "config", "aliases_de.json")]
        aliases = load_aliases(alias_files)
        if args.language == "de" and DE_ENTITIES_CFG.get("enable_capitalized_heuristic"):
            cap_data = []
            for vid, text in tmap.items():
                words = tokenize_translation(text)
                if words:
                    cap_data.append({"verse_id": vid, "words": words})
            cap_counts = compute_capitalized_counts(cap_data, DE_ENTITIES_CFG)

    merged = 0
    skipped_existing = 0
    missing = 0
    alias_updated = 0

    for v in data or []:
        vid = v.get("verse_id")
        if not vid:
            continue
        tval = tmap.get(vid)
        if not tval:
            missing += 1
            continue
        existing_draft = v.get("analysis_translation_draft")
        if existing_draft and not args.overwrite:
            skipped_existing += 1
        else:
            v["analysis_translation_draft"] = tval
            v["analysis_translation_lang"] = args.lang or "unknown"
            merged += 1
        if args.update_alias_hits and aliases:
            words = tokenize_translation(tval)
            new_hits = find_alias_hits(words, aliases, language=args.language, cap_counts=cap_counts, cap_cfg=DE_ENTITIES_CFG)
            for h in new_hits:
                if "alias_label" not in h:
                    h["alias_label"] = None
                h["source"] = "translation"
            existing_hits = v.get("alias_hits") or []
            v["alias_hits"] = _dedupe_alias_hits(existing_hits + new_hits)
            alias_updated += 1

    custom_json_dump(data, args.data)
    print(f"Merged translations: {merged}")
    print(f"Skipped existing: {skipped_existing}")
    print(f"Missing in source: {missing}")
    if args.update_alias_hits:
        print(f"Alias-hits updated: {alias_updated}")


if __name__ == "__main__":
    main()
