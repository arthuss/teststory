import json
import os
import argparse
import datetime

try:
    from .init_structure import (
        load_aliases,
        find_alias_hits,
        compute_capitalized_counts,
        _detect_language,
        _resolve_alias_files,
        DE_ENTITIES_CFG
    )
except ImportError:
    from init_structure import (
        load_aliases,
        find_alias_hits,
        compute_capitalized_counts,
        _detect_language,
        _resolve_alias_files,
        DE_ENTITIES_CFG
    )

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def refresh_aliases(data: list, aliases: list, update_entities: bool = True) -> int:
    changed = 0
    for verse in data:
        words = verse.get("words", []) or []
        new_hits = find_alias_hits(words, aliases)
        verse["alias_hits"] = new_hits
        if update_entities and isinstance(verse.get("analysis_entities"), dict):
            verse["analysis_entities"]["alias_hits"] = new_hits
        changed += 1
    return changed


def main():
    parser = argparse.ArgumentParser(description="Refresh alias_hits in story_data.json without re-init")
    parser.add_argument("--data-file", action="append", dest="data_files", help="Override story_data.json path (repeatable)")
    parser.add_argument("--language", dest="language", help="Override language hint (e.g., de, gez).")
    parser.add_argument("--aliases-file", action="append", dest="alias_files", help="Additional aliases.json paths (repeatable).")
    parser.add_argument("--no-update-entities", action="store_true", help="Do not update analysis_entities.alias_hits")
    args = parser.parse_args()

    config = load_config()
    default_data = os.path.join(os.path.dirname(__file__), config["files"]["data_file"])
    data_files = args.data_files if args.data_files else [default_data]

    for data_file in data_files:
        language = _detect_language(None, data_file, args.language)
        alias_files = _resolve_alias_files(language, args.alias_files or [])
        aliases = load_aliases(alias_files)
        if not aliases:
            print("⚠️ No aliases loaded. Nothing to refresh.")
            return
        if not os.path.exists(data_file):
            print(f"Data file not found: {data_file}")
            continue

        with open(data_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        cap_counts = {}
        if language == "de" and DE_ENTITIES_CFG.get("enable_capitalized_heuristic"):
            cap_counts = compute_capitalized_counts(data, DE_ENTITIES_CFG)
        for verse in data:
            new_hits = find_alias_hits(
                verse.get("words", []) or [],
                aliases,
                language=language,
                cap_counts=cap_counts,
                cap_cfg=DE_ENTITIES_CFG
            )
            verse["alias_hits"] = new_hits
            if not args.no_update_entities and isinstance(verse.get("analysis_entities"), dict):
                verse["analysis_entities"]["alias_hits"] = new_hits
        changed = len(data)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{data_file}.{timestamp}.bak"
        try:
            os.replace(data_file, backup_file)
            print(f"Created backup: {backup_file}")
        except Exception:
            print("Warning: backup rename failed; continuing.")

        with open(data_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Alias refresh complete: {data_file} | verses={changed}")


if __name__ == "__main__":
    main()
