import json
import os
import sys
import datetime
import argparse

# Config path
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

STAGE_ORDER = [
    "graphematic",
    "morphologic",
    "syntactic",
    "semantic",
    "websearch",
    "translation_draft",
    "translation",
    "entities",
    "asset_cards",
]

STAGE_KEY_MAP = {
    "translation_draft": "analysis_translation_draft",
    "translation": "analysis_translation",
}


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def usage():
    print("Usage:")
    print("  python engine/workers/reset_stage.py <stage> [--downstream] [--data-file <path>]")
    print("Examples:")
    print("  python engine/workers/reset_stage.py semantic")
    print("  python engine/workers/reset_stage.py semantic --downstream")
    print("  python engine/workers/reset_stage.py semantic --data-file story_data_de.json")
    print("Available stages:", ", ".join(STAGE_ORDER))


def resolve_stages(stage, downstream):
    if stage not in STAGE_ORDER:
        raise ValueError(f"Unknown stage: {stage}")
    if not downstream:
        return [stage]
    idx = STAGE_ORDER.index(stage)
    return STAGE_ORDER[idx:]


def reset_entry_stage(entry, stage):
    key = STAGE_KEY_MAP.get(stage, f"analysis_{stage}")
    if stage == "graphematic":
        val = entry.get(key)
        if isinstance(val, dict):
            val["punctuation_markers"] = []
            val["punctuation_index"] = []
            if "punctuation_links" in val:
                val["punctuation_links"] = []
            val["removed_artifacts"] = []
            val["uncertainties"] = []
            val["status"] = "pending"
        else:
            entry[key] = None
    else:
        entry[key] = None

    state_ids = entry.get("state_ids")
    if isinstance(state_ids, dict):
        if stage in state_ids:
            state_ids[stage] = {"id": None, "model": None}
        if stage == "translation_draft" and "translation" in state_ids:
            state_ids["translation"] = {"id": None, "model": None}


def main():
    parser = argparse.ArgumentParser(description="Reset analysis stages in story_data.json")
    parser.add_argument("stage", help="Stage to reset")
    parser.add_argument("--downstream", action="store_true", help="Reset selected stage and all downstream stages")
    parser.add_argument("--data-file", action="append", dest="data_files", help="Override story_data.json path (repeatable)")
    args = parser.parse_args()

    stage = args.stage.strip().lower()
    downstream = args.downstream

    if stage not in STAGE_ORDER:
        usage()
        sys.exit(1)

    config = load_config()
    default_data = os.path.join(os.path.dirname(__file__), config["files"]["data_file"])
    data_files = args.data_files if args.data_files else [default_data]

    stages_to_reset = resolve_stages(stage, downstream)

    for data_file in data_files:
        if not os.path.exists(data_file):
            print(f"Data file not found: {data_file}")
            continue

        with open(data_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data:
            for st in stages_to_reset:
                reset_entry_stage(entry, st)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{data_file}.{timestamp}.bak"
        try:
            os.replace(data_file, backup_file)
            print(f"Created backup: {backup_file}")
        except Exception:
            print("Warning: backup rename failed; continuing.")

        with open(data_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Reset stages in {data_file}: {', '.join(stages_to_reset)}")


if __name__ == "__main__":
    main()
