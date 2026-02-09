import json
import os
import argparse
import datetime

SCHEMA_VERSION = "interlanguage.links.v1"


def load_data(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _collect_subjects(verse: dict) -> list[str]:
    subjects = set()
    ent = verse.get("analysis_entities") or {}
    for e in ent.get("entities", []) or []:
        aid = e.get("asset_id")
        if aid:
            subjects.add(aid)
    for ah in verse.get("alias_hits", []) or []:
        aid = ah.get("alias_id")
        if aid:
            subjects.add(aid)
    return sorted(subjects)


def _build_index(data: list) -> dict:
    idx = {}
    for v in data or []:
        vid = v.get("verse_id")
        if not vid:
            continue
        idx[vid] = {
            "verse_id": vid,
            "chapter": v.get("chapter"),
            "verse": v.get("verse"),
            "subjects": _collect_subjects(v),
        }
    return idx

def _build_stats(links: list[dict]) -> dict:
    total = len(links)
    shared_counts = [len(l.get("shared_subjects") or []) for l in links]
    with_shared = sum(1 for c in shared_counts if c > 0)
    avg_shared = (sum(shared_counts) / total) if total else 0.0
    max_shared = max(shared_counts) if shared_counts else 0

    subject_freq: dict[str, int] = {}
    for l in links:
        for sid in l.get("shared_subjects") or []:
            subject_freq[sid] = subject_freq.get(sid, 0) + 1
    top_shared = sorted(subject_freq.items(), key=lambda x: (-x[1], x[0]))[:25]

    return {
        "total_links": total,
        "links_with_shared": with_shared,
        "percent_with_shared": round((with_shared / total * 100.0) if total else 0.0, 2),
        "avg_shared_per_link": round(avg_shared, 3),
        "max_shared_per_link": max_shared,
        "top_shared_subjects": [{"id": sid, "count": cnt} for sid, cnt in top_shared]
    }


def main():
    parser = argparse.ArgumentParser(description="Link parallel story_data files by verse_id + subject IDs")
    parser.add_argument("--data-a", required=True, help="Primary story_data.json")
    parser.add_argument("--data-b", required=True, help="Secondary story_data.json")
    parser.add_argument("--lang-a", default="gez", help="Language code for data-a")
    parser.add_argument("--lang-b", default="de", help="Language code for data-b")
    parser.add_argument("--out", default="stories/template/subjects/links.json", help="Output path")
    parser.add_argument("--min-shared", type=int, default=0, help="Minimum shared subjects to include a link")
    parser.add_argument("--stats-out", help="Optional path to write link stats JSON")
    parser.add_argument("--print-stats", action="store_true", help="Print shared-subject statistics")
    args = parser.parse_args()

    if not os.path.exists(args.data_a) or not os.path.exists(args.data_b):
        raise SystemExit("Data file not found.")

    data_a = load_data(args.data_a)
    data_b = load_data(args.data_b)
    idx_a = _build_index(data_a)
    idx_b = _build_index(data_b)

    links = []
    for vid, a in idx_a.items():
        b = idx_b.get(vid)
        if not b:
            continue
        shared = sorted(set(a.get("subjects", [])) & set(b.get("subjects", [])))
        if len(shared) < args.min_shared:
            continue
        links.append({
            "verse_id": vid,
            "chapter": a.get("chapter"),
            "verse": a.get("verse"),
            "shared_subjects": shared,
            args.lang_a: a.get("subjects", []),
            args.lang_b: b.get("subjects", [])
        })

    out = {
        "schema": SCHEMA_VERSION,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "languages": [
            {"id": args.lang_a, "data": os.path.abspath(args.data_a)},
            {"id": args.lang_b, "data": os.path.abspath(args.data_b)}
        ],
        "links": links
    }

    stats = _build_stats(links)
    out["stats"] = stats

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Links written: {args.out} | verses={len(links)}")
    if args.stats_out:
        os.makedirs(os.path.dirname(args.stats_out), exist_ok=True)
        with open(args.stats_out, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        print(f"Stats written: {args.stats_out}")
    if args.print_stats:
        print(f"Shared subjects: {stats['links_with_shared']}/{stats['total_links']} "
              f"({stats['percent_with_shared']}%), avg={stats['avg_shared_per_link']}, "
              f"max={stats['max_shared_per_link']}")


if __name__ == "__main__":
    main()
