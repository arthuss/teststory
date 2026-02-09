import argparse
import datetime as dt
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
INVENTORY_DEFAULT = REPO_ROOT / "policies" / "script_inventory.json"
ALLOWED_STATUS = {"core", "experimental", "oneoff", "archived"}


def _rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def load_inventory(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    scripts = data.get("scripts")
    if not isinstance(scripts, list):
        raise ValueError("Inventory must contain a 'scripts' list.")
    return data


def discover_python_files() -> list[str]:
    files: list[str] = []

    workers_dir = REPO_ROOT / "engine" / "workers"
    if workers_dir.exists():
        files.extend(_rel(p) for p in workers_dir.glob("*.py"))

    tools_dir = REPO_ROOT / "tools"
    if tools_dir.exists():
        files.extend(_rel(p) for p in tools_dir.glob("*.py"))

    files.extend(_rel(p) for p in REPO_ROOT.glob("*.py"))
    return sorted(set(files))


def _count_references(target_script: str, file_list: list[str]) -> int:
    needle = Path(target_script).name
    count = 0
    for rel_path in file_list:
        if rel_path == target_script:
            continue
        if rel_path in {"policies/script_inventory.json"}:
            continue
        path = REPO_ROOT / rel_path
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        if needle in text:
            count += 1
    return count


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def _archive_target(rel_path: str) -> str:
    return f"archive/scripts/{rel_path}"


def audit(inventory_path: Path) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    inv = load_inventory(inventory_path)
    scripts = inv["scripts"]
    inventory_paths = {}
    invalid_status: list[str] = []

    for item in scripts:
        rel = item.get("path")
        status = item.get("status")
        if not rel:
            invalid_status.append("Inventory entry missing 'path'.")
            continue
        if status not in ALLOWED_STATUS:
            invalid_status.append(f"{rel}: invalid status '{status}'.")
        inventory_paths[rel] = item

    discovered = discover_python_files()

    unregistered = [p for p in discovered if p not in inventory_paths]
    missing_files = [p for p in inventory_paths if not (REPO_ROOT / p).exists()]

    today = dt.date.today()
    expired: list[str] = []
    all_text_candidates = [p for p in discovered]
    for docs_dir in ("docs", "policies"):
        candidate_root = REPO_ROOT / docs_dir
        if candidate_root.exists():
            all_text_candidates.extend(
                _rel(p)
                for p in candidate_root.rglob("*")
                if p.is_file() and p.suffix.lower() in {".md", ".json", ".txt", ".yml", ".yaml"}
            )
    all_text_candidates = sorted(set(all_text_candidates))

    likely_unused: list[str] = []
    archive_candidates: list[str] = []
    for rel, meta in inventory_paths.items():
        status = meta.get("status")
        review_after = _parse_date(meta.get("review_after"))
        if status in {"experimental", "oneoff"} and review_after and review_after < today:
            expired.append(f"{rel} (review_after={review_after.isoformat()})")
            archive_candidates.append(rel)
        ref_count = _count_references(rel, all_text_candidates)
        if status in {"experimental", "oneoff"} and ref_count == 0:
            likely_unused.append(rel)
            archive_candidates.append(rel)

    archive_candidates = sorted(set(archive_candidates))
    return unregistered, missing_files, expired, invalid_status + likely_unused, archive_candidates


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit script lifecycle hygiene.")
    parser.add_argument(
        "--inventory",
        default=str(INVENTORY_DEFAULT),
        help="Path to script inventory JSON."
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit code if policy issues are found."
    )
    args = parser.parse_args()

    inventory_path = Path(args.inventory)
    if not inventory_path.is_absolute():
        inventory_path = (REPO_ROOT / inventory_path).resolve()

    if not inventory_path.exists():
        print(f"[ERROR] Missing inventory: {inventory_path}")
        return 2

    unregistered, missing_files, expired, warnings, archive_candidates = audit(inventory_path)

    print("Script Hygiene Report")
    print(f"- Inventory: {inventory_path}")
    print(f"- Unregistered scripts: {len(unregistered)}")
    print(f"- Missing inventory files: {len(missing_files)}")
    print(f"- Expired temporary scripts: {len(expired)}")
    print(f"- Warnings: {len(warnings)}")
    print(f"- Archive candidates: {len(archive_candidates)}")

    if unregistered:
        print("\n[UNREGISTERED]")
        for item in unregistered:
            print(f"- {item}")

    if missing_files:
        print("\n[MISSING FROM FS]")
        for item in missing_files:
            print(f"- {item}")

    if expired:
        print("\n[EXPIRED]")
        for item in expired:
            print(f"- {item}")

    if warnings:
        print("\n[WARNINGS]")
        for item in warnings:
            print(f"- {item}")

    if archive_candidates:
        print("\n[ARCHIVE_PLAN]")
        for rel in archive_candidates:
            print(f"- {rel} -> {_archive_target(rel)}")

    violations = len(unregistered) + len(missing_files) + len(expired)
    if args.strict and (violations > 0):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
