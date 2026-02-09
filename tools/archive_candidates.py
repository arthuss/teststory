import argparse
import datetime as dt
import json
import shutil
from pathlib import Path

from file_hygiene import INVENTORY_DEFAULT, REPO_ROOT, audit, load_inventory


def _archive_target(rel_path: str) -> Path:
    return REPO_ROOT / "archive" / "scripts" / rel_path


def _read_inventory(inventory_path: Path) -> tuple[dict, dict[str, dict]]:
    data = load_inventory(inventory_path)
    scripts = data.get("scripts", [])
    by_path: dict[str, dict] = {}
    for item in scripts:
        rel = item.get("path")
        if rel:
            by_path[rel] = item
    return data, by_path


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def _apply_archive(
    inventory_path: Path,
    candidates: list[str],
    allow_missing: bool
) -> tuple[list[str], list[str], list[str]]:
    inventory_data, by_path = _read_inventory(inventory_path)
    moved: list[str] = []
    skipped: list[str] = []
    missing: list[str] = []

    for rel in candidates:
        src = REPO_ROOT / rel
        dst = _unique_path(_archive_target(rel))
        if not src.exists():
            if allow_missing:
                missing.append(rel)
                meta = by_path.get(rel)
                if meta:
                    meta["status"] = "archived"
                    meta["archived_path"] = dst.relative_to(REPO_ROOT).as_posix()
                    meta["archived_at"] = dt.datetime.now().isoformat()
                continue
            skipped.append(f"{rel} (source missing)")
            continue

        _ensure_parent(dst)
        shutil.move(str(src), str(dst))
        moved.append(f"{rel} -> {dst.relative_to(REPO_ROOT).as_posix()}")

        meta = by_path.get(rel)
        if meta:
            meta["path"] = dst.relative_to(REPO_ROOT).as_posix()
            meta["status"] = "archived"
            meta["archived_at"] = dt.datetime.now().isoformat()
            meta["archived_from"] = rel
            meta["review_after"] = None
        else:
            inventory_data.setdefault("scripts", []).append(
                {
                    "path": dst.relative_to(REPO_ROOT).as_posix(),
                    "status": "archived",
                    "owner": "unknown",
                    "review_after": None,
                    "notes": f"Archived from {rel}",
                    "archived_at": dt.datetime.now().isoformat(),
                    "archived_from": rel,
                }
            )

    with inventory_path.open("w", encoding="utf-8") as f:
        json.dump(inventory_data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    return moved, skipped, missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Archive likely-unused temporary scripts from file_hygiene report."
    )
    parser.add_argument(
        "--inventory",
        default=str(INVENTORY_DEFAULT),
        help="Path to script inventory JSON."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply archive actions (default is dry-run)."
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Mark missing files as archived in inventory."
    )
    args = parser.parse_args()

    inventory_path = Path(args.inventory)
    if not inventory_path.is_absolute():
        inventory_path = (REPO_ROOT / inventory_path).resolve()
    if not inventory_path.exists():
        print(f"[ERROR] Missing inventory: {inventory_path}")
        return 2

    _, _, _, _, archive_candidates = audit(inventory_path)

    print("Archive Candidates")
    print(f"- Inventory: {inventory_path}")
    print(f"- Candidates: {len(archive_candidates)}")

    if not archive_candidates:
        return 0

    for rel in archive_candidates:
        print(f"- {rel} -> {_archive_target(rel).relative_to(REPO_ROOT).as_posix()}")

    if not args.apply:
        print("\nDry-run only. Use --apply to execute.")
        return 0

    moved, skipped, missing = _apply_archive(
        inventory_path=inventory_path,
        candidates=archive_candidates,
        allow_missing=args.allow_missing
    )

    print("\nArchive Result")
    print(f"- moved: {len(moved)}")
    print(f"- skipped: {len(skipped)}")
    print(f"- missing(marked): {len(missing)}")

    if moved:
        print("\n[MOVED]")
        for item in moved:
            print(f"- {item}")
    if skipped:
        print("\n[SKIPPED]")
        for item in skipped:
            print(f"- {item}")
    if missing:
        print("\n[MISSING-MARKED]")
        for item in missing:
            print(f"- {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
