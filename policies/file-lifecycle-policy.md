# File Lifecycle Policy

This policy prevents one-off helper scripts from accumulating in the repo.

## Rules
1. Every Python script must be registered in `policies/script_inventory.json`.
2. New one-off or experimental scripts are not allowed without a `review_after` date.
3. One-off and experimental scripts must be reviewed on or before `review_after`.
4. Archived scripts must be moved out of active paths (`engine/workers`, repo root).
5. Policy violations are handled by archiving, not hard delete.
6. PRs adding new scripts must include:
   - an inventory entry
   - intended usage (`notes`)
   - lifecycle status (`core`, `experimental`, `oneoff`, `archived`)

## Active Paths
- `engine/workers/*.py`
- `tools/*.py`
- repo root `*.py`

## Enforcement
Run:

```bash
python tools/file_hygiene.py
```

Strict mode (fails on policy violations):

```bash
python tools/file_hygiene.py --strict
```

## Archive Instead Of Delete
- Default action for expired or likely-unused temporary scripts:
  - move to `archive/scripts/`
  - set status to `archived` in `policies/script_inventory.json`
- Keep script history in Git; do not remove historical utility code by default.

## Lifecycle Status
- `core`: maintained production script, no expiry.
- `experimental`: active test script, requires `review_after`.
- `oneoff`: temporary helper, requires `review_after`.
- `archived`: not active; should not remain in active paths.
