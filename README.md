# Pipeline Documentation

## Overview
This project processes Ge'ez text through a multi-stage linguistic pipeline. The core philosophy is to establish a stable, atomic ID structure for every character (**Atomic IDs**) before performing high-level LLM analyses.

We use a **Hybrid Approach**:
- **Deterministic (Python)**: For hard data (Graphematics, Indices, Syntax Heuristics).
- **Probabilistic (LLM)**: For Reviews, Semantic Essays, and Translation.

## Configuration
All settings are stored in `engine/config/config.json`.
- **Separator**: Defines the character used to split words (e.g., `፡`).
- **Models**: Usage of local LLMs via LM Studio.
- **Orchestrator**: Uses the same `models` list (single source of truth).
- **Adaptive Tokens**: Dynamic calculation of token context.
- **Token Limits**: Specific limits per stage (e.g., `review`: 1024, `semantic`: 4096).

## Workflow & Commands

The pipeline is executed sequentially using `engine/workers/run_stage.py`. Most stages support a `--mode` flag to switch between Python-calculation and LLM-Review.

### 1. Initialize Structure
Parses the input text, generates Atomic IDs (`base_chars`) and Word Groupings (`words`). Also performs initial lexical lookups (Root keys, Assets).
```bash
python engine/workers/init_structure.py
```
Default single‑registry workflow (Ge’ez input + embedded translation):
```bash
python engine/workers/init_structure.py \
  --input input/complete_story.txt \
  --translation input/complete_story_de.txt \
  --translation-lang de \
  --data story_data.json
```
Language-aware init (loads `aliases_de.json` and DE heuristics):
```bash
python engine/workers/init_structure.py --input input/complete_story_de.txt --data story_data_de.json --registry stories/template/subjects/registry_de.json --language de
```

### 2. Graphematic Stage (Hybrid)
**Step A: Calculate Data (Python)**
Analyzes punctuation and artifacts deterministically.
```bash
python engine/workers/run_stage.py graphematic --mode local
```
**Step B: Review (LLM)**
(Optional) Asks the LLM to validate the text state. Output is a text review saved to `analysis_graphematic_review`.
```bash
python engine/workers/run_stage.py graphematic --mode llm
```

### 3. Morphologic Stage (Hybrid)
**Step A: Tokenize (Python/Text)**
Generates morphological tokens based on suffix math or heuristics.
```bash
python engine/workers/run_stage.py morphologic --mode text
```
**Step B: Review (LLM)**
(Optional) LLM reviews the tokenization. Output is a text review saved to `analysis_morphologic_review`.
```bash
python engine/workers/run_stage.py morphologic --mode llm
```

### 4. Syntactic Stage (Hybrid)
**Step A: Heuristic Parsing (Python)**
Generates a dependency tree based on heuristics (e.g., Verbal/Nominal chains).
```bash
python engine/workers/run_stage.py syntactic --mode heuristic
```
**Step B: Review (LLM)**
(Optional) LLM reviews the syntax tree. Output is a text review saved to `analysis_syntactic_review`.
```bash
python engine/workers/run_stage.py syntactic --mode llm
```

### 5. Semantic Stage (LLM Only)
Uses the full context (Text, Words, Syntax) to generate a deep semantic essay (text).
*Note: This stage does not use JSON output anymore, but free-form text reasoning.*
```bash
python engine/workers/run_stage.py semantic
```

### 6. Translation Stage (LLM Only - Two-Step)
**Step A: Draft & Reasoning (Text)**
Generates a translation strategy, discussing alternatives and style. Saved to `analysis_translation_draft`.
```bash
python engine/workers/run_stage.py translation
```
**Step B: Final JSON Generation**
Takes the Draft and Syntax Data to generate the constrained **JSON Translation Space**. Saved to `analysis_translation`.
```bash
python engine/workers/run_stage.py translation --mode json
```

### 7. Entities Stage (Python)
Extracts entity mentions and writes `analysis_entities`. Running this stage also rebuilds the registry by default.
```bash
python engine/workers/run_stage.py entities
```

### 8. Websearch Stage (Python Fetch Only)
Fetches sources in Python, caches raw text, and stores jobs in `analysis_websearch`. Summary is provided by the external agent workflow.
```bash
# Default: fetch-only
python engine/workers/run_stage.py websearch

# Fetch-only (Python fetch, no LLM summary)
python engine/workers/run_stage.py websearch --mode fetch

# Local mode (builds jobs only)
python engine/workers/run_stage.py websearch --mode local

# With explicit data + registry
python engine/workers/run_stage.py websearch --data-file story_data.json --registry-file stories/template/subjects/registry.json
```
Note: No MCP is required for the Python fetch path (default). MCP can be re‑enabled if needed.

## Dry Run & Debugging
You can simulate any stage without calling the LLM to inspect the prompt and token calculations.
```bash
# Example: Check semantic prompt for first 3 items
python engine/workers/run_stage.py semantic --dry-run --dry-run-limit 3 --dry-run-out logs/dryrun_semantic.jsonl
```

Streaming (stateful /api/v1/chat):
```bash
python engine/workers/run_stage.py semantic --stream
```
Config default (optional):
```json
{ "api": { "stream": true } }
```

Force rerun (ignore “already complete”):
```bash
python engine/workers/run_stage.py entities --data-file story_data_de.json --subjects-dir stories/template/subjects_de --force
```

## Reset Stages (Per Language)
Use `reset_stage.py` to clear specific analysis stages without re‑initializing the whole file.

```bash
# Default (Ge'ez)
python engine/workers/reset_stage.py entities --data-file story_data.json
python engine/workers/reset_stage.py websearch --data-file story_data.json
python engine/workers/reset_stage.py translation --data-file story_data.json
python engine/workers/reset_stage.py translation_draft --data-file story_data.json

# German
python engine/workers/reset_stage.py entities --data-file story_data_de.json
python engine/workers/reset_stage.py websearch --data-file story_data_de.json
python engine/workers/reset_stage.py translation --data-file story_data_de.json
python engine/workers/reset_stage.py translation_draft --data-file story_data_de.json

# Both at once
python engine/workers/reset_stage.py semantic --downstream --data-file story_data.json --data-file story_data_de.json
```

## Logs
By default, dry‑run logs and parse errors are written to `logs/`:
- `logs/dryrun_<stage>.jsonl`
- `logs/error_log.txt`
You can override dry‑run output with `--dry-run-out`.

## File Hygiene Policy
To prevent accumulation of one-off helper scripts, lifecycle is tracked in:
- `policies/file-lifecycle-policy.md`
- `policies/script_inventory.json`

Audit current state:
```bash
python tools/file_hygiene.py
```
The report includes an `ARCHIVE_PLAN` section for temporary scripts.

Apply archive plan:
```bash
python tools/archive_candidates.py --apply
```

Strict mode (non-zero exit code on violations):
```bash
python tools/file_hygiene.py --strict
```

## Alias Refresh (No Re‑Init)
Refreshes `alias_hits` using the current `engine/config/aliases.json` without deleting any analyses.
```bash
# Default (Ge’ez)
python engine/workers/refresh_aliases.py --data-file story_data.json

# German
python engine/workers/refresh_aliases.py --data-file story_data_de.json --language de

# Both
python engine/workers/refresh_aliases.py --data-file story_data.json --data-file story_data_de.json
```

## Interlanguage Linking (GE ↔ DE)
Link parallel runs by `verse_id` and shared `asset_id` / `alias_id`:
```bash
python engine/workers/link_languages.py --data-a story_data.json --data-b story_data_de.json --lang-a gez --lang-b de --out stories/template/subjects/links.json
```
Notes:
- Use shared alias IDs (e.g., `ENTITY_SUPREME_GZA`) to unify “God / The Lord / Gott / der Herr”.
- If you update `aliases.json`, run `refresh_aliases.py` before linking.
- For non‑Ge’ez text, entities can be derived from alias hits (e.g., DE names/titles).
Alias files:
- Base: `engine/config/aliases.json`
- German seeds: `engine/config/aliases_de.json`

Optional stats output:
```bash
python engine/workers/link_languages.py --data-a story_data.json --data-b story_data_de.json --lang-a gez --lang-b de --out stories/template/subjects/links.json --stats-out stories/template/subjects/links_stats.json --print-stats
```

## Merge Translation Into Ge’ez (No Separate DE Registry)
If you already have a parallel DE run, you can merge its verse text into `analysis_translation_draft` inside the Ge’ez `story_data.json`:
```bash
python engine/workers/merge_translation.py \
  --data story_data.json \
  --translation-data story_data_de.json \
  --lang de
```
This keeps a single registry (Ge’ez) while preserving the DE text for downstream enrichment.

## Archived DE Workflow
The previous fully separate DE pipeline is archived here:
`docs/archive/de-workflow.md`

## Single Registry Workflow (GE + Translation Draft)
If you want *one* registry (Ge’ez‑based) but still use the DE text for enrichment, merge the translation into `analysis_translation_draft` and use alias hits from the translation:
```bash
# Merge DE text into the Ge’ez file (also updates alias_hits from translation)
# Note (Windows): use -X utf8 if your console chokes on emoji logs.
python -X utf8 engine/workers/merge_translation.py \
  --data story_data.json \
  --translation-data story_data_de.json \
  --lang de \
  --update-alias-hits

# Rebuild registry/occurrences/asset_bible from the unified file
python -X utf8 engine/workers/run_stage.py entities --data-file story_data.json --subjects-dir stories/template/subjects --build-registry --build-occurrences --build-asset-bible --force
```

## Entities Outputs (Registry / Occurrences / Asset Bible)
The `entities` stage can also emit the three subject files per language:
- `registry.json`
- `occurrences.jsonl`
- `asset_bible.json`

By default, running `entities` will rebuild the internal registry and also emit a Visionexe‑style `registry.json`.

Internal vs Public:
- `registry_internal.json` = pipeline/internal asset map (used for prompts/state)
- `registry.json` = Visionexe subject registry list

```bash
# Ge’ez (default) — PowerShell (one line)
python engine/workers/run_stage.py entities --data-file story_data.json --subjects-dir stories/template/subjects --build-occurrences --build-asset-bible

# Ge’ez (default) — PowerShell (multi-line)
python engine/workers/run_stage.py entities `
  --data-file story_data.json `
  --subjects-dir stories/template/subjects `
  --build-occurrences `
  --build-asset-bible

# German — PowerShell (one line)
python engine/workers/run_stage.py entities --data-file story_data_de.json --subjects-dir stories/template/subjects_de --build-occurrences --build-asset-bible

### Asset Bible Schema (Visionexe‑Aligned)
`asset_bible.json` now follows the Visionexe structure. Each subject includes:
- `id`, `name`, `type`, `aliases`
- `roles`, `visual_traits`, `changes`, `notes`
- `sources` (source_id list from occurrences)
- `owner_names`, `owner_subject_ids`
- `occurrence_count`, `occurrences_sample`
- `is_dynamic`, `state_policy`, `states`

Timeline + Phases:
- `asset_bible.json` includes top‑level `story_id` and `timeline_id`.
- Dynamic subjects get `state_policy="phases"` with `phase_01..phase_N` states.

Overrides:
```powershell
python engine/workers/run_stage.py entities `
  --data-file story_data.json `
  --subjects-dir stories/template/subjects `
  --build-occurrences `
  --build-asset-bible `
  --timeline-id default `
  --phase-count 3 `
  --phase-labels "Phase 1,Phase 2,Phase 3"
```

## Asset Bible Enrichment (Cards)
Builds `asset_bible_cards.jsonl` by merging `asset_bible.json` with `analysis_websearch` summaries.

```powershell
# Ge’ez
python engine/workers/asset_bible_enricher.py `
  --data-file story_data.json `
  --asset-bible stories/template/subjects/asset_bible.json `
  --out stories/template/subjects/asset_bible_cards.jsonl `
  --cards-dir stories/template/subjects/cards `
  --concurrency 0 `
  --data-file-de story_data_de.json `
  --links stories/template/subjects/links.json

Notes:
- The enricher calls LM Studio (OpenAI‑compatible endpoint) and expects JSON output.
- Each card is also written to `cards/<SUBJECT_ID>/card.md` and `card.json`.
- If `--concurrency` is omitted or 0, it defaults to `len(models) * max_concurrent_per_model`.
- Optional: pass `--data-file-de` + `--links` to enrich Ge’ez subjects with DE websearch summaries (via links.json).

## Websearch Outputs
`analysis_websearch` is stored per verse in `story_data*.json`. Job templates, limits, and tool name are controlled by `engine/config/config.json` under `websearch`.

Subjects-aligned searches:
- Actor jobs focus on character traits + known props.
- Prop jobs focus on materials/usage.
- Environment jobs focus on geography/location.
- Scene jobs use verse_id + environment/spatial context.
Context window:
- Websearch includes prev/next verse summaries (configurable via `websearch.context_window`).
- `websearch.context_scope` controls which job scopes receive context (`scene` by default).
- `websearch.context_excerpt_chars` controls excerpt length (0 disables excerpts).
Prompt size:
- `websearch.compact_context=true` trims context (current-only window) to prevent 400s.
- `websearch.include_registry_context=true` adds registry context if you need it.
Actor‑Prop linker:
- Extra jobs tie actors to props present in the same verse (templates in `websearch.actor_prop_templates`).
Rerank hook:
- Websearch output includes `rerank_candidates` derived from sources for downstream reranking.
Parallel links:
- If `links.json` exists (from `link_languages.py`), websearch injects `parallel_links` into job context.
Search tools:
- Python resolves sources (currently Wikipedia API).
- Internal LLM summary is disabled in `run_stage`; use the external agent workflow output.
Cache:
- Raw source text is stored in `stories/template/subjects/web_cache` (and `_de` variant).
Prompts:
- Websearch prompt now emits only the query list (no JSON payload, no schema).

## Data Structure (`story_data.json`)
- `text`: Raw input.
- `base_chars`: Atomic IDs.
- `words`: Word objects with `pre_processing` (Root, Asset IDs).
- `analysis_graphematic`: Hard data (Python).
- `analysis_graphematic_review`: LLM Review (Text).
- `analysis_semantic`: Text essay.
- `analysis_translation_draft`: Text draft.
- `analysis_translation`: Structured JSON translation.

## Orchestrator
The orchestrator keeps LM Studio model instances loaded and restarts them if the heartbeat stops.
```bash
python engine/workers/launch_orchestrator.py
```

## LM Studio Auth + MCP (Tools)
If **Require Authentication** is enabled in LM Studio, set an API token:
- Create/edit `.env` in the repo root:
```
LMSTUDIO_API_TOKEN=YOUR_TOKEN_HERE
```

Config keys:
- `websearch.sources` / `websearch.sources_de` (list of source configs)
- `websearch.cache_dir` / `websearch.cache_dir_de`
Current supported source types:
- `wikipedia` (uses `lang`)
