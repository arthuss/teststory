import argparse
import asyncio
import datetime
import json
import os
import random
import re
import sys

import aiohttp


CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".env")


def _load_env_file():
    if not os.path.exists(ENV_FILE):
        return
    try:
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        pass


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def fix_malformed_json(text: str) -> str:
    text = re.sub(r"\}\s*\"", '}, "', text)
    text = re.sub(r"\]\s*\"", '], "', text)
    text = re.sub(r"\"\s*\n\s*\"", "\",\n\"", text)
    text = re.sub(r",\s*\}", "}", text)
    text = re.sub(r",\s*\]", "]", text)
    text = text.replace("}}}", "}}")
    if text.count("{") > text.count("}"):
        text += "}" * (text.count("{") - text.count("}"))
    if text.count("[") > text.count("]"):
        text += "]" * (text.count("[") - text.count("]"))
    return text


def extract_json_block(text: str) -> str | None:
    if not text:
        return None
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "\"":
                in_str = False
            continue
        else:
            if ch == "\"":
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
    return None


def safe_name(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def load_json(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_story_data(path: str) -> list:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def load_websearch_map(story_data: list) -> dict:
    mapping: dict[str, list[dict]] = {}
    for verse in story_data or []:
        ws = verse.get("analysis_websearch")
        if not isinstance(ws, dict):
            continue
        jobs = ws.get("jobs")
        if not isinstance(jobs, list):
            continue
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = job.get("job_id") or ""
            parts = job_id.split(":")
            subject_id = ""
            if len(parts) >= 3 and parts[0] in {"ent", "actorprop", "actor_prop"}:
                subject_id = parts[1]
            if not subject_id:
                continue
            summary = job.get("summary") or ""
            sources = job.get("sources") or []
            mapping.setdefault(subject_id, []).append({
                "summary": summary,
                "sources": sources,
                "job_id": job_id,
                "query": job.get("query")
            })
    return mapping


def load_links_map(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}
    links = data.get("links") if isinstance(data, dict) else None
    if not isinstance(links, list):
        return {}
    mapping: dict[str, dict[str, int]] = {}
    for link in links:
        gez_ids = link.get("gez") or []
        de_ids = link.get("de") or []
        if not gez_ids or not de_ids:
            continue
        for gid in gez_ids:
            if not gid:
                continue
            counts = mapping.setdefault(gid, {})
            for did in de_ids:
                if not did:
                    continue
                counts[did] = counts.get(did, 0) + 1
    return mapping


def top_linked_subjects(link_map: dict, subject_id: str, limit: int = 5) -> list[str]:
    if not link_map or not subject_id:
        return []
    counts = link_map.get(subject_id) or {}
    ordered = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return [k for k, _ in ordered[:limit]]


def build_prompt(subject: dict, web_items: list[dict], max_chars: int) -> str:
    name = subject.get("name") or subject.get("id") or ""
    subject_type = subject.get("type") or ""
    aliases = subject.get("aliases") or []
    occ_count = subject.get("occurrence_count") or 0
    states = subject.get("states") or []

    lines = []
    lines.append("Du erstellst eine Asset-Bible-Karte auf Basis der Quellen unten.")
    lines.append("Schreibe klaren, dichten Fließtext. Keine Listen, kein Markdown im JSON.")
    lines.append("Gib strikt JSON mit den geforderten Feldern zurück.")
    lines.append(f"Maximale Beschreibungslänge: {max_chars} Zeichen.")
    lines.append(f"ID: {subject.get('id','')}")
    lines.append(f"Name: {name}")
    lines.append(f"Type: {subject_type}")
    if aliases:
        lines.append(f"Aliases: {', '.join(aliases[:10])}")
    lines.append(f"Occurrences: {occ_count}")
    if states:
        lines.append("States:")
        for st in states:
            lines.append(f"- {st.get('state_id')} | {st.get('label')} ({st.get('chapter_start')}-{st.get('chapter_end')})")

    lines.append("\nQuellen (verwende URLs inline in den Texten):")
    for idx, item in enumerate(web_items[:8], 1):
        summary = (item.get("summary") or "").strip()
        urls = [s.get("url") for s in (item.get("sources") or []) if s.get("url")]
        if summary:
            lines.append(f"[Quelle {idx}] URLs: {', '.join(urls)}")
            lines.append(summary)

    lines.append("\nOUTPUT JSON Schema:")
    lines.append("{")
    lines.append('  "description": "...",')
    lines.append('  "visual_anatomy": ["..."],')
    lines.append('  "evolution": ["..."],')
    lines.append('  "props": ["..."],')
    lines.append('  "prompt_keywords": ["..."],')
    lines.append('  "prompt_block": "...",')
    lines.append('  "phase_prompts": [')
    lines.append('    {"state_id": "phase_01", "label": "...", "summary": "...", "prompt_keywords": ["..."], "prompt_block": "..."}')
    lines.append('  ]')
    lines.append("}")
    return "\n".join(lines)


def render_markdown(subject: dict, card: dict) -> str:
    name = subject.get("name") or "Unknown"
    subject_id = subject.get("id")
    subject_type = (subject.get("type") or "subject").upper()
    lines = []
    lines.append(f"## [{subject_type}] {name} (ID: {subject_id})")
    lines.append(f"**Description:** {card.get('description','').strip()}")
    lines.append("")
    lines.append("### 1. VISUAL ANATOMY / DESIGN")
    visual_anatomy = card.get("visual_anatomy", [])
    if isinstance(visual_anatomy, str):
        lines.append(visual_anatomy)
    else:
        for item in visual_anatomy:
            lines.append(f"{item}")
    lines.append("")
    lines.append("### 2. EVOLUTION / VARIANTS")
    evolution = card.get("evolution", [])
    if isinstance(evolution, str):
        lines.append(evolution)
    else:
        for item in evolution:
            lines.append(f"{item}")
    lines.append("")
    lines.append("### 3. PROPS & EQUIPMENT")
    props = card.get("props", [])
    if isinstance(props, str):
        lines.append(props)
    else:
        for item in props:
            lines.append(f"{item}")
    lines.append("")
    keywords = card.get("prompt_keywords", [])
    if keywords:
        lines.append("### 4. AI PROMPT KEYWORDS")
        if isinstance(keywords, str):
            lines.append(keywords)
        else:
            lines.append("`" + "`, `".join(keywords) + "`")
        lines.append("")
    prompt_block = card.get("prompt_block")
    if prompt_block:
        lines.append("### 5. PROMPT BLOCK (T2I)")
        lines.append(prompt_block.strip())
        lines.append("")
    phase_prompts = card.get("phase_prompts") or []
    if phase_prompts:
        lines.append("### 6. PHASE PROMPTS")
        for phase in phase_prompts:
            if not isinstance(phase, dict):
                continue
            label = phase.get("label") or phase.get("state_id") or "Phase"
            summary = phase.get("summary")
            prompt = phase.get("prompt_block")
            keywords = phase.get("prompt_keywords") or []
            if summary:
                lines.append(f"*   **{label}:** {summary}")
            else:
                lines.append(f"*   **{label}:**")
            if prompt:
                lines.append(f"    Prompt: {prompt.strip()}")
            if keywords:
                lines.append(f"    Keywords: {', '.join(keywords)}")
        lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def build_fallback_card(subject: dict) -> dict:
    name = subject.get("name") or "Unknown"
    roles = sorted(set(subject.get("roles") or []))
    traits = sorted(set(subject.get("visual_traits") or []))
    changes = sorted(set(subject.get("changes") or []))
    description = f"Auto-generated asset card for {name}."
    visual = []
    if traits:
        visual.append(f"Traits: {', '.join(traits)}")
    if roles:
        visual.append(f"Roles: {', '.join(roles)}")
    evolution = []
    phase_prompts = []
    if subject.get("states"):
        for state in subject.get("states") or []:
            label = state.get("label") or state.get("state_id")
            evolution.append(f"Phase ({label})")
            phase_label = label or "Phase"
            prompt = f"{name}, {', '.join(traits or roles)}"
            if phase_label and str(phase_label).lower() not in ("default", "none", "unknown"):
                prompt = f"{prompt}, {phase_label}"
            phase_prompts.append(
                {
                    "state_id": state.get("state_id") or "",
                    "label": phase_label,
                    "summary": "",
                    "prompt_keywords": traits[:12],
                    "prompt_block": prompt,
                }
            )
    if not evolution and changes:
        evolution.append(f"Changes: {', '.join(changes)}")
    return {
        "description": description,
        "visual_anatomy": visual or ["TBD."],
        "evolution": evolution or ["No known variants yet."],
        "props": ["TBD."],
        "prompt_keywords": traits[:12],
        "prompt_block": f"{name}, {', '.join(traits or roles)}",
        "phase_prompts": phase_prompts,
    }


def ensure_phase_prompts(card: dict, subject: dict) -> None:
    if card.get("phase_prompts"):
        return
    states = subject.get("states") or []
    if not states:
        return
    name = subject.get("name") or "Unknown"
    traits = list(card.get("prompt_keywords") or subject.get("visual_traits") or [])
    roles = list(subject.get("roles") or [])
    base_prompt = card.get("prompt_block") or f"{name}, {', '.join(traits or roles)}"
    phase_prompts = []
    for state in states:
        label = state.get("label") or state.get("state_id") or "Phase"
        prompt = base_prompt
        if label and str(label).lower() not in ("default", "none", "unknown"):
            prompt = f"{base_prompt}, {label}"
        phase_prompts.append(
            {
                "state_id": state.get("state_id") or "",
                "label": label,
                "summary": "",
                "prompt_keywords": traits[:12],
                "prompt_block": prompt,
            }
        )
    card["phase_prompts"] = phase_prompts


def _is_stateful_url(url: str) -> bool:
    return "/api/v1/chat" in (url or "")


async def call_lmstudio(session: aiohttp.ClientSession, url: str, token: str | None, model: str, prompt: str, max_tokens: int, temperature: float, semaphore: asyncio.Semaphore | None = None) -> str:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if _is_stateful_url(url):
        payload = {
            "model": model,
            "input": f"You are a concise asset-bible writer.\n\n{prompt}",
            "temperature": temperature,
            "max_output_tokens": max_tokens,
            "stream": False,
        }
    else:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a concise asset-bible writer."},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
    if semaphore is None:
        async with session.post(url, json=payload, headers=headers, timeout=180) as resp:
            try:
                data = await resp.json()
            except aiohttp.ContentTypeError:
                text = await resp.text()
                print(f"[asset_bible_enricher] HTTP {resp.status} non-JSON response from {url} (first 400 chars):")
                print(text[:400])
                # Fallback to stateful endpoint if completions URL is wrong for LM Studio
                if "/v1/chat/completions" in url:
                    fallback_url = url.replace("/v1/chat/completions", "/api/v1/chat")
                    return await call_lmstudio(session, fallback_url, token, model, prompt, max_tokens, temperature, semaphore)
                return ""
    else:
        async with semaphore:
            async with session.post(url, json=payload, headers=headers, timeout=180) as resp:
                try:
                    data = await resp.json()
                except aiohttp.ContentTypeError:
                    text = await resp.text()
                    print(f"[asset_bible_enricher] HTTP {resp.status} non-JSON response from {url} (first 400 chars):")
                    print(text[:400])
                    if "/v1/chat/completions" in url:
                        fallback_url = url.replace("/v1/chat/completions", "/api/v1/chat")
                        return await call_lmstudio(session, fallback_url, token, model, prompt, max_tokens, temperature, semaphore)
                    return ""
    if not isinstance(data, dict):
        return ""
    if _is_stateful_url(url):
        content = ""
        output = data.get("output")
        if isinstance(output, list):
            for item in output:
                if isinstance(item, dict):
                    itype = item.get("type")
                    if itype and itype not in ("message", "text"):
                        continue
                    if "content" in item and item.get("content") is not None:
                        content += str(item.get("content", ""))
                    elif "text" in item and item.get("text") is not None:
                        content += str(item.get("text", ""))
                elif isinstance(item, str):
                    content += item
        return content.strip()
    choices = data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return (message.get("content") or "").strip()


async def main_async(args):
    _load_env_file()
    config = load_config()
    # Match run_stage.py: use the stateful LM Studio endpoint explicitly
    lm_url = "http://localhost:1234/api/v1/chat"
    models = config.get("models") or [""]
    model = args.model or (models[0] if models else "")
    token = os.environ.get("LMSTUDIO_API_TOKEN") or os.environ.get("LM_API_TOKEN")
    max_concurrent_per_model = int(config.get("api", {}).get("max_concurrent_per_model", 4) or 4)
    semaphores = {m: asyncio.Semaphore(max_concurrent_per_model) for m in models if m}

    story_data = load_story_data(args.data_file)
    web_map = load_websearch_map(story_data)
    web_map_de = {}
    links_map = {}
    if args.data_file_de:
        web_map_de = load_websearch_map(load_story_data(args.data_file_de))
    if args.links:
        links_map = load_links_map(args.links)

    asset_bible = load_json(args.asset_bible)
    subjects = asset_bible.get("subjects") if isinstance(asset_bible, dict) else None
    if not subjects:
        print("[asset_bible_enricher] No subjects found.")
        return 1

    output_jsonl = args.out
    os.makedirs(os.path.dirname(output_jsonl), exist_ok=True)

    cards_dir = args.cards_dir
    if cards_dir:
        os.makedirs(cards_dir, exist_ok=True)

    existing = set()
    if args.resume and os.path.exists(output_jsonl):
        with open(output_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("id"):
                    existing.add(payload["id"])

    include_types = set([t.strip().lower() for t in (args.types or "").split(",") if t.strip()])

    queue = asyncio.Queue()
    for subject in subjects:
        sid = subject.get("id")
        if not sid:
            continue
        stype = (subject.get("type") or "").lower()
        if include_types and stype not in include_types:
            continue
        if args.resume and sid in existing:
            continue
        await queue.put(subject)

    if args.limit and args.limit > 0:
        # Trim queue to limit
        limited = []
        while not queue.empty() and len(limited) < args.limit:
            limited.append(queue.get_nowait())
        queue = asyncio.Queue()
        for item in limited:
            await queue.put(item)

    write_lock = asyncio.Lock()
    total = queue.qsize()
    counter = {"done": 0}

    async def worker():
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    subject = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                sid = subject.get("id")
                web_items = list(web_map.get(sid, []))
                if web_map_de and links_map:
                    linked = top_linked_subjects(links_map, sid, limit=5)
                    for lid in linked:
                        web_items.extend(web_map_de.get(lid, []))
                prompt = build_prompt(subject, web_items, args.max_summary_chars)
                chosen_model = args.model or random.choice(models)
                sem = semaphores.get(chosen_model)
                response = await call_lmstudio(session, lm_url, token, chosen_model, prompt, args.max_output_tokens, args.temperature, sem)

                card = None
                if response:
                    try:
                        card = json.loads(response)
                    except json.JSONDecodeError:
                        candidate = extract_json_block(response)
                        if candidate:
                            try:
                                card = json.loads(candidate)
                            except json.JSONDecodeError:
                                fixed = fix_malformed_json(candidate)
                                try:
                                    card = json.loads(fixed)
                                except json.JSONDecodeError:
                                    card = None
                if not isinstance(card, dict):
                    card = build_fallback_card(subject)
                ensure_phase_prompts(card, subject)

                markdown = render_markdown(subject, card)

                subject_dir = ""
                card_path = ""
                card_json_path = ""
                if cards_dir:
                    safe_id = safe_name(sid)
                    subject_dir = os.path.join(cards_dir, safe_id)
                    os.makedirs(subject_dir, exist_ok=True)
                    card_path = os.path.join(subject_dir, "card.md")
                    card_json_path = os.path.join(subject_dir, "card.json")
                    with open(card_path, "w", encoding="utf-8") as f:
                        f.write(markdown)
                    with open(card_json_path, "w", encoding="utf-8") as f:
                        json.dump(card, f, ensure_ascii=False, indent=2)

                record = {
                    "id": sid,
                    "name": subject.get("name"),
                    "type": subject.get("type"),
                    "owner_subject_ids": subject.get("owner_subject_ids") or [],
                    "owner_names": subject.get("owner_names") or [],
                    "subject_dir": os.path.relpath(subject_dir, os.path.dirname(output_jsonl)) if subject_dir else "",
                    "card_path": os.path.relpath(card_path, os.path.dirname(output_jsonl)) if card_path else "",
                    "card_json": os.path.relpath(card_json_path, os.path.dirname(output_jsonl)) if card_json_path else "",
                    "markdown": markdown,
                    "card": card,
                    "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                async with write_lock:
                    with open(output_jsonl, "a", encoding="utf-8") as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    counter["done"] += 1
                    print(f"[asset_bible_enricher] {counter['done']}/{total} {sid}")

    workers = []
    if not args.concurrency or args.concurrency <= 0:
        args.concurrency = max(1, len(models) * max_concurrent_per_model)
    parallel = max(1, args.concurrency)
    for _ in range(parallel):
        workers.append(asyncio.create_task(worker()))
    await asyncio.gather(*workers)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Build asset_bible_cards.jsonl with LLM enrichment.")
    parser.add_argument("--data-file", required=True, help="Path to story_data.json")
    parser.add_argument("--asset-bible", required=True, help="Path to asset_bible.json")
    parser.add_argument("--out", required=True, help="Output JSONL path")
    parser.add_argument("--cards-dir", help="Output folder for card.md + card.json")
    parser.add_argument("--model", help="LM Studio model id override")
    parser.add_argument("--max-output-tokens", type=int, default=2048)
    parser.add_argument("--max-summary-chars", type=int, default=1600)
    parser.add_argument("--temperature", type=float, default=0.3)
    parser.add_argument("--concurrency", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--types", help="Comma-separated subject types to include")
    parser.add_argument("--data-file-de", help="Optional DE story_data.json to enrich via links")
    parser.add_argument("--links", help="Optional links.json to map GEZ -> DE subjects")

    args = parser.parse_args()
    if args.limit <= 0:
        args.limit = 0
    if not args.cards_dir:
        args.cards_dir = ""

    exit_code = asyncio.run(main_async(args))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
