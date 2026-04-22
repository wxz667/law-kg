from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from pathlib import Path
from typing import Any


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True, help="Path to trained model dir")
    p.add_argument("--input", required=True, help="Path to relation_pairs.jsonl")
    p.add_argument("--documents", type=int, default=3)
    p.add_argument("--samples-per-doc", type=int, default=8)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--cpu", action="store_true")
    p.add_argument("--output", default="", help="Write detailed predictions to this jsonl if set")
    return p


def main() -> int:
    args = build_parser().parse_args()
    model_dir = Path(args.model)
    input_path = Path(args.input)
    out_path = Path(args.output) if args.output else None

    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    rows = iter_jsonl(input_path)

    by_doc: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        meta = r.get("metadata", {}) or {}
        doc_id = str(meta.get("document_node_id", "") or "").strip()
        if not doc_id:
            continue
        by_doc.setdefault(doc_id, []).append(r)

    rng = random.Random(args.seed)
    doc_ids = list(by_doc.keys())
    rng.shuffle(doc_ids)
    picked = doc_ids[: max(args.documents, 0)]

    tokenizer = AutoTokenizer.from_pretrained(str(model_dir), use_fast=True)
    model = AutoModelForSequenceClassification.from_pretrained(str(model_dir))
    device = torch.device("cpu") if args.cpu or not torch.cuda.is_available() else torch.device("cuda")
    model.to(device)
    model.eval()

    id2label = getattr(model.config, "id2label", None) or {}
    if not id2label and (model_dir / "labels.json").exists():
        label_to_id = json.loads((model_dir / "labels.json").read_text(encoding="utf-8"))
        id2label = {v: k for k, v in label_to_id.items()}

    all_outputs: list[dict[str, Any]] = []

    for doc_id in picked:
        samples = by_doc[doc_id]
        rng.shuffle(samples)
        samples = samples[: max(args.samples_per_doc, 0)]
        if not samples:
            continue

        preds = []
        with torch.no_grad():
            for r in samples:
                meta = r.get("metadata", {}) or {}
                text_a = str(r.get("source_text", "") or "").strip()
                text_b = str(r.get("target_text", "") or "").strip()
                if not text_b:
                    text_b = str(meta.get("target_name", "") or "").strip()
                if not text_b:
                    text_b = str(r.get("target_node_id", "") or "").strip()

                enc = tokenizer(
                    text_a,
                    text_b,
                    truncation=True,
                    padding=True,
                    max_length=256,
                    return_tensors="pt",
                )
                enc = {k: v.to(device) for k, v in enc.items()}
                out = model(**enc)
                logits = out.logits
                prob = torch.softmax(logits, dim=-1)[0]
                label_id = int(torch.argmax(prob).item())
                score = float(prob[label_id].item())
                pred_label = str(id2label.get(label_id, label_id))

                gold = str(r.get("relation_type", "") or "")
                preds.append((gold, pred_label))

                all_outputs.append(
                    {
                        "document_node_id": doc_id,
                        "sample_id": r.get("sample_id", ""),
                        "source_node_id": r.get("source_node_id", ""),
                        "target_node_id": r.get("target_node_id", ""),
                        "gold": gold,
                        "pred": pred_label,
                        "score": score,
                        "source_text": text_a,
                        "target_text": text_b,
                    }
                )

        c = Counter((g, p) for g, p in preds)
        print(doc_id)
        for (g, p), n in sorted(c.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
            print(f"  {g} -> {p}: {n}")

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in all_outputs) + ("\n" if all_outputs else ""),
            encoding="utf-8",
        )
        print(str(out_path.resolve()))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

