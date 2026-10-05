import argparse
import json
from pathlib import Path

from shop.config import Settings
from shop.integration_history import copy_history_files, import_assessment_history


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-database-url")
    parser.add_argument("--import-id", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--assessment-source", type=Path)
    parser.add_argument("--poster-source", type=Path)
    parser.add_argument("--evidence-source", type=Path)
    parser.add_argument("--evidence-target", type=Path)
    args = parser.parse_args()
    settings = Settings()
    result = {"import_id": args.import_id}
    if args.source_database_url is not None:
        result["database"] = import_assessment_history(args.source_database_url, settings.database_url, args.import_id)
    if args.assessment_source is not None:
        result["assessment_files"] = copy_history_files(args.assessment_source, Path(".local/assessment/source"))
    if args.poster_source is not None:
        result["poster_files"] = copy_history_files(args.poster_source, settings.poster_dir)
    if args.evidence_source is not None:
        if args.evidence_target is None:
            raise ValueError("历史证据需要指定目标目录")
        result["evidence_files"] = copy_history_files(args.evidence_source, args.evidence_target)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(args.report), "parts": list(result)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
