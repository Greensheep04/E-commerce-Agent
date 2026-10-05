import csv
import io
import json
from hashlib import sha256


def render_export(task, format):
    if task.operation not in {"query", "ask"} or task.result is None or task.status not in {"completed", "partial"}:
        raise ValueError("任务没有可导出的已核实结果")
    data = task.result["data"]
    if format == "markdown":
        lines = ["# 数据查询结果", "", data.get("answer", ""), "", "## 来源引用", ""]
        for index, reference in enumerate(data.get("evidence", []), start=1):
            locator = json.dumps(reference["locator"], ensure_ascii=False, sort_keys=True)
            lines.append(f"{index}. {reference['source_id']} / {reference['source_revision']} / {reference['record_id']}；{locator}")
        lines.extend(["", "## 运行信息", "", f"task_id: {task.task_id}", f"run_id: {task.run_id}"])
        raw, mime, filename = ("\n".join(lines) + "\n").encode(), "text/markdown", "answer.md"
    elif format == "csv":
        rows = data.get("rows", [])
        if not rows:
            raise ValueError("结果没有可导出的表格")
        fields = sorted({key for row in rows for key in row})
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                             for key, value in row.items()})
        raw, mime, filename = stream.getvalue().encode("utf-8-sig"), "text/csv", "result.csv"
    else:
        raise ValueError("导出格式必须为markdown或csv")
    return raw, mime, filename, sha256(raw).hexdigest()
