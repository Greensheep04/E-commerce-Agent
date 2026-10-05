import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRIC_NAMES = {"sales_amount", "order_count", "average_order_value", "quota_amount", "quota_completion"}
RATIO_NAMES = {"average_order_value", "quota_completion"}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def record_answer_digest(record):
    result = raw_result(record)
    answer = result.get("data", {}).get("answer") if result else None
    if answer is None:
        answer = ""
    if not isinstance(answer, str):
        raise TypeError("原始回答需要文本或null")
    return hashlib.sha256(answer.encode()).hexdigest()


def semantic_review_metadata(path, evaluated):
    review = read_json(path)
    if review.get("status") != "completed":
        raise ValueError("语义审核尚未完成")
    observed = {(item["case_id"], item["attempt"]): item for item in evaluated
                if item["actual_status"] not in {"missing", "duplicate"}}
    entries = review.get("reviews")
    if not isinstance(entries, list):
        raise TypeError("语义审核缺少逐项记录")
    seen = set()
    for item in entries:
        key = (item["case_id"], item["attempt"])
        if key not in observed or key in seen:
            raise ValueError("语义审核包含未知或重复的评价记录")
        seen.add(key)
        if any(item.get(name) != observed[key][name]
               for name in ("record_sha256", "answer_sha256", "references_sha256")):
            raise ValueError("语义审核与原始回答或引用摘要不一致")
    if seen != set(observed):
        raise ValueError("语义审核没有覆盖全部实际评价记录")
    return {"status": "completed", "path": str(path.resolve()), "sha256": digest_file(path),
            "attempts_requiring_review": len(observed), "observed_records": len(observed)}


def as_decimal(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).replace(",", "").rstrip("%"))
    except InvalidOperation:
        return None


def same_value(expected, actual):
    if expected is None or actual is None:
        return expected is actual
    if isinstance(expected, (int, float, Decimal)) and not isinstance(expected, bool):
        expected_decimal = as_decimal(expected)
        actual_decimal = as_decimal(actual)
        if expected_decimal is not None and actual_decimal is not None:
            return expected_decimal == actual_decimal
    if isinstance(expected, str):
        expected_decimal = as_decimal(expected)
        actual_decimal = as_decimal(actual)
        if expected_decimal is not None and actual_decimal is not None:
            return expected_decimal == actual_decimal
        if len(expected) >= 10 and expected[4:5] == "-":
            return str(actual).startswith(expected[:10])
    return expected == actual


def same_ratio_value(expected, actual):
    expected_decimal, actual_decimal = as_decimal(expected), as_decimal(actual)
    if expected_decimal is None or actual_decimal is None:
        return expected is actual
    # MetricFlow的ratio SQL使用浮点除法，金额及计数仍使用精确比较。
    return math.isclose(float(expected_decimal), float(actual_decimal), rel_tol=1e-14, abs_tol=0)


def expected_leaves(value, prefix=""):
    if isinstance(value, dict):
        for key, nested in value.items():
            yield from expected_leaves(nested, f"{prefix}.{key}" if prefix else key)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            yield from expected_leaves(nested, f"{prefix}[{index}]")
    else:
        yield prefix, value


def answer_contains(answer, value, *, ratio=False):
    if not isinstance(answer, str) or value is None:
        return False
    if isinstance(value, str):
        if len(value) >= 10 and value[4:5] == "-":
            return value[:10] in answer
        if as_decimal(value) is None:
            compact_answer = re.sub(r"\s+", "", answer).casefold()
            compact_value = re.sub(r"\s+", "", value).casefold()
            return compact_value in compact_answer
    expected_decimal = as_decimal(value)
    if expected_decimal is None:
        return str(value) in answer
    tokens = re.findall(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?%?", answer, flags=re.ASCII)
    actual_values = [as_decimal(token) for token in tokens]
    if expected_decimal in actual_values:
        return True
    if ratio and any(same_ratio_value(expected_decimal, actual) for actual in actual_values):
        return True
    if isinstance(value, str) and value.rstrip().endswith("%"):
        return any(token.endswith("%") and as_decimal(token) == expected_decimal for token in tokens)
    return False


def load_source_files(registry, fixture):
    sources = {item["source_key"]: item for item in registry["sources"]}
    period = fixture["fixed_analytics_period"]
    delimiter = "\t" if period["delimiter"] == "tab" else period["delimiter"]
    indexes = period["field_map"]
    start = date.fromisoformat(period["period_start_inclusive"])
    end = date.fromisoformat(period["period_end_exclusive"])
    order_source = sources["adventureworks-oltp:sales-order-header"]
    quota_source = sources["adventureworks:salesperson-quota-history"]
    order_path = Path(order_source.get("upstream_path") or order_source["original_path"])
    quota_path = Path(quota_source.get("upstream_path") or quota_source["original_path"])
    order_digest = digest_file(order_path)
    quota_digest = digest_file(quota_path)
    if order_digest != order_source["sha256"] or quota_digest != quota_source["sha256"]:
        raise ValueError("固定订单或目标 CSV 摘要与来源登记不一致")
    order_fields = indexes["SalesOrderHeader.csv"]
    quota_fields = indexes["SalesPersonQuotaHistory.csv"]
    order_rows = []
    with order_path.open("r", encoding="utf-8-sig", newline="") as file:
        for line_number, values in enumerate(csv.reader(file, delimiter=delimiter), start=1):
            order_date = datetime.fromisoformat(values[order_fields["OrderDate"]]).date()
            if order_date < start or order_date >= end or int(values[order_fields["Status"]]) != period["status_shipped"]:
                continue
            employee = values[order_fields["SalesPersonID"]].strip()
            if not employee:
                continue
            order_rows.append({
                "source_line": line_number,
                "order_id": values[order_fields["SalesOrderID"]],
                "order_date": order_date.isoformat(),
                "employee_id": int(employee),
                "subtotal": Decimal(values[order_fields["SubTotal"]]),
                "status": int(values[order_fields["Status"]]),
                "source_sha256": order_digest,
            })
    quota_rows = []
    with quota_path.open("r", encoding="utf-8-sig", newline="") as file:
        for line_number, values in enumerate(csv.reader(file, delimiter=delimiter), start=1):
            quota_date = datetime.fromisoformat(values[quota_fields["QuotaDate"]]).date()
            if quota_date < start or quota_date >= end:
                continue
            quota_rows.append({
                "source_line": line_number,
                "employee_id": int(values[quota_fields["BusinessEntityID"]]),
                "quota_date": quota_date.isoformat(),
                "quota_amount": Decimal(values[quota_fields["SalesQuota"]]),
                "source_sha256": quota_digest,
                "source_revision": quota_source["revision"],
            })
    return sources, order_rows, quota_rows, order_source, quota_source


def analytics_from_csv(case, order_rows, quota_rows):
    employee_ids = case.get("employee_ids", [])
    by_employee = {}
    for employee_id in employee_ids:
        orders = [row for row in order_rows if row["employee_id"] == employee_id]
        quotas = [row for row in quota_rows if row["employee_id"] == employee_id]
        with localcontext() as context:
            context.prec = 38
            sales = sum((row["subtotal"] for row in orders), Decimal(0))
            quota = sum((row["quota_amount"] for row in quotas), Decimal(0))
            by_employee[str(employee_id)] = {
                "sales_amount": sales,
                "order_count": len({row["order_id"] for row in orders}),
                "matching_order_rows": len(orders),
                "average_order_value": sales / len({row["order_id"] for row in orders})
                if orders else None,
                "quota_amount": quota,
                "quota_completion": sales / quota if quota else None,
                "quota_rows": quotas,
                "order_rows": sorted(orders, key=lambda row: row["source_line"]),
            }
    with localcontext() as context:
        context.prec = 38
        sales = sum((row["sales_amount"] for row in by_employee.values()), Decimal(0))
        order_count = sum(row["order_count"] for row in by_employee.values())
        quota = sum((row["quota_amount"] for row in by_employee.values()), Decimal(0))
        all_orders = sorted(
            (order for row in by_employee.values() for order in row["order_rows"]),
            key=lambda row: row["source_line"],
        )
        all_quotas = [quota for row in by_employee.values() for quota in row["quota_rows"]]
        return {
            "by_employee": by_employee,
            "sales_amount": sales,
            "order_count": order_count,
            "matching_order_rows": len(all_orders),
            "average_order_value": sales / order_count if order_count else None,
            "quota_amount": quota,
            "quota_completion": sales / quota if quota else None,
            "order_rows": all_orders,
            "quota_rows": all_quotas,
        }


def validate_fixture_metrics(case, computed):
    checks = []
    expected_metrics = case.get("expected_metrics", {})
    for name, expected in expected_metrics.items():
        if name in METRIC_NAMES and not isinstance(expected, dict):
            actual = computed.get(name)
            checks.append({"check": f"fixture_csv.{name}", "passed": same_value(expected, actual)})
        elif name in METRIC_NAMES and isinstance(expected, dict):
            for employee_id, expected_value in expected.items():
                if employee_id == "highest_employee_id":
                    metric = "sales_amount" if name == "sales_amount" else "order_count"
                    actual = max(computed["by_employee"].items(), key=lambda item: item[1][metric])[0]
                else:
                    actual = computed["by_employee"].get(str(employee_id), {}).get(name)
                checks.append({"check": f"fixture_csv.{name}.{employee_id}",
                               "passed": same_value(expected_value, actual)})
        elif name == "matching_order_rows":
            checks.append({"check": "fixture_csv.matching_order_rows",
                           "passed": same_value(expected, computed["matching_order_rows"])})
        elif name in {"average_order_value_display", "display"}:
            metric = "quota_completion" if str(expected).endswith("%") else "average_order_value"
            raw_value = computed.get(metric)
            actual = (f"{(raw_value * 100 if metric == 'quota_completion' else raw_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}%"
                      if raw_value is not None and metric == "quota_completion" else
                      f"{raw_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"
                      if raw_value is not None else None)
            checks.append({"check": f"fixture_csv.{name}", "passed": same_value(expected, actual)})
        elif name == "quota_completion_display":
            raw_value = computed.get("quota_completion")
            actual = f"{(raw_value * 100).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}%" if raw_value is not None else None
            checks.append({"check": "fixture_csv.quota_completion_display",
                           "passed": same_value(expected, actual)})
        elif name == "quota_date":
            actual = computed["quota_rows"][0]["quota_date"] if len(computed["quota_rows"]) == 1 else None
            checks.append({"check": "fixture_csv.quota_date", "passed": same_value(expected, actual)})
        elif name == "quota_source_line":
            actual = computed["quota_rows"][0]["source_line"] if len(computed["quota_rows"]) == 1 else None
            checks.append({"check": "fixture_csv.quota_source_line", "passed": same_value(expected, actual)})
        elif name in {"first_evidence", "last_evidence"}:
            if not computed["order_rows"]:
                actual = None
            else:
                row = computed["order_rows"][0 if name == "first_evidence" else -1]
                actual = {"line": row["source_line"], "SalesOrderID": row["order_id"]}
            checks.append({"check": f"fixture_csv.{name}", "passed": same_value(expected, actual)})
    return checks


def raw_result(record):
    if isinstance(record.get("result"), dict):
        return record["result"]
    if isinstance(record.get("data"), dict) and "status" in record:
        return record
    return None


def record_status(record, result):
    if record.get("timeout"):
        return "timeout"
    if record.get("error"):
        return "unknown" if record["error"].get("type") in {"JSONDecodeError", "ReadError"} else "failed"
    status = result.get("status") if result else None
    if status:
        return status
    observations = record.get("observations", [])
    if observations:
        return observations[-1].get("status", "unknown")
    return "unknown"


def evidence_list(result):
    data = result.get("data", {}) if result else {}
    return data.get("evidence", []) if isinstance(data, dict) else []


def reference_data(reference):
    return reference.get("data") if isinstance(reference, dict) and reference.get("status") == "completed" else None


def employee_rows(value):
    found = []
    if isinstance(value, dict):
        if "employee_id" in value:
            found.append(value)
        for nested in value.values():
            found.extend(employee_rows(nested))
    elif isinstance(value, list):
        for nested in value:
            found.extend(employee_rows(nested))
    return found


def validate_metric_references(case, result, references, sources, order_source, quota_source,
                                order_rows, quota_rows):
    checks = []
    evidence = evidence_list(result)
    if len(evidence) != len(references):
        checks.append({"check": "metric_reference_count", "passed": False})
        return checks
    metric_pairs = [(ref, response) for ref, response in zip(evidence, references, strict=True)
                    if ref.get("kind") == "metric"]
    for index, (ref, response) in enumerate(metric_pairs, start=1):
        detail = reference_data(response)
        if ref.get("kind") != "metric" or not isinstance(detail, dict):
            checks.append({"check": f"metric_reference_{index}_readback", "passed": False})
            continue
        checks.append({"check": f"metric_reference_{index}_snapshot_binding", "passed":
                       detail.get("snapshot_id") == ref.get("snapshot_id")
                       and detail.get("source_revision") == ref.get("source_revision")
                       and detail.get("aggregate_sha256") == ref.get("content_sha256")})
        rows = detail.get("rows", [])
        actual_orders = [row for row in rows if row.get("record_type") == "order"]
        raw_orders_by_line = {row["source_line"]: row for row in order_rows}
        order_checks = []
        for row in actual_orders:
            source_row = raw_orders_by_line.get(row.get("source_line"))
            order_checks.append(source_row is not None
                                and str(row.get("record_id")) == source_row["order_id"]
                                and row.get("employee_id") == source_row["employee_id"]
                                and same_value(row.get("subtotal"), source_row["subtotal"])
                                and row.get("source_sha256") == order_source["sha256"])
        checks.append({"check": f"metric_reference_{index}_orders_match_csv",
                       "passed": bool(actual_orders) and all(order_checks)})
        selected_ids = next((item["filters"]["SalesPersonID"] for item in case.get("source_evidence", [])
                             if isinstance(item.get("filters", {}).get("SalesPersonID"), list)),
                            case.get("employee_ids"))
        if selected_ids:
            expected_lines = {row["source_line"] for row in order_rows
                              if row["employee_id"] in selected_ids}
            actual_lines = [row.get("source_line") for row in actual_orders]
            checks.append({"check": f"metric_reference_{index}_complete_order_scope",
                           "passed": len(actual_lines) == len(set(actual_lines))
                           and set(actual_lines) == expected_lines})
        original_quotas = detail.get("original_quotas")
        if original_quotas is not None:
            source_quotas = {row["employee_id"]: row for row in quota_rows}
            quota_checks = []
            for row in original_quotas:
                source_row = source_quotas.get(row.get("employee_id"))
                quota_checks.append(source_row is not None
                                    and row.get("record_id") == f"quota:{row.get('employee_id')}"
                                    and row.get("quota_date") == source_row["quota_date"]
                                    and same_value(row.get("quota_amount"), source_row["quota_amount"])
                                    and row.get("source_line") == source_row["source_line"]
                                    and row.get("source_sha256") == quota_source["sha256"]
                                    and row.get("source_revision") == quota_source["revision"])
            needs_quota = "quota_completion" in ref.get("locator", {}).get("metric_ids", [])
            checks.append({"check": f"metric_reference_{index}_quotas_match_csv",
                           "passed": (not needs_quota and not original_quotas)
                           or (needs_quota and bool(original_quotas) and all(quota_checks))})
    return checks


def validate_document_references(case, result, references, sources, reference_documents, registry_root):
    checks = []
    evidence = evidence_list(result)
    if len(evidence) != len(references):
        return [{"check": "document_reference_count", "passed": False}]
    document_pairs = [(ref, response) for ref, response in zip(evidence, references, strict=True)
                      if ref.get("kind") == "document"]
    evidence = [ref for ref, _ in document_pairs]
    references = [response for _, response in document_pairs]
    citations = {entry["reference_id"]: entry["citations"]
                 for entry in reference_documents.get("reference_entries", [])}
    for index, (ref, response) in enumerate(zip(evidence, references, strict=True), start=1):
        detail = reference_data(response)
        chunk_ref = detail.get("ref", {}) if isinstance(detail, dict) else {}
        text = detail.get("text", "") if isinstance(detail, dict) else ""
        checks.append({"check": f"document_reference_{index}_original_binding", "passed":
                       ref.get("kind") == "document" and isinstance(detail, dict)
                       and chunk_ref.get("source_id") == ref.get("source_id")
                       and chunk_ref.get("source_sha256") == ref.get("source_sha256")
                       and chunk_ref.get("content_sha256") == ref.get("content_sha256")
                       and chunk_ref.get("start_line") == ref.get("start_line")
                       and chunk_ref.get("end_line") == ref.get("end_line")
                       and isinstance(text, str) and bool(text)
                       and hashlib.sha256(text.encode()).hexdigest() == detail.get("text_sha256")})
        source_matches = [item for item in sources.values()
                          if item["source_id"] == ref.get("source_id")
                          and item["revision"] == ref.get("source_revision")
                          and item["sha256"] == ref.get("source_sha256")]
        checks.append({"check": f"document_reference_{index}_source_digest",
                       "passed": bool(source_matches)})
    expected_source_keys = {item["source_key"] for item in case.get("source_evidence", [])
                           if item.get("source_key") in sources
                           and sources[item["source_key"]].get("document_key")}
    expected_source_ids = {sources[key]["source_id"] for key in expected_source_keys}
    actual_source_ids = {ref.get("source_id") for ref in evidence}
    checks.append({"check": "fixture_document_source_coverage",
                   "passed": bool(expected_source_ids) and expected_source_ids <= actual_source_ids})
    for item in case.get("source_evidence", []):
        key = item.get("source_key")
        if key not in sources:
            continue
        source_id = sources[key]["source_id"]
        source = sources[key]
        if not source.get("document_key"):
            continue
        source_path = (registry_root / source["path"]).resolve()
        if not source_path.is_relative_to(registry_root.resolve()):
            raise ValueError("固定文档来源路径超出登记目录")
        checks.append({"check": f"fixture_document_original_digest:{key}", "passed":
                       source_path.is_file() and digest_file(source_path) == source["sha256"]})
        required_lines = []
        if item.get("reference_id"):
            matching_citations = [citation for citation in citations.get(item["reference_id"], [])
                                  if citation.get("document_key") == item.get("document_key")]
            if not matching_citations:
                checks.append({"check": f"fixture_citation_definition:{key}:{item['reference_id']}",
                               "passed": False})
            required_lines.extend(
                (citation["start_line"], citation["end_line"], citation["quote"])
                for citation in matching_citations
            )
        if item.get("line_start") is not None:
            required_lines.append((item["line_start"], item.get("line_end", item["line_start"]), None))
        for line_start, line_end, _quote in required_lines:
            # 固定预期的位置属于原件或固定参考文本；引用位置属于Docling转换结果。
            matching = [ref for ref in evidence if ref.get("source_id") == source_id]
            checks.append({"check": f"fixture_citation_source:{key}:{line_start}-{line_end}",
                           "passed": bool(matching)})
    return checks


def disposition_checks(case, result, references):
    disposition = case.get("expected_disposition", {})
    if not disposition:
        return []
    checks = []
    if disposition.get("must_deny_access"):
        http_denied = any(reference.get("http_status") == 403 for reference in references
                          if isinstance(reference, dict))
        status = result.get("status") if result else None
        data = result.get("data", {}) if result else {}
        blocked = status == "DATA_BLOCKED" and bool(data.get("unknowns")) and not (
            data.get("rows") or data.get("evidence"))
        checks.append({"check": "deterministic_access_denial",
                       "passed": http_denied or blocked or status in {"forbidden", "denied"}
                       or result is not None and result.get("http_status") in {403, 404}})
    if disposition.get("must_not_return_requested_employee_rows"):
        requested = case.get("requested_employee_ids", [])
        data = result.get("data", {}) if result else {}
        returned = employee_rows(data)
        returned.extend(employee_rows(references))
        checks.append({"check": "requested_employee_rows_absent",
                       "passed": not any(row.get("employee_id") in requested for row in returned)})
    requested_metric = case.get("requested_metric")
    if requested_metric and disposition.get("must_decline_numeric_result"):
        data = result.get("data", {}) if result else {}
        rows = data.get("rows", []) if isinstance(data, dict) else []
        checks.append({"check": "unsupported_metric_absent_from_structured_rows",
                       "passed": not any(requested_metric in row for row in rows if isinstance(row, dict))})
    return checks


def evaluate(case, record, references, source_data, fixture):
    result = raw_result(record)
    status = record_status(record, result)
    checks = []
    input_value = record.get("input", {})
    if input_value:
        checks.append({"check": "input_binds_fixed_case", "passed":
                       input_value.get("case_id") == case["id"]
                       and input_value.get("category") == case["category"]
                       and input_value.get("expected", {}).get("id") == case["id"]
                       and input_value.get("payload", {}).get("question") == case["query"]})
    else:
        checks.append({"check": "input_binds_fixed_case", "passed": False})
    checks.append({"check": "finished_record", "passed": bool(record.get("finished_at"))})
    if record.get("source_stable") is not None:
        checks.append({"check": "source_stable", "passed": record["source_stable"] is True})
    elif record.get("source_before") is not None or record.get("source_after") is not None:
        checks.append({"check": "source_stable", "passed": record.get("source_before") == record.get("source_after")})
    else:
        checks.append({"check": "source_stable", "passed": False})
    refusal = case.get("category") == "refusal"
    data = result.get("data", {}) if result else {}
    blocked = status == "DATA_BLOCKED" and isinstance(data, dict) and bool(data.get("unknowns"))
    rejected = refusal and (blocked or status in {"forbidden", "denied"}
                           or result is not None and result.get("http_status") in {403, 404})
    if status not in {"completed", "partial"} and not rejected or result is None:
        checks.append({"check": "terminal_success_or_partial", "passed": False})
        checks.extend(disposition_checks(case, result, references))
    else:
        checks.append({"check": "terminal_success_or_partial", "passed": True})
        data = result.get("data", {})
        answer = data.get("answer", "") if isinstance(data, dict) else ""
        for key, expected in expected_leaves(case.get("expected_metrics", {})):
            if key == "formula" or key.endswith(".formula"):
                continue
            checks.append({"check": f"answer_mentions:{key}", "passed": answer_contains(
                answer, expected, ratio=key in RATIO_NAMES)})
        if case.get("category") == "analytics":
            computed = analytics_from_csv(case, source_data["orders"], source_data["quotas"])
            checks.extend(validate_fixture_metrics(case, computed))
            checks.extend(validate_metric_references(
                case, result, references, source_data["sources"], source_data["order_source"],
                source_data["quota_source"], source_data["orders"], source_data["quotas"],
            ))
            rows = data.get("rows", []) if isinstance(data, dict) else []
            employee_rows_by_id = {str(row.get("employee_id")): row for row in rows
                                   if isinstance(row, dict) and row.get("employee_id") is not None}
            for metric in case.get("expected_metrics", {}):
                if metric in METRIC_NAMES and metric != "quota_amount":
                    declared = case["expected_metrics"][metric]
                    compare = same_ratio_value if metric in RATIO_NAMES else same_value
                    if isinstance(declared, dict):
                        for employee_id in declared:
                            if employee_id == "highest_employee_id":
                                continue
                            expected = computed["by_employee"][str(employee_id)][metric]
                            actual = employee_rows_by_id.get(str(employee_id), {}).get(metric)
                            checks.append({"check": f"result_rows.{metric}.{employee_id}_match_csv",
                                           "passed": compare(expected, actual)})
                        continue
                    expected = computed.get(metric)
                    actual_rows = [row for row in rows if isinstance(row, dict)]
                    if not any(metric in row for row in actual_rows):
                        continue
                    if len(actual_rows) == 1:
                        actual = actual_rows[0].get(metric)
                    elif metric in RATIO_NAMES:
                        actual = None
                        per_employee_checks = [compare(
                            computed["by_employee"][employee_id][metric], row.get(metric))
                            for employee_id, row in employee_rows_by_id.items()
                            if employee_id in computed["by_employee"]]
                        complete = set(employee_rows_by_id) == set(computed["by_employee"])
                        checks.append({"check": f"result_rows.{metric}_per_employee_match_csv",
                                       "passed": complete and all(per_employee_checks)})
                        continue
                    else:
                        actual = sum((as_decimal(row.get(metric)) or Decimal(0) for row in actual_rows), Decimal(0))
                    checks.append({"check": f"result_rows.{metric}_match_csv",
                                   "passed": compare(expected, actual)})
            if "quota_amount" in case.get("expected_metrics", {}):
                originals = []
                for response in references:
                    detail = reference_data(response)
                    if isinstance(detail, dict):
                        originals.extend(detail.get("original_quotas", []))
                unique_originals = {
                    (row.get("employee_id"), row.get("quota_date"), row.get("source_line")): row
                    for row in originals
                }
                actual_quota = sum((as_decimal(row.get("quota_amount")) or Decimal(0)
                                    for row in unique_originals.values()), Decimal(0))
                checks.append({"check": "result_original_quotas.quota_amount_match_csv",
                               "passed": bool(unique_originals)
                               and same_value(computed["quota_amount"], actual_quota)})
            original_unknowns = data.get("unknowns", []) if isinstance(data, dict) else []
        elif case.get("category") in {"document", "cross_document"}:
            checks.append({"check": "document_evidence_present",
                           "passed": any(ref.get("kind") == "document" for ref in evidence_list(result))})
            if any(ref.get("kind") == "metric" for ref in evidence_list(result)):
                checks.extend(validate_metric_references(
                    case, result, references, source_data["sources"], source_data["order_source"],
                    source_data["quota_source"], source_data["orders"], source_data["quotas"],
                ))
            if any(ref.get("kind") == "document" for ref in evidence_list(result)):
                checks.extend(validate_document_references(
                    case, result, references, source_data["sources"], source_data["reference_documents"],
                    source_data["registry_root"],
                ))
            original_unknowns = data.get("unknowns", []) if isinstance(data, dict) else []
        else:
            original_unknowns = data.get("unknowns", []) if isinstance(data, dict) else []
            checks.extend(disposition_checks(case, result, references))
            checks.append({"check": "refusal_has_no_business_result",
                           "passed": not data.get("rows") and not data.get("evidence")})
    semantic_items = []
    semantic_items.extend(case.get("expected_claims", []))
    if case.get("expected_relations"):
        semantic_items.append({"expected_relations": case["expected_relations"]})
    if case.get("expected_disposition"):
        semantic_items.append({"expected_disposition": case["expected_disposition"]})
    if status in {"completed", "partial"}:
        semantic_items.append({"answer_and_citations": "逐项核对语义是否覆盖必需事实，引用是否支撑对应句子"})
    passed = bool(checks) and all(item["passed"] for item in checks)
    return {
        "case_id": case["id"], "category": case["category"],
        "attempt": input_value.get("attempt"), "actual_status": status,
        "score_candidate": int(passed), "checks": checks,
        "unknowns": original_unknowns if status in {"completed", "partial"} or rejected else [],
        "manual_review_required": semantic_items,
        "task_id": record.get("task_id") or (result.get("data", {}).get("task_id") if result else None),
        "request_id": input_value.get("request_id"),
    }


def planned_slots(fixture):
    return [(case, attempt) for case in fixture["cases"] for attempt in range(1, case["attempts"] + 1)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=ROOT / "tests/fixtures/data_center_cases.json")
    parser.add_argument("--registry", type=Path, default=ROOT / ".local/data-center/sources/r3/sources.json")
    parser.add_argument("--attempt-dir", type=Path, default=ROOT / ".scratch/evidence/dc-formal-20261004-r1/attempts")
    parser.add_argument("--reference-documents", type=Path,
                        default=ROOT / ".scratch/evidence/dc01-inputs-20261004-r2/reference_documents.json")
    parser.add_argument("--record", type=Path)
    parser.add_argument("--case-id")
    parser.add_argument("--references", type=Path)
    parser.add_argument("--semantic-review", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture = read_json(args.fixture)
    registry = read_json(args.registry)
    sources, orders, quotas, order_source, quota_source = load_source_files(registry, fixture)
    source_data = {"sources": sources, "orders": orders, "quotas": quotas,
                   "order_source": order_source, "quota_source": quota_source,
                   "registry_root": (ROOT / registry["source_root"]).resolve(),
                   "reference_documents": read_json(args.reference_documents)}
    slots = planned_slots(fixture)
    if len(slots) != fixture["execution_policy"]["planned_attempts"]:
        raise ValueError("固定场景数量与正式评价计划次数不一致")
    case_by_id = {case["id"]: case for case in fixture["cases"]}
    if args.record:
        record = read_json(args.record)
        if args.case_id:
            case = case_by_id[args.case_id]
        else:
            case_id = record.get("input", {}).get("case_id") or record.get("case")
            if case_id not in case_by_id:
                raise ValueError("单项记录没有绑定固定场景，请提供 --case-id")
            case = case_by_id[case_id]
        refs = read_json(args.references) if args.references else record.get("references", [])
        if not isinstance(refs, list):
            raise TypeError("引用回读记录必须是 JSON 数组")
        evaluated = [evaluate(case, record, refs, source_data, fixture)]
        denominator = 1
        thresholds = {"record_only": {"attempts": 1, "passed_minimum": 1}}
    else:
        files = {}
        for path in args.attempt_dir.glob("*.json"):
            match = re.fullmatch(r"([A-Z]+-\d+)-(\d+)\.json", path.name)
            if not match:
                continue
            key = (match.group(1), int(match.group(2)))
            files.setdefault(key, []).append(path)
        evaluated = []
        request_ids = []
        for case, attempt in slots:
            paths = files.get((case["id"], attempt), [])
            if len(paths) != 1:
                evaluated.append({"case_id": case["id"], "category": case["category"],
                                  "attempt": attempt, "actual_status": "missing" if not paths else "duplicate",
                                  "score_candidate": 0, "checks": [{"check": "unique_attempt_record", "passed": False}],
                                  "unknowns": [], "manual_review_required": [], "task_id": None,
                                  "request_id": None})
                continue
            record = read_json(paths[0])
            refs = record.get("references", [])
            if not isinstance(refs, list):
                refs = []
            value = evaluate(case, record, refs, source_data, fixture)
            value["record_path"] = str(paths[0].resolve())
            value["record_sha256"] = digest_file(paths[0])
            value["answer_sha256"] = record_answer_digest(record)
            value["references_sha256"] = hashlib.sha256(json.dumps(
                refs, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            evaluated.append(value)
            if value.get("request_id"):
                request_ids.append(value["request_id"])
        duplicate_requests = {request_id for request_id, count in Counter(request_ids).items() if count > 1}
        for item in evaluated:
            if item.get("request_id") in duplicate_requests:
                item["checks"].append({"check": "unique_request_id", "passed": False})
                item["score_candidate"] = 0
        denominator = len(slots)
        thresholds = fixture["acceptance_thresholds"]
    category_report = {}
    for category, threshold in thresholds.items():
        if category == "overall":
            category_items = evaluated
        else:
            category_items = [item for item in evaluated if item["category"] == category]
        category_report[category] = {
            "denominator": threshold["attempts"],
            "observed_records": sum(item["actual_status"] not in {"missing", "duplicate"}
                                    for item in category_items),
            "candidate_passes": sum(item["score_candidate"] for item in category_items),
            "minimum": threshold["passed_minimum"],
            "meets_automatic_candidate_threshold":
                sum(item["actual_status"] not in {"missing", "duplicate"}
                    for item in category_items) == threshold["attempts"]
                and sum(item["score_candidate"] for item in category_items) >= threshold["passed_minimum"],
        }
    report = {
        "fixture_id": fixture["fixture_id"], "fixture_sha256": digest_file(args.fixture),
        "source_registry_sha256": digest_file(args.registry),
        "source_files": {
            "sales_order_header": {"path": str(order_source["upstream_path"]), "sha256": digest_file(Path(order_source["upstream_path"]))},
            "salesperson_quota_history": {"path": str(quota_source["upstream_path"]), "sha256": digest_file(Path(quota_source["upstream_path"]))},
        },
        "scope": "record_only" if args.record else "formal_90_attempts",
        "denominator": denominator,
        "observed_records": sum(item["actual_status"] not in {"missing", "duplicate"}
                                for item in evaluated),
        "status_counts": dict(Counter(item["actual_status"] for item in evaluated)),
        "candidate_passes": sum(item["score_candidate"] for item in evaluated),
        "category_thresholds": category_report,
        "semantic_review": {
            "status": "pending_lead_review" if any(item["manual_review_required"] for item in evaluated) else "none",
            "attempts_requiring_review": sum(bool(item["manual_review_required"]) for item in evaluated),
            "note": "自动核查只确认固定数值、来源标识、行号、摘要、原始CSV重算和权限结果；自然语言含义及语句与引用的支持关系需要主代理逐项审阅。",
        },
        "numeric_comparison": {"csv_decimal_precision": 38, "amounts_and_counts": "exact",
                               "metricflow_ratios_relative_tolerance": "1e-14",
                               "display_values": "exact", "natural_language_and_formulas": "lead_review"},
        "attempts": evaluated,
    }
    if args.semantic_review:
        if args.record:
            raise ValueError("语义审核需要正式90次报告的逐项摘要")
        report["semantic_review"] = semantic_review_metadata(args.semantic_review, evaluated)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"status": "written", "scope": report["scope"],
                      "denominator": denominator, "observed_records": report["observed_records"],
                      "candidate_passes": report["candidate_passes"],
                      "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
