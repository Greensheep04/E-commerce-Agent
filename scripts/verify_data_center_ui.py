import argparse
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--task-id")
    parser.add_argument("--task-actor", default="assessment-admin")
    args = parser.parse_args()
    inputs_bytes = args.inputs.read_bytes()
    inputs = json.loads(inputs_bytes)
    accounts = inputs.get("actors", inputs.get("accounts", {}))
    admin = accounts.get("admin")
    task_actor = accounts.get(args.task_actor)
    if not admin or not task_actor:
        raise ValueError("固定输入缺少来源管理员或任务所属账号")
    if not all(value.get("username") and value.get("password_file") for value in (admin, task_actor)):
        raise ValueError("固定输入账号缺少用户名或密码文件")
    passwords = [Path(value["password_file"]).read_text(encoding="utf-8").strip()
                 for value in (admin, task_actor)]
    if not all(passwords):
        raise ValueError("固定身份密码文件为空")
    modules_path = os.environ.get("SHOP_BROWSER_NODE_MODULES")
    if not modules_path:
        raise ValueError("需要配置SHOP_BROWSER_NODE_MODULES")
    base_url = inputs.get("base_url", "http://127.0.0.1:8002").rstrip("/")
    report_path = args.output.resolve()
    run_id = "dc-ui-" + uuid.uuid4().hex
    source_paths = [
        Path("frontend/src/main.tsx"),
        Path("frontend/src/data-center/api.ts"),
        Path("frontend/src/data-center/DataCenterWorkspace.tsx"),
        Path("frontend/src/data-center/styles.css"),
        Path("scripts/verify_data_center_ui.py"),
    ]
    source_hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    script = r'''
import assert from "node:assert/strict";
import path from "node:path";
import { pathToFileURL } from "node:url";
const { chromium } = await import(pathToFileURL(path.join(process.env.SHOP_BROWSER_NODE_MODULES, "playwright", "index.mjs")));
const fs = await import("node:fs/promises");
const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
const page = await browser.newPage();
const errors = [];
page.on("pageerror", error => errors.push(error.message));
await page.goto(process.env.DATA_CENTER_BASE_URL + "/harness/?workspace=data-center");
await page.getByLabel("用户名").fill(process.env.DATA_CENTER_ADMIN_USERNAME);
await page.getByLabel("密码").fill(process.env.DATA_CENTER_ADMIN_PASSWORD);
await page.getByRole("button", { name: "登录" }).click();
await page.getByRole("heading", { name: "数据中台" }).waitFor();
for (const label of ["资料目录", "中文问答", "指标定义", "任务记录", "来源管理"]) {
  await page.getByRole("button", { name: label }).waitFor();
}
await page.getByRole("button", { name: "来源管理" }).click();
await page.getByLabel("来源编号").waitFor();
await page.getByRole("button", { name: "退出登录" }).click();
await page.getByRole("heading", { name: "登录数据中台" }).waitFor();
await page.getByLabel("用户名").fill(process.env.DATA_CENTER_TASK_USERNAME);
await page.getByLabel("密码").fill(process.env.DATA_CENTER_TASK_PASSWORD);
await page.getByRole("button", { name: "登录" }).click();
await page.getByRole("heading", { name: "数据中台" }).waitFor();
await page.getByRole("button", { name: "中文问答" }).click();
await page.getByLabel("问题").waitFor();
await page.getByRole("button", { name: "指标定义" }).click();
await page.getByRole("heading", { name: "销售额", exact: true }).waitFor();
await page.getByRole("button", { name: "任务记录" }).click();
await page.getByRole("heading", { name: "任务记录" }).waitFor();
  const taskId = process.env.DATA_CENTER_TASK_ID;
  if (taskId) {
  const taskButton = page.getByRole("button").filter({ hasText: taskId });
  await taskButton.waitFor();
  await taskButton.click();
  await page.getByText(`任务编号：${taskId}`, { exact: true }).waitFor();
  const evidenceButtons = page.locator(".dc-evidence");
  assert.ok(await evidenceButtons.count() > 0, "完成任务缺少可回读的来源引用");
  await evidenceButtons.first().click();
  const rawSourceView = page.locator(".dc-raw pre").last();
  await rawSourceView.waitFor();
  const rawSource = await rawSourceView.innerText();
  assert.ok(rawSource.includes('"quota_date": "2023-08-30"'), "原始目标记录缺少目标日期");
  assert.ok(rawSource.includes('"quota_amount": "1324000.0000"'), "原始目标记录金额不符");
  assert.ok(rawSource.includes('"source_line": 18'), "原始目标记录缺少来源行号");
  assert.ok(rawSource.includes('"source_sha256": "769cfb3d44c1f422dd23eb24bc0d310f82739eb6f49598520a3eb47e68161064"'),
    "原始目标记录缺少已核对来源摘要");
  const markdownPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "下载 Markdown" }).click();
  const markdown = await markdownPromise;
  assert.equal(markdown.suggestedFilename(), "data-center.md");
  const markdownBytes = await fs.readFile(await markdown.path());
  const markdownText = markdownBytes.toString("utf8");
  assert.ok(markdownText.includes("# 数据查询结果") && markdownText.includes("1,324,000.000000"));
  assert.ok(markdownText.includes(taskId), "Markdown 导出缺少任务编号");
  const csvPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "下载 CSV" }).click();
  const csv = await csvPromise;
  assert.equal(csv.suggestedFilename(), "data-center.csv");
  const csvBytes = await fs.readFile(await csv.path());
  assert.ok(csvBytes.length > 3 && csvBytes[0] === 0xef && csvBytes[1] === 0xbb && csvBytes[2] === 0xbf);
  const csvText = csvBytes.subarray(3).toString("utf8");
  assert.ok(csvText.includes("employee_id") && csvText.includes("275") && csvText.includes("1142965"),
    "CSV 导出缺少真实指标结果");
}
assert.deepEqual(errors, []);
await browser.close();
'''
    environment = {
        **os.environ,
        "DATA_CENTER_BASE_URL": base_url,
        "DATA_CENTER_ADMIN_USERNAME": admin["username"],
        "DATA_CENTER_ADMIN_PASSWORD": passwords[0],
        "DATA_CENTER_TASK_USERNAME": task_actor["username"],
        "DATA_CENTER_TASK_PASSWORD": passwords[1],
        "DATA_CENTER_TASK_ID": args.task_id or "",
        "SHOP_BROWSER_NODE_MODULES": modules_path,
        "PLAYWRIGHT_BROWSERS_PATH": str(Path(".scratch/playwright").resolve()),
        "TMPDIR": str(Path(".scratch/browser-tmp").resolve()),
    }
    Path(environment["TMPDIR"]).mkdir(parents=True, exist_ok=True)
    Path(environment["PLAYWRIGHT_BROWSERS_PATH"]).mkdir(parents=True, exist_ok=True)
    result = subprocess.run(["node", "--input-type=module", "-e", script],
                            check=False, capture_output=True, text=True, env=environment)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    report = {"status": "passed", "run_id": run_id, "base_url": base_url,
              "task_id": args.task_id,
              "inputs_path": str(args.inputs.resolve()), "output_path": str(report_path),
              "inputs_sha256": hashlib.sha256(inputs_bytes).hexdigest(),
              "source_sha256": source_hashes,
              "checks": ["real_admin_session", "source_management_form", f"real_{args.task_actor}_session",
                         "catalog_navigation", "question_form", "metric_definitions", "task_list",
                         *( ["task_references", "quota_source_fields", "markdown_content", "csv_content_and_bom"]
                           if args.task_id else [])],
              "model_calls": 0}
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "passed", "run_id": run_id, "checks": len(report["checks"])}))


if __name__ == "__main__":
    main()
