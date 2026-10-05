import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash, randomUUID } from "node:crypto";

const baseUrl = process.env.SHOP_CUSTOMER_URL ?? "http://127.0.0.1:8001";
process.env.PLAYWRIGHT_BROWSERS_PATH = path.resolve(".scratch/playwright");
process.env.TMPDIR = path.resolve(".scratch/browser-tmp");
await fs.mkdir(process.env.TMPDIR, { recursive: true });
const { chromium } = await import(pathToFileURL(path.join(process.env.SHOP_BROWSER_NODE_MODULES, "playwright", "index.mjs")));
const runId = "cus03-controls-" + new Date().toISOString().replaceAll(":", "").replaceAll("-", "") + "-" + randomUUID().slice(0, 8);
const directory = path.join(".scratch/evidence", runId);
await fs.mkdir(directory, { recursive: true });
const report = { run_id: runId, scope: "CUS-03取消按钮及状态显示", status: "running", errors: [], checks: [] };
report.frontend_sha256 = createHash("sha256").update(await fs.readFile("frontend/src/CustomerApp.tsx")).digest("hex");
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext();
  const login = await context.request.post(baseUrl + "/api/login", { data: { username: "customer-11091",
    password: (await fs.readFile(".local/auth/customer-11091.password", "utf8")).trim() } });
  assert.ok(login.ok());
  const created = await context.request.post(baseUrl + "/api/customer/conversations", { data: { title: runId } });
  assert.ok(created.ok());
  const conversation = await created.json();
  report.conversation_id = conversation.id;
  const page = await context.newPage();
  page.on("pageerror", error => report.errors.push(error.message));
  await page.goto(baseUrl + "/harness/?workspace=customer");
  await page.getByRole("button", { name: new RegExp(runId) }).click();
  const submitting = page.waitForResponse(response => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.getByPlaceholder("输入订单或商品问题，按 Enter 发送，Shift + Enter 换行").fill("查询订单" + conversation.orders[0].order_number + "的完整明细");
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const submitted = await (await submitting).json();
  report.turn_id = submitted.id;
  const cancelling = page.waitForResponse(response => response.url().endsWith("/turns/" + submitted.id + "/cancel") && response.request().method() === "POST");
  await page.getByRole("button", { name: "取消本轮", exact: true }).click();
  const response = await cancelling;
  assert.ok(response.ok());
  report.cancel_result = await response.json();
  assert.equal(report.cancel_result.status, "cancelled");
  assert.equal(report.cancel_result.result, null);
  report.checks.push({ name: "取消按钮请求实际消息接口", passed: true });
  const article = page.locator('[data-turn-id="' + submitted.id + '"]');
  await article.getByText("已取消", { exact: true }).waitFor();
  assert.equal(await article.locator(".customer-result-text").count(), 0);
  await page.reload();
  await page.getByRole("button", { name: new RegExp(runId) }).click();
  await page.locator('[data-turn-id="' + submitted.id + '"]').getByText("已取消", { exact: true }).waitFor();
  const actual = await context.request.get(baseUrl + "/api/customer/turns/" + submitted.id);
  assert.ok(actual.ok());
  report.actual = await actual.json();
  assert.equal(report.actual.status, "cancelled");
  assert.equal(report.actual.result, null);
  report.checks.push({ name: "取消显示及历史与服务器一致", passed: true });
  assert.deepEqual(report.errors, []);
  report.status = "passed";
} catch (error) {
  report.status = "failed";
  report.failure = String(error);
  throw error;
} finally {
  report.completed_at = new Date().toISOString();
  await fs.writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  await browser.close();
  console.log(path.join(directory, "report.json"));
}
