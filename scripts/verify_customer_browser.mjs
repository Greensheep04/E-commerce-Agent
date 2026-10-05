import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID, createHash } from "node:crypto";

const baseUrl = process.env.SHOP_CUSTOMER_URL ?? "http://127.0.0.1:8001";
const modulesPath = process.env.SHOP_BROWSER_NODE_MODULES;
assert.ok(modulesPath, "需要配置SHOP_BROWSER_NODE_MODULES");
process.env.PLAYWRIGHT_BROWSERS_PATH = path.resolve(".scratch/playwright");
process.env.TMPDIR = path.resolve(".scratch/browser-tmp");
await fs.mkdir(process.env.TMPDIR, { recursive: true });
const { chromium } = await import(pathToFileURL(path.join(modulesPath, "playwright", "index.mjs")));
const runId = "cus03-browser-" + new Date().toISOString().replaceAll(":", "").replaceAll("-", "") + "-" + randomUUID().slice(0, 8);
const directory = path.join(".scratch/evidence", runId);
await fs.mkdir(directory, { recursive: true });
const files = ["frontend/src/CustomerApp.tsx", "frontend/src/customer.css", "frontend/src/main.tsx",
  "src/shop/customer_api.py", "src/shop/customer_runtime.py", "src/shop/customer_agent.py"];
const hashes = {};
for (const file of files) hashes[file] = createHash("sha256").update(await fs.readFile(file)).digest("hex");
const report = { run_id: runId, scope: "CUS-03真实浏览器", checks: [], errors: [], source_hashes: hashes, status: "running" };
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
page.on("pageerror", error => report.errors.push(error.message));
async function save() {
  await fs.writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
}
async function check(name, operation) {
  await operation();
  report.checks.push({ name, passed: true });
  await save();
}
try {
  await page.goto(baseUrl + "/harness/?workspace=customer");
  await page.getByLabel("用户名", { exact: true }).fill("customer-11091");
  await page.getByLabel("密码", { exact: true }).fill((await fs.readFile(".local/auth/customer-11091.password", "utf8")).trim());
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.getByRole("button", { name: "退出登录", exact: true }).waitFor();
  await check("账号与来源显示", async () => {
    await page.getByText("Dalton Perez", { exact: false }).first().waitFor();
    assert.ok((await page.locator("body").innerText()).includes("microsoft/adventureworks-oltp"));
  });
  const conversation = await (await context.request.post(baseUrl + "/api/customer/conversations", { data: { title: runId } })).json();
  await page.reload();
  await page.getByRole("button", { name: new RegExp(runId) }).click();
  const order = conversation.orders[0].order_number;
  const message = "请查询订单" + order + "的完整明细。<script>window.customerProbe=1</script>";
  const responsePromise = page.waitForResponse(response => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.getByPlaceholder("输入订单或商品问题，按 Enter 发送，Shift + Enter 换行").fill(message);
  await page.getByRole("button", { name: "发送", exact: true }).click();
  const submitted = await (await responsePromise).json();
  await check("完整答复与服务器相同", async () => {
    await page.waitForFunction(() => document.querySelector(".customer-result-text"), { timeout: 125000 });
    const turn = await (await context.request.get(baseUrl + "/api/customer/turns/" + submitted.id)).json();
    assert.equal(turn.status, "completed");
    assert.equal(await page.locator(".customer-result-text").last().innerText(), turn.result.text);
    report.turn = turn;
    assert.ok(turn.result.orders.some(item => item.order_number === order));
  });
  await check("React文字安全", async () => {
    assert.ok((await page.locator("body").innerText()).includes("<script>window.customerProbe=1</script>"));
    assert.equal(await page.evaluate(() => window.customerProbe), undefined);
    assert.equal(await page.locator("script:not([src])").count(), 0);
  });
  await check("历史与事件重连", async () => {
    await page.reload();
    await page.getByRole("button", { name: new RegExp(runId) }).click();
    await page.locator(".customer-result-text").last().waitFor();
    assert.equal(await page.locator(".customer-result-text").last().innerText(), report.turn.result.text);
    const events = await (await context.request.get(baseUrl + "/api/customer/conversations/" + conversation.id + "/events?after=0")).json();
    const after = await (await context.request.get(baseUrl + "/api/customer/conversations/" + conversation.id + "/events?after=" + events.last_id)).json();
    assert.equal(after.events.length, 0);
    assert.ok(events.events.some(item => item.type === "answer"));
  });
  await check("两种页面宽度", async () => {
    for (const width of [1440, 390]) {
      await page.setViewportSize({ width, height: 1000 });
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    }
  });
  await check("跨账号会话拒绝", async () => {
    const other = await browser.newContext();
    await other.request.post(baseUrl + "/api/login", { data: { username: "customer-11176",
      password: (await fs.readFile(".local/auth/customer-11176.password", "utf8")).trim() } });
    for (const endpoint of ["/api/customer/conversations/" + conversation.id,
      "/api/customer/conversations/" + conversation.id + "/events", "/api/customer/turns/" + submitted.id]) {
      assert.equal((await other.request.get(baseUrl + endpoint)).status(), 404);
    }
    assert.equal((await other.request.post(baseUrl + "/api/customer/turns/" + submitted.id + "/cancel")).status(), 404);
    await other.close();
  });
  await check("退出登录", async () => {
    await page.getByRole("button", { name: "退出登录", exact: true }).click();
    await page.getByLabel("用户名", { exact: true }).waitFor();
    assert.equal((await context.request.get(baseUrl + "/api/customer/conversations/" + conversation.id)).status(), 401);
  });
  assert.deepEqual(report.errors, []);
  report.status = "passed";
} catch (error) {
  report.failure = String(error);
  report.status = "failed";
  throw error;
} finally {
  await save();
  await browser.close();
  console.log(path.join(directory, "report.json"));
}
